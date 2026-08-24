package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** JSONL v4 persistence backend compatible with the session format published in pi 0.84.2. */
public final class JsonlSessionRepository implements SessionRepository {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int RECOVERY_CHECKPOINT_VERSION = 1;
    private static final Pattern SESSION_ID = Pattern.compile(
            "^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$"
    );
    private static final DateTimeFormatter FILE_TIME = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'")
            .withZone(ZoneOffset.UTC);

    private final Path sessionsRoot;
    private final Path cwd;
    private final Clock clock;
    private final SessionIdGenerator idGenerator;
    private final JsonlOperationAbortNotifier abortNotifier;
    private final MarkerObservationMode markerObservationMode;
    private final WatchServiceFactory watchServiceFactory;
    private final LongAdder watchServiceStarts = new LongAdder();
    private final LongAdder watchServiceFallbacks = new LongAdder();
    private final LongAdder pollingStarts = new LongAdder();
    private final LongAdder durableAdvisoriesAccepted = new LongAdder();
    private final LongAdder durableAdvisoriesRejected = new LongAdder();
    private final Set<String> activeDestinations = new HashSet<>();

    public JsonlSessionRepository(Path sessionsRoot, Path cwd) {
        this(sessionsRoot, cwd, Clock.systemUTC(), SessionIdGenerator.uuidV7());
    }

    public JsonlSessionRepository(
            Path sessionsRoot,
            Path cwd,
            JsonlOperationAbortNotifier abortNotifier
    ) {
        this(
                sessionsRoot, cwd, Clock.systemUTC(), SessionIdGenerator.uuidV7(),
                abortNotifier, MarkerObservationMode.POLLING
        );
    }

    public JsonlSessionRepository(Path sessionsRoot) {
        this(sessionsRoot, Path.of(".").toAbsolutePath().normalize());
    }

    public JsonlSessionRepository(
            Path sessionsRoot,
            Path cwd,
            Clock clock,
            SessionIdGenerator idGenerator
    ) {
        this(
                sessionsRoot, cwd, clock, idGenerator,
                JsonlOperationAbortNotifier.NONE, MarkerObservationMode.POLLING
        );
    }

    public JsonlSessionRepository(
            Path sessionsRoot,
            Path cwd,
            Clock clock,
            SessionIdGenerator idGenerator,
            JsonlOperationAbortNotifier abortNotifier,
            boolean markerPollingEnabled
    ) {
        this(
                sessionsRoot, cwd, clock, idGenerator, abortNotifier,
                markerPollingEnabled
                        ? MarkerObservationMode.POLLING
                        : MarkerObservationMode.DISABLED
        );
    }

    public JsonlSessionRepository(
            Path sessionsRoot,
            Path cwd,
            Clock clock,
            SessionIdGenerator idGenerator,
            JsonlOperationAbortNotifier abortNotifier,
            MarkerObservationMode markerObservationMode
    ) {
        this(
                sessionsRoot, cwd, clock, idGenerator, abortNotifier,
                markerObservationMode,
                path -> path.getFileSystem().newWatchService()
        );
    }

    JsonlSessionRepository(
            Path sessionsRoot,
            Path cwd,
            Clock clock,
            SessionIdGenerator idGenerator,
            JsonlOperationAbortNotifier abortNotifier,
            MarkerObservationMode markerObservationMode,
            WatchServiceFactory watchServiceFactory
    ) {
        this.sessionsRoot = sessionsRoot.toAbsolutePath().normalize();
        this.cwd = cwd.toAbsolutePath().normalize();
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
        this.idGenerator = java.util.Objects.requireNonNull(
                idGenerator, "idGenerator"
        );
        this.abortNotifier = java.util.Objects.requireNonNull(
                abortNotifier, "abortNotifier"
        );
        this.markerObservationMode = java.util.Objects.requireNonNull(
                markerObservationMode, "markerObservationMode"
        );
        this.watchServiceFactory = java.util.Objects.requireNonNull(
                watchServiceFactory, "watchServiceFactory"
        );
    }

    @ExperimentalSessionApi
    public MarkerObservationDiagnostics markerObservationDiagnostics() {
        return new MarkerObservationDiagnostics(
                watchServiceStarts.sum(), watchServiceFallbacks.sum(),
                pollingStarts.sum(), durableAdvisoriesAccepted.sum(),
                durableAdvisoriesRejected.sum()
        );
    }

    @Override
    public CompletionStage<AgentSession> create(CreateOptions options) {
        CreateOptions effective = options == null ? CreateOptions.DEFAULT : options;
        return stage(() -> {
            String id = effective.id() == null ? idGenerator.next() : effective.id();
            validateId(id);
            claim(id);
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(idLeasePath(id))) {
                if (findPath(id) != null) {
                    throw error(SessionError.Code.ALREADY_EXISTS,
                            "Session already exists: " + id);
                }
                long createdAt = clock.millis();
                Files.createDirectories(sessionDirectory());
                Path path = freshSessionPath(createdAt, id);
                JsonlSessionCodec.Header header = new JsonlSessionCodec.Header(
                        id, createdAt, cwd.toString(), effective.parentSessionId(), null
                );
                InMemorySessionState state = new InMemorySessionState(
                        metadata(header), clock
                );
                writeSnapshot(path, header, state);
                return load(path);
            } catch (SessionError failure) {
                throw failure;
            } catch (IOException failure) {
                throw storage("Failed to create session " + id, failure);
            } finally {
                release(id);
            }
        });
    }

    @Override
    public CompletionStage<AgentSession> open(SessionMetadata metadata) {
        return stage(() -> {
            Path path = findPath(metadata.id());
            if (path == null) {
                throw error(SessionError.Code.NOT_FOUND,
                        "Session not found: " + metadata.id());
            }
            AgentSession session = load(path);
            SessionMetadata loaded = join(session.metadata());
            if (!loaded.id().equals(metadata.id())) {
                throw error(SessionError.Code.INVALID_ENTRY,
                        "Session id does not match header: " + metadata.id());
            }
            return session;
        });
    }

    @Override
    public CompletionStage<List<SessionMetadata>> list() {
        return stage(() -> {
            if (!Files.exists(sessionsRoot)) return List.of();
            ArrayList<Listed> listed = new ArrayList<>();
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(path -> Files.isRegularFile(path)
                                && path.getFileName().toString().endsWith(".jsonl"))
                        .forEach(path -> {
                            try {
                                String first = firstLine(path);
                                if (first == null) return;
                                JsonlSessionCodec.Header header = JsonlSessionCodec.decodeHeader(first);
                                listed.add(new Listed(
                                        metadata(header), Files.getLastModifiedTime(path).toMillis()
                                ));
                            } catch (RuntimeException | IOException ignored) {
                                // Listing skips malformed sessions; open reports the corruption.
                            }
                        });
            } catch (IOException failure) {
                throw storage("Failed to list sessions " + sessionsRoot, failure);
            }
            return listed.stream()
                    .sorted(Comparator.comparingLong(Listed::modifiedAt).reversed())
                    .map(Listed::metadata)
                    .toList();
        });
    }

    @Override
    public CompletionStage<Void> delete(SessionMetadata metadata) {
        return stage(() -> {
            validateId(metadata.id());
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(
                    idLeasePath(metadata.id())
            )) {
                Path path = findPath(metadata.id());
                if (path != null) {
                    try (JsonlWriterLease sessionLease =
                                 JsonlWriterLease.acquire(path)) {
                        Files.deleteIfExists(path);
                    }
                }
            } catch (IOException failure) {
                throw storage("Failed to delete session " + metadata.id(), failure);
            }
            return null;
        });
    }

    @ExperimentalSessionApi
    public CompletionStage<MaintenanceReport> inspectMaintenance() {
        return inspectMaintenance(MaintenanceQuery.ALL);
    }

    @ExperimentalSessionApi
    public CompletionStage<MaintenanceReport> inspectMaintenance(
            MaintenanceQuery query
    ) {
        if (query == null) {
            return CompletableFuture.failedFuture(new NullPointerException("query"));
        }
        return stage(() -> {
            if (!Files.exists(sessionsRoot)) {
                return new MaintenanceAccumulator(query).finish();
            }
            Set<Path> finalSessions = new HashSet<>();
            Set<Path> stagingTargets = new HashSet<>();
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(Files::isRegularFile).forEach(path -> {
                    Path normalized = path.toAbsolutePath().normalize();
                    if (normalized.getFileName().toString().endsWith(".jsonl")) {
                        finalSessions.add(normalized);
                    }
                    Path target = stagingTarget(normalized);
                    if (target != null) {
                        stagingTargets.add(target.toAbsolutePath().normalize());
                    }
                });
            } catch (IOException failure) {
                throw storage("Failed to inspect repository " + sessionsRoot, failure);
            }
            Set<String> persistedIds = new HashSet<>();
            for (Path session : finalSessions) {
                try {
                    String first = firstLine(session);
                    if (first != null) persistedIds.add(
                            JsonlSessionCodec.decodeHeader(first).id()
                    );
                } catch (IOException | RuntimeException ignored) {
                    // The artifact remains reportable even if its header is malformed.
                }
            }
            MaintenanceAccumulator accumulator = new MaintenanceAccumulator(query);
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(Files::isRegularFile).forEach(file -> {
                    MaintenanceArtifact artifact = maintenanceArtifact(
                            file, finalSessions, stagingTargets, persistedIds
                    );
                    if (artifact != null) accumulator.accept(artifact);
                });
            } catch (IOException failure) {
                throw storage("Failed to inspect repository " + sessionsRoot, failure);
            }
            return accumulator.finish();
        });
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryCheckpoint> createRecoveryCheckpoint(
            Path destination
    ) {
        if (destination == null) {
            return CompletableFuture.failedFuture(new NullPointerException("destination"));
        }
        return stage(() -> {
            Path target = destination.toAbsolutePath().normalize();
            if (target.startsWith(sessionsRoot)) {
                throw new IllegalArgumentException(
                        "Recovery checkpoint must be outside the sessions root"
                );
            }
            ArrayList<Path> paths = new ArrayList<>();
            if (Files.exists(sessionsRoot)) {
                try (Stream<Path> stream = Files.walk(sessionsRoot)) {
                    stream.filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString()
                                    .endsWith(".jsonl"))
                            .map(path -> path.toAbsolutePath().normalize())
                            .forEach(paths::add);
                } catch (IOException failure) {
                    throw storage("Failed to scan recovery checkpoint sources", failure);
                }
            }
            paths.sort(Comparator.comparing(Path::toString));
            ArrayList<RecoveryGenerationFingerprint> generations = new ArrayList<>();
            for (Path path : paths) {
                try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
                    if (!Files.exists(path)) continue;
                    generations.add(recoveryFingerprint(path));
                } catch (IOException failure) {
                    throw storage("Failed to fingerprint session " + path, failure);
                }
            }
            RecoveryCheckpoint checkpoint = new RecoveryCheckpoint(
                    RECOVERY_CHECKPOINT_VERSION, clock.millis(),
                    sessionsRoot, generations
            );
            writeRecoveryCheckpoint(target, checkpoint);
            return checkpoint;
        });
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryCheckpoint> readRecoveryCheckpoint(Path source) {
        if (source == null) {
            return CompletableFuture.failedFuture(new NullPointerException("source"));
        }
        return stage(() -> {
            Path path = source.toAbsolutePath().normalize();
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
                return decodeRecoveryCheckpoint(Files.readString(
                        path, StandardCharsets.UTF_8
                ));
            } catch (IOException failure) {
                throw storage("Failed to read recovery checkpoint " + path, failure);
            }
        });
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryCheckpointReport> verifyRecoveryCheckpoint(
            RecoveryCheckpoint checkpoint,
            Integer limit
    ) {
        if (limit != null && limit <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "limit must be positive"
            ));
        }
        return verifyRecoveryCheckpointPage(
                checkpoint, new RecoveryCheckpointQuery(limit, null)
        );
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryCheckpointReport> verifyRecoveryCheckpointPage(
            RecoveryCheckpoint checkpoint,
            RecoveryCheckpointQuery query
    ) {
        if (checkpoint == null) {
            return CompletableFuture.failedFuture(new NullPointerException("checkpoint"));
        }
        if (query == null) {
            return CompletableFuture.failedFuture(new NullPointerException("query"));
        }
        return stage(() -> verifyRecoveryCheckpointNow(checkpoint, query));
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryCheckpointBatchReport>
    verifyRecoveryCheckpointBatch(
            RecoveryCheckpoint checkpoint,
            RecoveryCheckpointScanQuery query
    ) {
        if (checkpoint == null) {
            return CompletableFuture.failedFuture(new NullPointerException("checkpoint"));
        }
        if (query == null) {
            return CompletableFuture.failedFuture(new NullPointerException("query"));
        }
        return stage(() -> verifyRecoveryCheckpointBatchNow(checkpoint, query));
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryBatchReport> inspectRecoveryBatch(
            RecoveryScanQuery query
    ) {
        if (query == null) {
            return CompletableFuture.failedFuture(new NullPointerException("query"));
        }
        return stage(() -> {
            RecoveryAccumulator accumulator = new RecoveryAccumulator(
                    query.recovery()
            );
            if (!Files.exists(sessionsRoot)) {
                return new RecoveryBatchReport(accumulator.finish(), null, true);
            }
            Comparator<Path> order = Comparator.comparing(Path::toString);
            PriorityQueue<Path> selected = new PriorityQueue<>(
                    query.maxSessions(), order.reversed()
            );
            long eligible = 0;
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                java.util.Iterator<Path> iterator = paths
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString()
                                .endsWith(".jsonl"))
                        .map(path -> path.toAbsolutePath().normalize())
                        .iterator();
                while (iterator.hasNext()) {
                    Path path = iterator.next();
                    if (query.after() != null
                            && path.toString().compareTo(
                            query.after().generationPath().toString()) <= 0) {
                        continue;
                    }
                    eligible++;
                    if (selected.size() < query.maxSessions()) {
                        selected.add(path);
                    } else if (order.compare(path, selected.element()) < 0) {
                        selected.remove();
                        selected.add(path);
                    }
                }
            } catch (IOException failure) {
                throw storage("Failed to scan recovery batch "
                        + sessionsRoot, failure);
            }
            ArrayList<Path> batch = new ArrayList<>(selected);
            batch.sort(order);
            long budgetNanos;
            try {
                budgetNanos = query.maxDuration() == null
                        ? Long.MAX_VALUE : query.maxDuration().toNanos();
            } catch (ArithmeticException ignored) {
                budgetNanos = Long.MAX_VALUE;
            }
            long started = System.nanoTime();
            int inspected = 0;
            Path last = null;
            for (Path path : batch) {
                if (inspected > 0
                        && System.nanoTime() - started >= budgetNanos) {
                    break;
                }
                inspectRecoveryPath(path, accumulator);
                inspected++;
                last = path;
            }
            boolean complete = inspected == eligible;
            RecoveryScanCursor next = complete || last == null
                    ? null : new RecoveryScanCursor(last);
            return new RecoveryBatchReport(
                    accumulator.finish(), next, complete
            );
        });
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryReport> inspectRecovery() {
        return inspectRecovery(RecoveryQuery.ALL);
    }

    @ExperimentalSessionApi
    public CompletionStage<RecoveryReport> inspectRecovery(RecoveryQuery query) {
        if (query == null) {
            return CompletableFuture.failedFuture(new NullPointerException("query"));
        }
        return stage(() -> {
            RecoveryAccumulator accumulator = new RecoveryAccumulator(query);
            if (!Files.exists(sessionsRoot)) return accumulator.finish();
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString()
                                .endsWith(".jsonl"))
                        .forEach(path -> inspectRecoveryPath(path, accumulator));
            } catch (IOException failure) {
                throw storage("Failed to inspect repository recovery "
                        + sessionsRoot, failure);
            }
            return accumulator.finish();
        });
    }

    private void inspectRecoveryPath(
            Path path,
            RecoveryAccumulator accumulator
    ) {
        accumulator.sessionScanned();
        AgentSession session;
        try {
            session = loadForInspection(path);
        } catch (RuntimeException failure) {
            accumulator.failure(new RecoveryFailure(
                    path.toAbsolutePath().normalize(),
                    recoverySessionId(path),
                    failure instanceof SessionError error
                            ? error.code() : SessionError.Code.STORAGE,
                    failure.getMessage() == null
                            ? failure.getClass().getSimpleName()
                            : failure.getMessage()
            ));
            return;
        }
        SessionMetadata sessionMetadata = join(session.metadata());
        try {
            for (SessionOperationInspector.OpenOperation operation
                    : join(SessionOperationInspector.inspect(session))) {
                accumulator.operation(new RecoveryOperation(
                        sessionMetadata,
                        path.toAbsolutePath().normalize(), operation
                ));
            }
        } catch (RuntimeException failure) {
            accumulator.failure(new RecoveryFailure(
                    path.toAbsolutePath().normalize(), sessionMetadata.id(),
                    failure instanceof SessionError error
                            ? error.code() : SessionError.Code.STORAGE,
                    failure.getMessage() == null
                            ? failure.getClass().getSimpleName()
                            : failure.getMessage()
            ));
        } finally {
            session.close();
        }
    }

    private static String recoverySessionId(Path path) {
        try {
            String first = firstLine(path);
            return first == null ? null : JsonlSessionCodec.decodeHeader(first).id();
        } catch (IOException | RuntimeException ignored) {
            return null;
        }
    }

    @ExperimentalSessionApi
    public CompletionStage<CleanupResult> cleanupOrphanedStaging() {
        return cleanupOrphanedStaging(StagingCleanupPolicy.IMMEDIATE);
    }

    @ExperimentalSessionApi
    public CompletionStage<CleanupResult> cleanupOrphanedStaging(
            StagingCleanupPolicy policy
    ) {
        if (policy == null) {
            return CompletableFuture.failedFuture(new NullPointerException("policy"));
        }
        return stage(() -> {
            if (!Files.exists(sessionsRoot)) return new CleanupResult(0, 0);
            ArrayList<Path> candidates = new ArrayList<>();
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> stagingTarget(path) != null)
                        .forEach(candidates::add);
            } catch (IOException failure) {
                throw storage("Failed to scan staging files " + sessionsRoot, failure);
            }
            int deleted = 0;
            long cutoff;
            try {
                cutoff = Math.subtractExact(
                        clock.millis(), policy.minimumAge().toMillis()
                );
            } catch (ArithmeticException failure) {
                cutoff = Long.MIN_VALUE;
            }
            for (Path candidate : candidates) {
                Path target = stagingTarget(candidate);
                try (JsonlWriterLease ignored = JsonlWriterLease.acquire(target)) {
                    BasicFileAttributes attributes;
                    try {
                        attributes = Files.readAttributes(
                                candidate, BasicFileAttributes.class
                        );
                    } catch (java.nio.file.NoSuchFileException ignoredMissing) {
                        continue;
                    }
                    if (attributes.lastModifiedTime().toMillis() <= cutoff
                            && Files.deleteIfExists(candidate)) {
                        deleted++;
                    }
                } catch (IOException failure) {
                    throw storage("Failed to clean staging file " + candidate, failure);
                }
            }
            return new CleanupResult(candidates.size(), deleted);
        });
    }

    @ExperimentalSessionApi
    public CompletionStage<OperationSignalCleanupResult>
    cleanupUnassociatedOperationSignals() {
        return cleanupUnassociatedOperationSignals(
                OperationSignalCleanupPolicy.IMMEDIATE
        );
    }

    @ExperimentalSessionApi
    public CompletionStage<OperationSignalCleanupResult>
    cleanupUnassociatedOperationSignals(OperationSignalCleanupPolicy policy) {
        if (policy == null) {
            return CompletableFuture.failedFuture(new NullPointerException("policy"));
        }
        return stage(() -> {
            if (!Files.exists(sessionsRoot)) {
                return new OperationSignalCleanupResult(0, 0, 0, 0);
            }
            ArrayList<OperationSignalTarget> candidates = new ArrayList<>();
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(Files::isRegularFile).forEach(path -> {
                    OperationSignalTarget target = operationSignalTarget(path);
                    if (target != null) candidates.add(target);
                });
            } catch (IOException failure) {
                throw storage("Failed to scan operation abort signals "
                        + sessionsRoot, failure);
            }
            long cutoff;
            try {
                cutoff = Math.subtractExact(
                        clock.millis(), policy.minimumAge().toMillis()
                );
            } catch (ArithmeticException failure) {
                cutoff = Long.MIN_VALUE;
            }
            int deleted = 0;
            int associated = 0;
            int tooYoung = 0;
            for (OperationSignalTarget candidate : candidates) {
                try (JsonlWriterLease operation = JsonlWriterLease.acquire(
                        candidate.operationLeaseBase()
                ); JsonlWriterLease generation = JsonlWriterLease.acquire(
                        candidate.sessionPath()
                )) {
                    BasicFileAttributes attributes;
                    try {
                        attributes = Files.readAttributes(
                                candidate.marker(), BasicFileAttributes.class
                        );
                    } catch (java.nio.file.NoSuchFileException ignored) {
                        continue;
                    }
                    if (Files.exists(candidate.sessionPath())) {
                        associated++;
                    } else if (attributes.lastModifiedTime().toMillis() > cutoff) {
                        tooYoung++;
                    } else if (Files.deleteIfExists(candidate.marker())) {
                        deleted++;
                    }
                } catch (IOException failure) {
                    throw storage("Failed to clean operation abort signal "
                            + candidate.marker(), failure);
                }
            }
            return new OperationSignalCleanupResult(
                    candidates.size(), deleted, associated, tooYoung
            );
        });
    }

    @Override
    public CompletionStage<AgentSession> copyRetained(
            SessionMetadata source,
            SessionRetainedCopyOptions options
    ) {
        return stage(() -> {
            if (options == null) {
                throw new NullPointerException("options");
            }
            Path sourcePath = findPath(source.id());
            if (sourcePath == null) {
                throw error(SessionError.Code.NOT_FOUND,
                        "Session not found: " + source.id());
            }
            AgentSession loaded = load(sourcePath);
            String id = options.id() == null ? idGenerator.next() : options.id();
            validateId(id);
            claim(id);
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(idLeasePath(id))) {
                if (findPath(id) != null) {
                    throw error(SessionError.Code.ALREADY_EXISTS,
                            "Session already exists: " + id);
                }
                long createdAt = clock.millis();
                Files.createDirectories(sessionDirectory());
                Path destination = freshSessionPath(createdAt, id);
                SessionMetadata targetMetadata = new SessionMetadata(
                        id, createdAt, SessionMetadata.CURRENT_STORAGE_VERSION,
                        options.parentSessionId() == null
                                ? source.id() : options.parentSessionId()
                );
                InMemorySessionState snapshot = loaded.state().retainedCopy(
                        targetMetadata, options
                );
                JsonlSessionCodec.Header header = new JsonlSessionCodec.Header(
                        id, createdAt, cwd.toString(), targetMetadata.parentSessionId(), null
                );
                writeSnapshot(destination, header, snapshot);
                return load(destination);
            } catch (SessionError failure) {
                throw failure;
            } catch (IOException failure) {
                throw storage("Failed to copy retained session " + source.id(), failure);
            } finally {
                release(id);
            }
        });
    }

    @Override
    public CompletionStage<AgentSession> fork(
            SessionMetadata source,
            SessionForkOptions options
    ) {
        return stage(() -> {
            Path sourcePath = findPath(source.id());
            if (sourcePath == null) {
                throw error(SessionError.Code.NOT_FOUND,
                        "Session not found: " + source.id());
            }
            AgentSession loaded = load(sourcePath);
            String id = options.id() == null ? idGenerator.next() : options.id();
            validateId(id);
            claim(id);
            Path destination = null;
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(idLeasePath(id))) {
                if (findPath(id) != null) {
                    throw error(SessionError.Code.ALREADY_EXISTS,
                            "Session already exists: " + id);
                }
                long createdAt = clock.millis();
                Files.createDirectories(sessionDirectory());
                destination = freshSessionPath(createdAt, id);
                Path staged = Path.of(destination + ".tmp");
                SessionMetadata targetMetadata = new SessionMetadata(
                        id, createdAt, SessionMetadata.CURRENT_STORAGE_VERSION,
                        options.parentSessionId() == null ? source.id() : options.parentSessionId()
                );
                InMemorySessionState snapshot = loaded.state().fork(targetMetadata, options);
                JsonlSessionCodec.Header header = new JsonlSessionCodec.Header(
                        id, createdAt, cwd.toString(), targetMetadata.parentSessionId(), null
                );
                try (JsonlWriterLease destinationLease =
                             JsonlWriterLease.acquire(destination)) {
                    Files.deleteIfExists(staged);
                    try {
                        Files.writeString(
                                staged, JsonlSessionCodec.encodeHeader(header),
                                StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE
                        );
                        for (SessionLogItem mutation : snapshot.log(0, null)) {
                            Files.writeString(
                                    staged, JsonlSessionCodec.encodeMutation(mutation),
                                    StandardCharsets.UTF_8, StandardOpenOption.APPEND
                            );
                        }
                        publish(staged, destination);
                    } catch (Throwable failure) {
                        Files.deleteIfExists(staged);
                        throw failure;
                    }
                }
                return load(destination);
            } catch (SessionError failure) {
                throw failure;
            } catch (IOException failure) {
                throw storage("Failed to fork session " + source.id(), failure);
            } finally {
                release(id);
            }
        });
    }

    private RecoveryCheckpointBatchReport verifyRecoveryCheckpointBatchNow(
            RecoveryCheckpoint checkpoint,
            RecoveryCheckpointScanQuery query
    ) {
        validateRecoveryCheckpoint(checkpoint);
        java.util.LinkedHashMap<String, RecoveryGenerationFingerprint> expected =
                new java.util.LinkedHashMap<>();
        for (RecoveryGenerationFingerprint fingerprint : checkpoint.generations()) {
            expected.put(fingerprint.relativePath(), fingerprint);
        }
        Comparator<String> order = Comparator.naturalOrder();
        PriorityQueue<String> selected = new PriorityQueue<>(
                query.maxGenerations(), order.reversed()
        );
        long eligible = 0;
        for (String relative : expected.keySet()) {
            if (query.after() != null
                    && relative.compareTo(query.after().relativePath()) <= 0) {
                continue;
            }
            eligible++;
            retainSmallest(selected, relative, query.maxGenerations(), order);
        }
        if (Files.exists(sessionsRoot)) {
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                java.util.Iterator<Path> iterator = paths
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString()
                                .endsWith(".jsonl"))
                        .iterator();
                while (iterator.hasNext()) {
                    String relative = recoveryRelativePath(iterator.next());
                    if (expected.containsKey(relative)
                            || query.after() != null
                            && relative.compareTo(query.after().relativePath()) <= 0) {
                        continue;
                    }
                    eligible++;
                    retainSmallest(
                            selected, relative, query.maxGenerations(), order
                    );
                }
            } catch (IOException failure) {
                throw storage("Failed to scan checkpoint verification batch", failure);
            }
        }
        ArrayList<String> batch = new ArrayList<>(selected);
        batch.sort(order);
        long budgetNanos = durationNanos(query.maxDuration());
        long started = System.nanoTime();
        int inspected = 0;
        String last = null;
        RecoveryCheckpointAccumulator accumulator =
                new RecoveryCheckpointAccumulator(query.verification());
        for (String relative : batch) {
            if (inspected > 0
                    && System.nanoTime() - started >= budgetNanos) {
                break;
            }
            verifyCheckpointCandidate(
                    relative, expected.get(relative), accumulator
            );
            inspected++;
            last = relative;
        }
        boolean complete = inspected == eligible;
        RecoveryCheckpointScanCursor next = complete || last == null
                ? null : new RecoveryCheckpointScanCursor(last);
        return new RecoveryCheckpointBatchReport(
                accumulator.finish(), inspected, next, complete
        );
    }

    private static <T> void retainSmallest(
            PriorityQueue<T> retained,
            T value,
            int limit,
            Comparator<T> order
    ) {
        if (retained.size() < limit) {
            retained.add(value);
        } else if (order.compare(value, retained.element()) < 0) {
            retained.remove();
            retained.add(value);
        }
    }

    private static long durationNanos(Duration duration) {
        if (duration == null) return Long.MAX_VALUE;
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private void verifyCheckpointCandidate(
            String relative,
            RecoveryGenerationFingerprint expected,
            RecoveryCheckpointAccumulator accumulator
    ) {
        Path path = checkpointPath(relative);
        if (!Files.exists(path)) {
            if (expected != null) {
                accumulator.accept(new RecoveryCheckpointDetail(
                        relative, CheckpointStatus.MISSING, expected, null
                ));
            }
            return;
        }
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
            if (!Files.exists(path)) {
                if (expected != null) {
                    accumulator.accept(new RecoveryCheckpointDetail(
                            relative, CheckpointStatus.MISSING, expected, null
                    ));
                }
                return;
            }
            RecoveryGenerationFingerprint current = recoveryFingerprint(path);
            if (expected == null) {
                accumulator.accept(new RecoveryCheckpointDetail(
                        relative, CheckpointStatus.ADDED, null, current
                ));
            } else {
                accumulator.accept(new RecoveryCheckpointDetail(
                        relative,
                        expected.equals(current)
                                ? CheckpointStatus.UNCHANGED
                                : CheckpointStatus.CHANGED,
                        expected, current
                ));
            }
        } catch (IOException failure) {
            throw storage("Failed to verify checkpoint generation " + path, failure);
        }
    }

    private void validateRecoveryCheckpoint(RecoveryCheckpoint checkpoint) {
        if (checkpoint.version() != RECOVERY_CHECKPOINT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported recovery checkpoint version " + checkpoint.version()
            );
        }
        if (!checkpoint.repositoryRoot().equals(sessionsRoot)) {
            throw new IllegalArgumentException(
                    "Recovery checkpoint belongs to " + checkpoint.repositoryRoot()
                            + ", not " + sessionsRoot
            );
        }
    }

    private RecoveryCheckpointReport verifyRecoveryCheckpointNow(
            RecoveryCheckpoint checkpoint,
            RecoveryCheckpointQuery query
    ) {
        validateRecoveryCheckpoint(checkpoint);
        RecoveryCheckpointAccumulator accumulator =
                new RecoveryCheckpointAccumulator(query);
        java.util.LinkedHashMap<String, RecoveryGenerationFingerprint> expected =
                new java.util.LinkedHashMap<>();
        for (RecoveryGenerationFingerprint fingerprint : checkpoint.generations()) {
            expected.put(fingerprint.relativePath(), fingerprint);
            Path path = checkpointPath(fingerprint.relativePath());
            if (!Files.exists(path)) {
                accumulator.accept(new RecoveryCheckpointDetail(
                        fingerprint.relativePath(), CheckpointStatus.MISSING,
                        fingerprint, null
                ));
                continue;
            }
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
                if (!Files.exists(path)) {
                    accumulator.accept(new RecoveryCheckpointDetail(
                            fingerprint.relativePath(), CheckpointStatus.MISSING,
                            fingerprint, null
                    ));
                    continue;
                }
                RecoveryGenerationFingerprint current = recoveryFingerprint(path);
                accumulator.accept(new RecoveryCheckpointDetail(
                        fingerprint.relativePath(),
                        fingerprint.equals(current)
                                ? CheckpointStatus.UNCHANGED
                                : CheckpointStatus.CHANGED,
                        fingerprint, current
                ));
            } catch (IOException failure) {
                throw storage("Failed to verify checkpoint generation " + path, failure);
            }
        }
        if (Files.exists(sessionsRoot)) {
            try (Stream<Path> paths = Files.walk(sessionsRoot)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString()
                                .endsWith(".jsonl"))
                        .forEach(path -> {
                            String relative = recoveryRelativePath(path);
                            if (expected.containsKey(relative)) return;
                            try (JsonlWriterLease ignored =
                                         JsonlWriterLease.acquire(path)) {
                                if (!Files.exists(path)) return;
                                accumulator.accept(new RecoveryCheckpointDetail(
                                        relative, CheckpointStatus.ADDED,
                                        null, recoveryFingerprint(path)
                                ));
                            } catch (IOException failure) {
                                throw storage(
                                        "Failed to verify added generation " + path,
                                        failure
                                );
                            }
                        });
            } catch (IOException failure) {
                throw storage("Failed to scan checkpoint additions", failure);
            }
        }
        return accumulator.finish();
    }

    private Path checkpointPath(String relativePath) {
        Path relative = Path.of(relativePath);
        if (relative.isAbsolute()) {
            throw new IllegalArgumentException(
                    "Checkpoint generation path must be relative: " + relativePath
            );
        }
        Path resolved = sessionsRoot.resolve(relative).normalize();
        if (!resolved.startsWith(sessionsRoot)) {
            throw new IllegalArgumentException(
                    "Checkpoint generation escapes repository: " + relativePath
            );
        }
        return resolved;
    }

    private RecoveryGenerationFingerprint recoveryFingerprint(Path path)
            throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        String sessionId = null;
        Long tailSequence = null;
        try {
            String first = firstLine(path);
            if (first != null) sessionId = JsonlSessionCodec.decodeHeader(first).id();
        } catch (RuntimeException ignored) {
        }
        try {
            TailLine tail = tailLine(path);
            if (tail.hasPreviousLine()) {
                tailSequence = JsonlSessionCodec.decodeMutation(tail.value()).sequence();
            } else {
                tailSequence = 0L;
            }
        } catch (IOException | RuntimeException ignored) {
        }
        return new RecoveryGenerationFingerprint(
                recoveryRelativePath(path), sessionId, tailSequence,
                bytes.length, sha256(bytes)
        );
    }

    private String recoveryRelativePath(Path path) {
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(sessionsRoot)) {
            throw new IllegalArgumentException(
                    "Generation is outside repository: " + normalized
            );
        }
        return sessionsRoot.relativize(normalized).toString();
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes)
            );
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private void writeRecoveryCheckpoint(
            Path destination,
            RecoveryCheckpoint checkpoint
    ) {
        Path staged = Path.of(destination + ".tmp");
        try {
            Files.createDirectories(destination.getParent());
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(destination)) {
                Files.deleteIfExists(staged);
                try (FileChannel channel = FileChannel.open(
                        staged, StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE
                )) {
                    write(channel, encodeRecoveryCheckpoint(checkpoint));
                    channel.force(true);
                }
                publishTransaction(staged, destination);
            }
        } catch (IOException failure) {
            try {
                Files.deleteIfExists(staged);
            } catch (IOException ignored) {
            }
            throw storage("Failed to publish recovery checkpoint "
                    + destination, failure);
        }
    }

    private static String encodeRecoveryCheckpoint(RecoveryCheckpoint checkpoint) {
        ObjectNode root = MAPPER.createObjectNode()
                .put("version", checkpoint.version())
                .put("capturedAt", checkpoint.capturedAt())
                .put("repositoryRoot", checkpoint.repositoryRoot().toString())
                .put("consistency", "per_generation");
        ArrayNode generations = root.putArray("generations");
        for (RecoveryGenerationFingerprint fingerprint : checkpoint.generations()) {
            ObjectNode item = generations.addObject()
                    .put("relativePath", fingerprint.relativePath())
                    .put("size", fingerprint.size())
                    .put("sha256", fingerprint.sha256());
            if (fingerprint.sessionId() == null) item.putNull("sessionId");
            else item.put("sessionId", fingerprint.sessionId());
            if (fingerprint.tailSequence() == null) item.putNull("tailSequence");
            else item.put("tailSequence", fingerprint.tailSequence());
        }
        try {
            return MAPPER.writeValueAsString(root) + "\n";
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("Failed to encode recovery checkpoint", failure);
        }
    }

    private static RecoveryCheckpoint decodeRecoveryCheckpoint(String value) {
        try {
            JsonNode root = MAPPER.readTree(value);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("Checkpoint must be a JSON object");
            }
            int version = root.path("version").asInt(-1);
            long capturedAt = root.path("capturedAt").asLong(-1);
            String repositoryRoot = root.path("repositoryRoot").asText(null);
            JsonNode values = root.path("generations");
            if (capturedAt < 0 || repositoryRoot == null || !values.isArray()) {
                throw new IllegalArgumentException("Invalid recovery checkpoint payload");
            }
            ArrayList<RecoveryGenerationFingerprint> generations = new ArrayList<>();
            for (JsonNode item : values) {
                JsonNode tail = item.get("tailSequence");
                JsonNode session = item.get("sessionId");
                generations.add(new RecoveryGenerationFingerprint(
                        item.path("relativePath").asText(null),
                        session == null || session.isNull() ? null : session.asText(),
                        tail == null || tail.isNull() ? null : tail.longValue(),
                        item.path("size").asLong(-1),
                        item.path("sha256").asText(null)
                ));
            }
            return new RecoveryCheckpoint(
                    version, capturedAt, Path.of(repositoryRoot), generations
            );
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Invalid recovery checkpoint JSON", failure
            );
        }
    }

    private void writeSnapshot(
            Path destination,
            JsonlSessionCodec.Header header,
            InMemorySessionState snapshot
    ) throws IOException {
        Path staged = Path.of(destination + ".tmp");
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(destination)) {
            Files.deleteIfExists(staged);
            try (FileChannel channel = FileChannel.open(
                    staged, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE
            )) {
                write(channel, JsonlSessionCodec.encodeHeader(header));
                for (SessionLogItem mutation : snapshot.log(0, null)) {
                    write(channel, JsonlSessionCodec.encodeMutation(mutation));
                }
                channel.force(true);
            } catch (Throwable failure) {
                Files.deleteIfExists(staged);
                throw failure;
            }
            try {
                publishTransaction(staged, destination);
            } catch (Throwable failure) {
                Files.deleteIfExists(staged);
                throw failure;
            }
        }
    }

    private static void write(FileChannel channel, String value) throws IOException {
        ByteBuffer encoded = StandardCharsets.UTF_8.encode(value);
        while (encoded.hasRemaining()) channel.write(encoded);
    }

    private AgentSession load(Path path) {
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
            return loadUnderLease(path);
        } catch (SessionError failure) {
            throw failure;
        } catch (IOException failure) {
            throw storage("Failed to acquire session writer lease " + path, failure);
        }
    }

    private AgentSession loadForInspection(Path path) {
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
            return loadUnderLease(path, false);
        } catch (SessionError failure) {
            throw failure;
        } catch (IOException failure) {
            throw storage("Failed to acquire session inspection lease " + path, failure);
        }
    }

    private AgentSession loadUnderLease(Path path) {
        return loadUnderLease(path, true);
    }

    private AgentSession loadUnderLease(Path path, boolean repairTail) {
        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            String[] physical = content.split("\\n", -1);
            int lineCount = physical.length;
            if (lineCount > 0 && physical[lineCount - 1].isEmpty()) lineCount--;
            if (lineCount == 0 || physical[0].isEmpty()) {
                throw invalidFile(path, 1, "is missing a header", null);
            }
            JsonlSessionCodec.Header header;
            try {
                header = JsonlSessionCodec.decodeHeader(physical[0]);
            } catch (RuntimeException failure) {
                throw invalidFile(path, 1, failure.getMessage(), failure);
            }
            SessionMetadata metadata = metadata(header);
            InMemorySessionState state = new InMemorySessionState(
                    metadata, clock, persistence(path)
            );
            for (int index = 1; index < lineCount; index++) {
                try {
                    state.replay(JsonlSessionCodec.decodeMutation(physical[index]));
                } catch (JsonlSessionCodec.JsonlSyntaxException failure) {
                    if (repairTail && index == lineCount - 1) {
                        StringBuilder valid = new StringBuilder();
                        for (int prefix = 0; prefix < index; prefix++) {
                            valid.append(physical[prefix]).append('\n');
                        }
                        rewriteAtomically(path, valid.toString());
                        return new AgentSession(state, idGenerator, "main");
                    }
                    throw invalidFile(path, index + 1, failure.getMessage(), failure);
                } catch (RuntimeException failure) {
                    throw invalidFile(path, index + 1, failure.getMessage(), failure);
                }
            }
            if (repairTail && !content.endsWith("\n")) {
                Files.writeString(
                        path, "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND
                );
            }
            return new AgentSession(state, idGenerator, "main");
        } catch (SessionError failure) {
            throw failure;
        } catch (IOException failure) {
            throw storage("Failed to read session " + path, failure);
        }
    }

    private InMemorySessionState.PersistenceSink persistence(Path path) {
        return new InMemorySessionState.PersistenceSink() {
            @Override
            public AutoCloseable acquireOperationExecution(
                    String lane,
                    String runId,
                    long expectedSequence
            ) {
                JsonlWriterLease lease = null;
                try {
                    lease = JsonlWriterLease.acquire(
                            operationLeasePath(path, lane, runId)
                    );
                    verifySequence(path, expectedSequence);
                    return lease;
                } catch (RuntimeException | Error failure) {
                    if (lease != null) lease.close();
                    throw failure;
                } catch (IOException failure) {
                    if (lease != null) lease.close();
                    throw storage(
                            "Failed to acquire operation execution lease "
                                    + path + " for run " + runId,
                            failure
                    );
                }
            }

            @Override
            public List<SessionLogItem> reconcileOperationAbort(
                    String lane,
                    String runId,
                    long expectedSequence
            ) {
                try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
                    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
                    ArrayList<SessionLogItem> suffix = new ArrayList<>();
                    long expected = expectedSequence + 1;
                    for (int index = 1; index < lines.size(); index++) {
                        String line = lines.get(index);
                        if (line.isEmpty()) continue;
                        SessionLogItem mutation = JsonlSessionCodec.decodeMutation(line);
                        if (mutation.sequence() <= expectedSequence) continue;
                        if (mutation.sequence() != expected++) {
                            throw new SessionError(
                                    SessionError.Code.STORAGE,
                                    "Non-consecutive JSONL suffix while reconciling abort for "
                                            + runId
                            );
                        }
                        if (!(mutation instanceof SessionLogItem.Record item)
                                || !item.record().lane().equals(lane)
                                || !(item.record().storedValue()
                                instanceof SessionRecordDraft.AbortRequested abort)
                                || !abort.runId().equals(runId)) {
                            throw new SessionError(
                                    SessionError.Code.STORAGE,
                                    "Stale JSONL session writer for " + path
                                            + ": suffix is not an abort for run " + runId
                                            + "; reopen the session"
                            );
                        }
                        suffix.add(mutation);
                    }
                    return List.copyOf(suffix);
                } catch (SessionError failure) {
                    throw failure;
                } catch (IOException | RuntimeException failure) {
                    throw storage(
                            "Failed to reconcile operation abort for " + path,
                            failure
                    );
                }
            }

            @Override
            public AutoCloseable observeOperationAbort(
                    String lane,
                    String runId,
                    Runnable cancellation
            ) {
                AtomicBoolean closed = new AtomicBoolean();
                AtomicBoolean signaled = new AtomicBoolean();
                Runnable notify = () -> {
                    if (closed.get() || !signaled.compareAndSet(false, true)) return;
                    try {
                        cancellation.run();
                    } catch (RuntimeException ignored) {
                        // The durable abort record remains authoritative.
                    }
                };
                JsonlOperationAbortNotifier.Key key =
                        new JsonlOperationAbortNotifier.Key(path, lane, runId);
                Path marker = operationAbortPath(path, lane, runId);
                Runnable externalNotify = () -> verifyAbortAdvisory(
                        path, lane, runId, null, notify
                );
                Runnable markerNotify = () -> {
                    Long sequence = markerSequence(marker);
                    if (sequence == null) {
                        durableAdvisoriesRejected.increment();
                    } else {
                        verifyAbortAdvisory(
                                path, lane, runId, sequence, notify
                        );
                    }
                };
                AutoCloseable external;
                try {
                    if (abortNotifier
                            instanceof JsonlOperationAbortNotifier.Sequenced sequenced) {
                        external = sequenced.observeNotifications(
                                key, notification -> {
                                    if (!notification.key().equals(key)) {
                                        durableAdvisoriesRejected.increment();
                                        return;
                                    }
                                    verifyAbortAdvisory(
                                            path, lane, runId,
                                            notification.sequence(), notify
                                    );
                                }
                        );
                    } else {
                        external = abortNotifier.observe(key, externalNotify);
                    }
                    if (external == null) external = () -> { };
                } catch (RuntimeException ignored) {
                    external = () -> { };
                }
                Thread observer = startMarkerObserver(
                        markerObservationMode, marker, runId,
                        closed, signaled, markerNotify, watchServiceFactory,
                        watchServiceStarts::increment,
                        watchServiceFallbacks::increment,
                        pollingStarts::increment
                );
                AutoCloseable externalSubscription = external;
                return () -> {
                    closed.set(true);
                    try {
                        externalSubscription.close();
                    } catch (Exception ignored) {
                    }
                    if (observer != null && Thread.currentThread() != observer) {
                        observer.interrupt();
                    }
                };
            }

            @Override
            public void persist(SessionLogItem mutation) {
                try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
                    verifySequence(path, mutation.sequence() - 1);
                    Files.writeString(
                            path, JsonlSessionCodec.encodeMutation(mutation),
                            StandardCharsets.UTF_8, StandardOpenOption.APPEND
                    );
                } catch (IOException failure) {
                    throw storage("Failed to append session " + path, failure);
                }
                signalAbort(path, mutation);
            }

            @Override
            public void persistBatch(List<SessionLogItem> mutations) {
                if (mutations.isEmpty()) return;
                Path staged = null;
                try (JsonlWriterLease ignored = JsonlWriterLease.acquire(path)) {
                    verifyBatch(mutations);
                    verifySequence(path, mutations.getFirst().sequence() - 1);
                    staged = Files.createTempFile(
                            path.getParent(), path.getFileName().toString(), ".txn.tmp"
                    );
                    Files.copy(path, staged, StandardCopyOption.REPLACE_EXISTING);
                    try (FileChannel channel = FileChannel.open(
                            staged, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                    )) {
                        for (SessionLogItem mutation : mutations) {
                            ByteBuffer encoded = StandardCharsets.UTF_8.encode(
                                    JsonlSessionCodec.encodeMutation(mutation)
                            );
                            while (encoded.hasRemaining()) channel.write(encoded);
                        }
                        channel.force(true);
                    }
                    publishTransaction(staged, path);
                    staged = null;
                } catch (IOException failure) {
                    throw storage("Failed to commit session transaction " + path, failure);
                } finally {
                    if (staged != null) {
                        try {
                            Files.deleteIfExists(staged);
                        } catch (IOException ignored) {
                            // The original session remains authoritative.
                        }
                    }
                }
                mutations.forEach(mutation -> signalAbort(path, mutation));
            }
        };
    }

    private static void verifyBatch(List<SessionLogItem> mutations) {
        long expected = mutations.getFirst().sequence();
        for (SessionLogItem mutation : mutations) {
            if (mutation.sequence() != expected) {
                throw new SessionError(
                        SessionError.Code.STORAGE,
                        "Session transaction has non-consecutive sequence "
                                + mutation.sequence() + "; expected " + expected
                );
            }
            expected++;
        }
    }

    private static void verifySequence(Path path, long expected) throws IOException {
        TailLine tail = tailLine(path);
        long actual = 0;
        if (tail.hasPreviousLine()) {
            try {
                actual = JsonlSessionCodec.decodeMutation(tail.value()).sequence();
            } catch (RuntimeException failure) {
                SessionError error = new SessionError(
                        SessionError.Code.STORAGE,
                        "Cannot write corrupt JSONL session " + path
                );
                error.initCause(failure);
                throw error;
            }
        }
        if (actual != expected) {
            throw new SessionError(
                    SessionError.Code.STORAGE,
                    "Stale JSONL session writer for " + path
                            + ": expected sequence " + expected
                            + " but disk is at " + actual + "; reopen the session"
            );
        }
    }

    private static TailLine tailLine(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size == 0) throw new IOException("Session file is empty: " + path);
            int window = 8 * 1024;
            while (true) {
                long start = Math.max(0, size - window);
                int length = Math.toIntExact(size - start);
                ByteBuffer bytes = ByteBuffer.allocate(length);
                channel.position(start);
                while (bytes.hasRemaining() && channel.read(bytes) != -1) {
                    // Continue until the selected tail window is complete.
                }
                bytes.flip();
                int end = bytes.limit();
                while (end > 0 && (bytes.get(end - 1) == '\n'
                        || bytes.get(end - 1) == '\r')) end--;
                int delimiter = end - 1;
                while (delimiter >= 0 && bytes.get(delimiter) != '\n') delimiter--;
                if (delimiter >= 0 || start == 0) {
                    int lineStart = delimiter + 1;
                    ByteBuffer line = bytes.slice(lineStart, end - lineStart);
                    return new TailLine(
                            StandardCharsets.UTF_8.decode(line).toString(),
                            delimiter >= 0
                    );
                }
                if (window >= Integer.MAX_VALUE / 2) {
                    throw new IOException("Final JSONL line is too large: " + path);
                }
                window = Math.min(Integer.MAX_VALUE / 2, window * 2);
            }
        }
    }

    private record TailLine(String value, boolean hasPreviousLine) {
    }

    private void rewriteAtomically(Path path, String validPrefix) throws IOException {
        Path staged = Path.of(path + ".tmp");
        try {
            Files.writeString(
                    staged, validPrefix, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
            publish(staged, path);
        } catch (Throwable failure) {
            Files.deleteIfExists(staged);
            throw failure;
        }
    }

    private static void publish(Path staged, Path destination) throws IOException {
        try {
            Files.move(
                    staged, destination, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(staged, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void publishTransaction(Path staged, Path destination) throws IOException {
        Files.move(
                staged, destination, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private Path findPath(String id) {
        if (!Files.exists(sessionsRoot)) return null;
        String suffix = "_" + id + ".jsonl";
        try (Stream<Path> paths = Files.walk(sessionsRoot)) {
            return paths.filter(path -> Files.isRegularFile(path)
                            && path.getFileName().toString().endsWith(suffix))
                    .findFirst().orElse(null);
        } catch (IOException failure) {
            throw storage("Failed to find session " + id, failure);
        }
    }

    private MaintenanceArtifact maintenanceArtifact(
            Path file,
            Set<Path> finalSessions,
            Set<Path> stagingTargets,
            Set<String> persistedIds
    ) {
        Path normalized = file.toAbsolutePath().normalize();
        String name = normalized.getFileName().toString();
        OperationSignalTarget signal = operationSignalTarget(normalized);
        ArtifactKind kind;
        Path target = null;
        String sessionId = null;
        boolean associatedDataPresent;
        if (name.endsWith(".jsonl")) {
            kind = ArtifactKind.SESSION;
            target = normalized;
            associatedDataPresent = true;
            try {
                String first = firstLine(normalized);
                if (first != null) sessionId = JsonlSessionCodec.decodeHeader(first).id();
            } catch (IOException | RuntimeException ignored) {
                // Malformed final files are still visible to maintenance tooling.
            }
        } else if (stagingTarget(normalized) != null) {
            kind = ArtifactKind.STAGING;
            target = stagingTarget(normalized).toAbsolutePath().normalize();
            associatedDataPresent = finalSessions.contains(target);
        } else if (name.endsWith(".jsonl.lock")) {
            kind = ArtifactKind.SESSION_LOCK;
            target = Path.of(normalized.toString().substring(
                    0, normalized.toString().length() - ".lock".length()
            ));
            associatedDataPresent = finalSessions.contains(target)
                    || stagingTargets.contains(target);
        } else if (signal != null) {
            kind = ArtifactKind.OPERATION_ABORT_SIGNAL;
            target = signal.sessionPath();
            associatedDataPresent = finalSessions.contains(target)
                    || stagingTargets.contains(target);
        } else if (normalized.getParent() != null
                && normalized.getParent().getFileName().toString()
                .endsWith(".jsonl.operations")
                && name.endsWith(".lock")) {
            kind = ArtifactKind.OPERATION_LOCK;
            String parent = normalized.getParent().toString();
            target = Path.of(parent.substring(
                    0, parent.length() - ".operations".length()
            )).toAbsolutePath().normalize();
            associatedDataPresent = finalSessions.contains(target)
                    || stagingTargets.contains(target);
        } else if (normalized.getParent() != null
                && normalized.getParent().equals(
                        sessionsRoot.resolve(".writer-leases").normalize()
                ) && name.endsWith(".lock")) {
            kind = ArtifactKind.SESSION_ID_LOCK;
            sessionId = name.substring(0, name.length() - ".lock".length());
            String id = sessionId;
            associatedDataPresent = persistedIds.contains(id)
                    || stagingTargets.stream().anyMatch(path -> path.getFileName()
                    .toString().endsWith("_" + id + ".jsonl"));
        } else {
            return null;
        }
        try {
            BasicFileAttributes attributes = Files.readAttributes(
                    normalized, BasicFileAttributes.class
            );
            return new MaintenanceArtifact(
                    kind, normalized, target, sessionId,
                    attributes.size(), attributes.lastModifiedTime().toMillis(),
                    associatedDataPresent
            );
        } catch (java.nio.file.NoSuchFileException ignored) {
            return null;
        } catch (IOException failure) {
            throw storage("Failed to inspect repository artifact " + normalized, failure);
        }
    }

    private static Path stagingTarget(Path candidate) {
        String path = candidate.toString();
        if (path.endsWith(".jsonl.tmp")) {
            return Path.of(path.substring(0, path.length() - ".tmp".length()));
        }
        if (!path.endsWith(".txn.tmp")) return null;
        int extension = path.lastIndexOf(".jsonl");
        if (extension < 0) return null;
        int suffixStart = extension + ".jsonl".length();
        int suffixEnd = path.length() - ".txn.tmp".length();
        if (suffixStart == suffixEnd) return null;
        for (int index = suffixStart; index < suffixEnd; index++) {
            if (!Character.isDigit(path.charAt(index))) return null;
        }
        return Path.of(path.substring(0, suffixStart));
    }

    private void verifyAbortAdvisory(
            Path sessionPath,
            String lane,
            String runId,
            Long expectedSequence,
            Runnable notify
    ) {
        if (hasDurableAbort(
                sessionPath, lane, runId, expectedSequence
        )) {
            durableAdvisoriesAccepted.increment();
            notify.run();
        } else {
            durableAdvisoriesRejected.increment();
        }
    }

    private static Long markerSequence(Path marker) {
        try {
            if (Files.size(marker) > 32) return null;
            String value = Files.readString(marker, StandardCharsets.UTF_8);
            if (value.isEmpty() || !value.equals(value.strip())) {
                return null;
            }
            long sequence = Long.parseLong(value);
            return sequence > 0 ? sequence : null;
        } catch (IOException | RuntimeException ignored) {
            return null;
        }
    }

    private static boolean hasDurableAbort(
            Path sessionPath,
            String lane,
            String runId,
            Long expectedSequence
    ) {
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(sessionPath)) {
            if (!Files.exists(sessionPath)) return false;
            try (java.io.BufferedReader reader = Files.newBufferedReader(
                    sessionPath, StandardCharsets.UTF_8
            )) {
                reader.readLine();
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) continue;
                    SessionLogItem mutation = JsonlSessionCodec.decodeMutation(line);
                    if (expectedSequence != null
                            && mutation.sequence() > expectedSequence) {
                        return false;
                    }
                    if ((expectedSequence == null
                            || mutation.sequence() == expectedSequence)
                            && mutation instanceof SessionLogItem.Record item
                            && item.record().lane().equals(lane)
                            && item.record().storedValue()
                            instanceof SessionRecordDraft.AbortRequested abort
                            && abort.runId().equals(runId)) {
                        return true;
                    }
                    if (expectedSequence != null
                            && mutation.sequence() == expectedSequence) {
                        return false;
                    }
                }
            }
            return false;
        } catch (IOException | RuntimeException ignored) {
            return false;
        }
    }

    private static Thread startMarkerObserver(
            MarkerObservationMode mode,
            Path marker,
            String runId,
            AtomicBoolean closed,
            AtomicBoolean signaled,
            Runnable notify,
            WatchServiceFactory watchServiceFactory,
            Runnable onWatchServiceStart,
            Runnable onWatchServiceFallback,
            Runnable onPollingStart
    ) {
        if (mode == MarkerObservationMode.DISABLED) {
            if (Files.exists(marker)) notify.run();
            return null;
        }
        return Thread.ofVirtual()
                .name("pi-operation-abort-" + runId)
                .start(() -> {
                    if (mode == MarkerObservationMode.WATCH_SERVICE) {
                        onWatchServiceStart.run();
                        if (watchMarker(
                                marker, closed, signaled, notify,
                                watchServiceFactory
                        )) return;
                        onWatchServiceFallback.run();
                    }
                    onPollingStart.run();
                    pollMarker(marker, closed, signaled, notify);
                });
    }

    private static boolean watchMarker(
            Path marker,
            AtomicBoolean closed,
            AtomicBoolean signaled,
            Runnable notify,
            WatchServiceFactory watchServiceFactory
    ) {
        try {
            Files.createDirectories(marker.getParent());
            try (WatchService watcher = watchServiceFactory.open(marker)) {
                marker.getParent().register(
                        watcher,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY
                );
                MarkerStamp observed = markerStamp(marker);
                if (observed != null) {
                    notify.run();
                    if (signaled.get()) return true;
                }
                while (!closed.get() && !signaled.get()) {
                    WatchKey key;
                    try {
                        key = watcher.poll(
                                100, java.util.concurrent.TimeUnit.MILLISECONDS
                        );
                    } catch (InterruptedException ignored) {
                        return true;
                    }
                    if (key != null) {
                        key.pollEvents();
                        if (!key.reset()) return false;
                    }
                    MarkerStamp current = markerStamp(marker);
                    if (!java.util.Objects.equals(current, observed)) {
                        observed = current;
                        if (current != null) {
                            notify.run();
                            if (signaled.get()) return true;
                        }
                    }
                }
                return true;
            }
        } catch (IOException | RuntimeException ignored) {
            return false;
        }
    }

    private static void pollMarker(
            Path marker,
            AtomicBoolean closed,
            AtomicBoolean signaled,
            Runnable notify
    ) {
        MarkerStamp observed = null;
        MarkerStamp pending = null;
        while (!closed.get() && !signaled.get()) {
            MarkerStamp current = markerStamp(marker);
            if (current == null) {
                observed = null;
                pending = null;
            } else if (current.equals(observed)) {
                pending = null;
            } else if (current.equals(pending)) {
                observed = current;
                pending = null;
                notify.run();
                if (signaled.get()) return;
            } else {
                pending = current;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
                return;
            }
        }
    }

    private static MarkerStamp markerStamp(Path marker) {
        try {
            BasicFileAttributes attributes = Files.readAttributes(
                    marker, BasicFileAttributes.class
            );
            long size = attributes.size();
            return new MarkerStamp(
                    attributes.lastModifiedTime().toMillis(),
                    size,
                    size <= 32
                            ? Files.readString(marker, StandardCharsets.UTF_8)
                            : null
            );
        } catch (java.nio.file.NoSuchFileException ignored) {
            return null;
        } catch (IOException | RuntimeException ignored) {
            return null;
        }
    }

    private record MarkerStamp(long modifiedAt, long size, String payload) {
    }

    private void signalAbort(Path sessionPath, SessionLogItem mutation) {
        if (!(mutation instanceof SessionLogItem.Record item)
                || !(item.record().storedValue()
                instanceof SessionRecordDraft.AbortRequested abort)) {
            return;
        }
        Path marker = operationAbortPath(
                sessionPath, item.record().lane(), abort.runId()
        );
        try {
            Files.createDirectories(marker.getParent());
            Files.writeString(
                    marker, Long.toString(mutation.sequence()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (IOException ignored) {
            // Advisory signaling cannot invalidate an already committed abort.
        }
        try {
            abortNotifier.publish(new JsonlOperationAbortNotifier.Notification(
                    new JsonlOperationAbortNotifier.Key(
                            sessionPath, item.record().lane(), abort.runId()
                    ),
                    mutation.sequence()
            ));
        } catch (RuntimeException ignored) {
            // External notification is advisory; JSONL records remain authoritative.
        }
    }

    private static Path operationAbortPath(
            Path sessionPath,
            String lane,
            String runId
    ) {
        return Path.of(operationLeasePath(sessionPath, lane, runId) + ".abort");
    }

    private static OperationSignalTarget operationSignalTarget(Path candidate) {
        Path normalized = candidate.toAbsolutePath().normalize();
        Path parent = normalized.getParent();
        if (parent == null || parent.getFileName() == null
                || !parent.getFileName().toString().endsWith(".jsonl.operations")) {
            return null;
        }
        String name = normalized.getFileName().toString();
        if (!name.endsWith(".abort")) return null;
        String hash = name.substring(0, name.length() - ".abort".length());
        if (hash.length() != 64) return null;
        for (int index = 0; index < hash.length(); index++) {
            char value = hash.charAt(index);
            if (!(value >= '0' && value <= '9')
                    && !(value >= 'a' && value <= 'f')) return null;
        }
        String parentPath = parent.toString();
        Path sessionPath = Path.of(parentPath.substring(
                0, parentPath.length() - ".operations".length()
        ));
        return new OperationSignalTarget(
                normalized, parent.resolve(hash), sessionPath
        );
    }

    private static Path operationLeasePath(
            Path sessionPath,
            String lane,
            String runId
    ) {
        String key = lane + '\0' + runId;
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(
                    key.getBytes(StandardCharsets.UTF_8)
            );
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
        return Path.of(sessionPath + ".operations").resolve(
                HexFormat.of().formatHex(digest)
        );
    }

    private Path idLeasePath(String id) {
        return sessionsRoot.resolve(".writer-leases").resolve(id);
    }

    private Path sessionDirectory() {
        String directory = "--" + cwd.toString()
                .replaceFirst("^[/\\\\]", "")
                .replace('/', '-')
                .replace('\\', '-')
                .replace(':', '-') + "--";
        return sessionsRoot.resolve(directory);
    }

    private Path freshSessionPath(long createdAt, String id) {
        long fileTimestamp = createdAt;
        while (true) {
            Path candidate = sessionDirectory().resolve(fileName(fileTimestamp, id));
            if (!Files.exists(candidate)
                    && !Files.exists(Path.of(candidate + ".lock"))
                    && !Files.exists(Path.of(candidate + ".tmp"))) {
                return candidate;
            }
            if (fileTimestamp == Long.MAX_VALUE) {
                throw error(SessionError.Code.STORAGE,
                        "Cannot allocate a fresh JSONL path for session " + id);
            }
            fileTimestamp++;
        }
    }

    private static String fileName(long createdAt, String id) {
        return FILE_TIME.format(Instant.ofEpochMilli(createdAt)) + "_" + id + ".jsonl";
    }

    private static String firstLine(Path path) throws IOException {
        try (java.io.BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return reader.readLine();
        }
    }

    private static SessionMetadata metadata(JsonlSessionCodec.Header header) {
        return new SessionMetadata(
                header.id(), header.createdAt(),
                SessionMetadata.CURRENT_STORAGE_VERSION, header.parentSessionId()
        );
    }

    private synchronized void claim(String id) {
        if (!activeDestinations.add(id)) {
            throw error(SessionError.Code.ALREADY_EXISTS,
                    "Session already exists: " + id);
        }
    }

    private synchronized void release(String id) {
        activeDestinations.remove(id);
    }

    private static void validateId(String id) {
        if (id == null || !SESSION_ID.matcher(id).matches()) {
            throw error(
                    SessionError.Code.INVALID_PAYLOAD,
                    "Session id must be non-empty, contain only alphanumeric characters, "
                            + "'-', '_', and '.', and start and end with an alphanumeric character"
            );
        }
    }

    private static SessionError invalidFile(
            Path path,
            int line,
            String message,
            Throwable cause
    ) {
        SessionError error = new SessionError(
                SessionError.Code.INVALID_ENTRY,
                "Invalid JSONL v4 session " + path + ": line " + line + " " + message
        );
        if (cause != null) error.initCause(cause);
        return error;
    }

    private static SessionError storage(String message, Throwable cause) {
        SessionError error = new SessionError(SessionError.Code.STORAGE, message);
        error.initCause(cause);
        return error;
    }

    private static SessionError error(SessionError.Code code, String message) {
        return new SessionError(code, message);
    }

    private static <T> CompletionStage<T> stage(Supplier<T> operation) {
        try {
            return CompletableFuture.completedFuture(operation.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private static final class MaintenanceAccumulator {
        private static final Comparator<MaintenanceArtifact> PATH_ORDER =
                Comparator.comparing(value -> value.path().toString());

        private final MaintenanceQuery query;
        private final EnumMap<ArtifactKind, Long> counts =
                new EnumMap<>(ArtifactKind.class);
        private final ArrayList<MaintenanceArtifact> unlimited;
        private final PriorityQueue<MaintenanceArtifact> bounded;
        private long matched;

        private MaintenanceAccumulator(MaintenanceQuery query) {
            this.query = query;
            for (ArtifactKind kind : ArtifactKind.values()) counts.put(kind, 0L);
            unlimited = query.limit() == null ? new ArrayList<>() : null;
            bounded = query.limit() == null ? null : new PriorityQueue<>(
                    query.limit(), PATH_ORDER.reversed()
            );
        }

        private void accept(MaintenanceArtifact artifact) {
            counts.compute(artifact.kind(), (ignored, count) -> count + 1);
            if (!query.kinds().contains(artifact.kind())) return;
            matched++;
            if (unlimited != null) {
                unlimited.add(artifact);
                return;
            }
            if (bounded.size() < query.limit()) {
                bounded.add(artifact);
            } else if (PATH_ORDER.compare(artifact, bounded.element()) < 0) {
                bounded.remove();
                bounded.add(artifact);
            }
        }

        private MaintenanceReport finish() {
            ArrayList<MaintenanceArtifact> artifacts = unlimited != null
                    ? unlimited : new ArrayList<>(bounded);
            artifacts.sort(PATH_ORDER);
            return new MaintenanceReport(
                    artifacts, counts, matched, artifacts.size() < matched
            );
        }
    }

    private static final Comparator<RecoveryDetail> RECOVERY_ORDER =
            Comparator.comparing((RecoveryDetail detail) -> detail.path().toString())
                    .thenComparing(detail -> detail.kind().name())
                    .thenComparing(JsonlSessionRepository::recoveryLane);

    private static String recoveryLane(RecoveryDetail detail) {
        return detail instanceof RecoveryOperation operation
                ? operation.operation().lane() : "";
    }

    private static int compareRecoveryCursor(
            RecoveryDetail detail,
            RecoveryCursor cursor
    ) {
        int path = detail.path().toString().compareTo(cursor.path().toString());
        if (path != 0) return path;
        int kind = detail.kind().name().compareTo(cursor.kind().name());
        if (kind != 0) return kind;
        return recoveryLane(detail).compareTo(cursor.lane());
    }

    private static RecoveryCursor recoveryCursor(RecoveryDetail detail) {
        return new RecoveryCursor(
                detail.path(), detail.kind(), recoveryLane(detail)
        );
    }

    private static final class RecoveryAccumulator {
        private final RecoveryQuery query;
        private final EnumMap<RecoveryKind, Long> counts =
                new EnumMap<>(RecoveryKind.class);
        private final ArrayList<RecoveryDetail> unlimited;
        private final PriorityQueue<RecoveryDetail> bounded;
        private long sessionsScanned;
        private long unresolvedSafe;
        private long unresolvedNever;
        private long matched;

        private RecoveryAccumulator(RecoveryQuery query) {
            this.query = query;
            for (RecoveryKind kind : RecoveryKind.values()) counts.put(kind, 0L);
            unlimited = query.limit() == null ? new ArrayList<>() : null;
            bounded = query.limit() == null ? null : new PriorityQueue<>(
                    query.limit(), RECOVERY_ORDER.reversed()
            );
        }

        private void sessionScanned() {
            sessionsScanned++;
        }

        private void operation(RecoveryOperation operation) {
            counts.compute(RecoveryKind.OPEN_OPERATION,
                    (ignored, count) -> count + 1);
            for (SessionOperationInspector.UnresolvedToolEffect effect
                    : operation.operation().unresolvedTools()) {
                if (effect.replay() == SessionRecordDraft.Replay.SAFE) {
                    unresolvedSafe++;
                } else {
                    unresolvedNever++;
                }
            }
            accept(operation);
        }

        private void failure(RecoveryFailure failure) {
            counts.compute(RecoveryKind.CORRUPT_SESSION,
                    (ignored, count) -> count + 1);
            accept(failure);
        }

        private void accept(RecoveryDetail detail) {
            if (!query.kinds().contains(detail.kind())) return;
            if (detail instanceof RecoveryOperation operation
                    && !query.toolRecoveries().isEmpty()
                    && operation.operation().unresolvedTools().stream()
                    .noneMatch(effect -> query.toolRecoveries()
                            .contains(effect.recovery()))) {
                return;
            }
            if (query.after() != null
                    && compareRecoveryCursor(detail, query.after()) <= 0) {
                return;
            }
            matched++;
            if (unlimited != null) {
                unlimited.add(detail);
                return;
            }
            if (bounded.size() < query.limit()) {
                bounded.add(detail);
            } else if (RECOVERY_ORDER.compare(detail, bounded.element()) < 0) {
                bounded.remove();
                bounded.add(detail);
            }
        }

        private RecoveryReport finish() {
            ArrayList<RecoveryDetail> details = unlimited != null
                    ? unlimited : new ArrayList<>(bounded);
            details.sort(RECOVERY_ORDER);
            boolean truncated = details.size() < matched;
            RecoveryCursor nextCursor = truncated && !details.isEmpty()
                    ? recoveryCursor(details.getLast()) : null;
            return new RecoveryReport(
                    details, counts, sessionsScanned,
                    unresolvedSafe, unresolvedNever,
                    matched, truncated, nextCursor
            );
        }
    }

    private static int compareCheckpointCursor(
            RecoveryCheckpointDetail detail,
            RecoveryCheckpointCursor cursor
    ) {
        int path = detail.relativePath().compareTo(cursor.relativePath());
        if (path != 0) return path;
        return detail.status().name().compareTo(cursor.status().name());
    }

    private static RecoveryCheckpointCursor checkpointCursor(
            RecoveryCheckpointDetail detail
    ) {
        return new RecoveryCheckpointCursor(
                detail.relativePath(), detail.status()
        );
    }

    private static final class RecoveryCheckpointAccumulator {
        private static final Comparator<RecoveryCheckpointDetail> ORDER =
                Comparator.comparing(RecoveryCheckpointDetail::relativePath)
                        .thenComparing(detail -> detail.status().name());

        private final RecoveryCheckpointQuery query;
        private final EnumMap<CheckpointStatus, Long> counts =
                new EnumMap<>(CheckpointStatus.class);
        private final ArrayList<RecoveryCheckpointDetail> unlimited;
        private final PriorityQueue<RecoveryCheckpointDetail> bounded;
        private long matched;

        private RecoveryCheckpointAccumulator(RecoveryCheckpointQuery query) {
            this.query = query;
            for (CheckpointStatus status : CheckpointStatus.values()) {
                counts.put(status, 0L);
            }
            unlimited = query.limit() == null ? new ArrayList<>() : null;
            bounded = query.limit() == null ? null : new PriorityQueue<>(
                    query.limit(), ORDER.reversed()
            );
        }

        private void accept(RecoveryCheckpointDetail detail) {
            counts.compute(detail.status(), (ignored, count) -> count + 1);
            if (detail.status() == CheckpointStatus.UNCHANGED) return;
            if (query.after() != null
                    && compareCheckpointCursor(detail, query.after()) <= 0) {
                return;
            }
            matched++;
            if (unlimited != null) {
                unlimited.add(detail);
            } else if (bounded.size() < query.limit()) {
                bounded.add(detail);
            } else if (ORDER.compare(detail, bounded.element()) < 0) {
                bounded.remove();
                bounded.add(detail);
            }
        }

        private RecoveryCheckpointReport finish() {
            ArrayList<RecoveryCheckpointDetail> details = unlimited != null
                    ? unlimited : new ArrayList<>(bounded);
            details.sort(ORDER);
            boolean truncated = details.size() < matched;
            RecoveryCheckpointCursor next = truncated && !details.isEmpty()
                    ? checkpointCursor(details.getLast()) : null;
            return new RecoveryCheckpointReport(
                    details, counts, matched, truncated, next
            );
        }
    }

    public enum CheckpointStatus {
        UNCHANGED,
        CHANGED,
        MISSING,
        ADDED
    }

    public record RecoveryGenerationFingerprint(
            String relativePath,
            String sessionId,
            Long tailSequence,
            long size,
            String sha256
    ) {
        public RecoveryGenerationFingerprint {
            if (relativePath == null || relativePath.isBlank()) {
                throw new IllegalArgumentException("relativePath must not be blank");
            }
            Path relative = Path.of(relativePath);
            if (relative.isAbsolute() || relative.normalize().startsWith("..")) {
                throw new IllegalArgumentException(
                        "Invalid relative checkpoint path " + relativePath
                );
            }
            if (tailSequence != null && tailSequence < 0) {
                throw new IllegalArgumentException("Invalid tail sequence");
            }
            if (size < 0) throw new IllegalArgumentException("Invalid generation size");
            if (sha256 == null || !sha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Invalid generation SHA-256");
            }
        }
    }

    public record RecoveryCheckpoint(
            int version,
            long capturedAt,
            Path repositoryRoot,
            List<RecoveryGenerationFingerprint> generations
    ) {
        public RecoveryCheckpoint {
            if (version < 1 || capturedAt < 0) {
                throw new IllegalArgumentException("Invalid recovery checkpoint header");
            }
            repositoryRoot = java.util.Objects.requireNonNull(
                    repositoryRoot, "repositoryRoot"
            ).toAbsolutePath().normalize();
            generations = new ArrayList<>(generations);
            generations.sort(Comparator.comparing(
                    RecoveryGenerationFingerprint::relativePath
            ));
            HashSet<String> paths = new HashSet<>();
            for (RecoveryGenerationFingerprint generation : generations) {
                if (!paths.add(generation.relativePath())) {
                    throw new IllegalArgumentException(
                            "Duplicate checkpoint generation "
                                    + generation.relativePath()
                    );
                }
            }
            generations = List.copyOf(generations);
        }
    }

    public record RecoveryCheckpointDetail(
            String relativePath,
            CheckpointStatus status,
            RecoveryGenerationFingerprint expected,
            RecoveryGenerationFingerprint current
    ) {
        public RecoveryCheckpointDetail {
            java.util.Objects.requireNonNull(relativePath, "relativePath");
            java.util.Objects.requireNonNull(status, "status");
            if ((status == CheckpointStatus.UNCHANGED
                    || status == CheckpointStatus.CHANGED)
                    && (expected == null || current == null)
                    || status == CheckpointStatus.MISSING
                    && (expected == null || current != null)
                    || status == CheckpointStatus.ADDED
                    && (expected != null || current == null)) {
                throw new IllegalArgumentException(
                        "Checkpoint detail payload does not match status"
                );
            }
        }
    }

    public record RecoveryCheckpointCursor(
            String relativePath,
            CheckpointStatus status
    ) {
        public RecoveryCheckpointCursor {
            if (relativePath == null || relativePath.isBlank()) {
                throw new IllegalArgumentException(
                        "relativePath must not be blank"
                );
            }
            java.util.Objects.requireNonNull(status, "status");
        }
    }

    public record RecoveryCheckpointQuery(
            Integer limit,
            RecoveryCheckpointCursor after
    ) {
        public static final RecoveryCheckpointQuery ALL =
                new RecoveryCheckpointQuery(null, null);

        public RecoveryCheckpointQuery {
            if (limit != null && limit <= 0) {
                throw new IllegalArgumentException("limit must be positive");
            }
        }
    }

    public record RecoveryCheckpointScanCursor(String relativePath) {
        public RecoveryCheckpointScanCursor {
            if (relativePath == null || relativePath.isBlank()) {
                throw new IllegalArgumentException(
                        "relativePath must not be blank"
                );
            }
            Path relative = Path.of(relativePath);
            if (relative.isAbsolute() || relative.normalize().startsWith("..")) {
                throw new IllegalArgumentException(
                        "Invalid checkpoint scan path " + relativePath
                );
            }
        }
    }

    public record RecoveryCheckpointScanQuery(
            RecoveryCheckpointQuery verification,
            RecoveryCheckpointScanCursor after,
            int maxGenerations,
            Duration maxDuration
    ) {
        public RecoveryCheckpointScanQuery(int maxGenerations) {
            this(RecoveryCheckpointQuery.ALL, null, maxGenerations, null);
        }

        public RecoveryCheckpointScanQuery {
            verification = verification == null
                    ? RecoveryCheckpointQuery.ALL : verification;
            if (maxGenerations < 1) {
                throw new IllegalArgumentException(
                        "maxGenerations must be positive"
                );
            }
            if (maxDuration != null && maxDuration.isNegative()) {
                throw new IllegalArgumentException(
                        "maxDuration must be non-negative"
                );
            }
        }
    }

    public record RecoveryCheckpointBatchReport(
            RecoveryCheckpointReport verification,
            int generationsInspected,
            RecoveryCheckpointScanCursor nextScanCursor,
            boolean scanComplete
    ) {
        public RecoveryCheckpointBatchReport {
            java.util.Objects.requireNonNull(verification, "verification");
            if (generationsInspected < 0
                    || scanComplete != (nextScanCursor == null)) {
                throw new IllegalArgumentException(
                        "Invalid checkpoint batch report"
                );
            }
        }
    }

    public record RecoveryCheckpointReport(
            List<RecoveryCheckpointDetail> details,
            Map<CheckpointStatus, Long> counts,
            long matched,
            boolean truncated,
            RecoveryCheckpointCursor nextCursor
    ) {
        public RecoveryCheckpointReport {
            details = List.copyOf(details);
            counts = Map.copyOf(counts);
            if (matched < details.size()
                    || truncated != (details.size() < matched)
                    || (truncated && nextCursor == null)
                    || (!truncated && nextCursor != null)) {
                throw new IllegalArgumentException(
                        "Invalid checkpoint verification counts"
                );
            }
        }

        public long count(CheckpointStatus status) {
            return counts.getOrDefault(status, 0L);
        }
    }

    @FunctionalInterface
    interface WatchServiceFactory {
        WatchService open(Path marker) throws IOException;
    }

    public enum MarkerObservationMode {
        POLLING,
        WATCH_SERVICE,
        DISABLED
    }

    public record MarkerObservationDiagnostics(
            long watchServiceStarts,
            long watchServiceFallbacks,
            long pollingStarts,
            long durableAdvisoriesAccepted,
            long durableAdvisoriesRejected
    ) {
        public MarkerObservationDiagnostics {
            if (watchServiceStarts < 0 || watchServiceFallbacks < 0
                    || pollingStarts < 0 || durableAdvisoriesAccepted < 0
                    || durableAdvisoriesRejected < 0
                    || watchServiceFallbacks > watchServiceStarts) {
                throw new IllegalArgumentException(
                        "Invalid marker observation diagnostics"
                );
            }
        }
    }

    public enum RecoveryKind {
        OPEN_OPERATION,
        CORRUPT_SESSION
    }

    public sealed interface RecoveryDetail permits
            RecoveryOperation, RecoveryFailure {
        RecoveryKind kind();

        Path path();
    }

    public record RecoveryOperation(
            SessionMetadata session,
            Path path,
            SessionOperationInspector.OpenOperation operation
    ) implements RecoveryDetail {
        public RecoveryOperation {
            java.util.Objects.requireNonNull(session, "session");
            java.util.Objects.requireNonNull(path, "path");
            java.util.Objects.requireNonNull(operation, "operation");
        }

        @Override
        public RecoveryKind kind() {
            return RecoveryKind.OPEN_OPERATION;
        }
    }

    public record RecoveryFailure(
            Path path,
            String sessionId,
            SessionError.Code code,
            String message
    ) implements RecoveryDetail {
        public RecoveryFailure {
            java.util.Objects.requireNonNull(path, "path");
            java.util.Objects.requireNonNull(code, "code");
            java.util.Objects.requireNonNull(message, "message");
        }

        @Override
        public RecoveryKind kind() {
            return RecoveryKind.CORRUPT_SESSION;
        }
    }

    public record RecoveryScanCursor(Path generationPath) {
        public RecoveryScanCursor {
            generationPath = java.util.Objects.requireNonNull(
                    generationPath, "generationPath"
            ).toAbsolutePath().normalize();
        }
    }

    public record RecoveryScanQuery(
            RecoveryQuery recovery,
            RecoveryScanCursor after,
            int maxSessions,
            Duration maxDuration
    ) {
        public RecoveryScanQuery(int maxSessions) {
            this(RecoveryQuery.ALL, null, maxSessions, null);
        }

        public RecoveryScanQuery {
            recovery = recovery == null ? RecoveryQuery.ALL : recovery;
            if (maxSessions < 1) {
                throw new IllegalArgumentException("maxSessions must be positive");
            }
            if (maxDuration != null && maxDuration.isNegative()) {
                throw new IllegalArgumentException(
                        "maxDuration must be non-negative"
                );
            }
        }
    }

    public record RecoveryBatchReport(
            RecoveryReport recovery,
            RecoveryScanCursor nextScanCursor,
            boolean scanComplete
    ) {
        public RecoveryBatchReport {
            java.util.Objects.requireNonNull(recovery, "recovery");
            if (scanComplete != (nextScanCursor == null)) {
                throw new IllegalArgumentException(
                        "scanComplete does not match nextScanCursor"
                );
            }
        }
    }

    public record RecoveryCursor(
            Path path,
            RecoveryKind kind,
            String lane
    ) {
        public RecoveryCursor {
            path = java.util.Objects.requireNonNull(path, "path")
                    .toAbsolutePath().normalize();
            java.util.Objects.requireNonNull(kind, "kind");
            lane = lane == null ? "" : lane;
        }
    }

    public record RecoveryQuery(
            Set<RecoveryKind> kinds,
            Integer limit,
            Set<SessionOperationInspector.ToolRecovery> toolRecoveries,
            RecoveryCursor after
    ) {
        public static final RecoveryQuery ALL =
                new RecoveryQuery(null, null, null, null);

        public RecoveryQuery(Set<RecoveryKind> kinds, Integer limit) {
            this(kinds, limit, null, null);
        }

        public RecoveryQuery(
                Set<RecoveryKind> kinds,
                Integer limit,
                Set<SessionOperationInspector.ToolRecovery> toolRecoveries
        ) {
            this(kinds, limit, toolRecoveries, null);
        }

        public RecoveryQuery {
            kinds = kinds == null
                    ? Set.of(RecoveryKind.values()) : Set.copyOf(kinds);
            toolRecoveries = toolRecoveries == null
                    ? Set.of() : Set.copyOf(toolRecoveries);
            if (limit != null && limit <= 0) {
                throw new IllegalArgumentException("limit must be positive");
            }
        }
    }

    public record RecoveryReport(
            List<RecoveryDetail> details,
            Map<RecoveryKind, Long> counts,
            long sessionsScanned,
            long unresolvedSafe,
            long unresolvedNever,
            long matched,
            boolean truncated,
            RecoveryCursor nextCursor
    ) {
        public RecoveryReport {
            details = List.copyOf(details);
            counts = Map.copyOf(counts);
            if (sessionsScanned < 0 || unresolvedSafe < 0 || unresolvedNever < 0
                    || matched < details.size()
                    || (truncated && nextCursor == null)
                    || (!truncated && nextCursor != null)) {
                throw new IllegalArgumentException("Invalid recovery report counts");
            }
        }

        public long count(RecoveryKind kind) {
            return counts.getOrDefault(kind, 0L);
        }

        public List<RecoveryOperation> operations() {
            return details.stream().filter(RecoveryOperation.class::isInstance)
                    .map(RecoveryOperation.class::cast).toList();
        }

        public List<RecoveryFailure> failures() {
            return details.stream().filter(RecoveryFailure.class::isInstance)
                    .map(RecoveryFailure.class::cast).toList();
        }
    }

    public enum ArtifactKind {
        SESSION,
        STAGING,
        SESSION_LOCK,
        SESSION_ID_LOCK,
        OPERATION_LOCK,
        OPERATION_ABORT_SIGNAL
    }

    public record MaintenanceArtifact(
            ArtifactKind kind,
            Path path,
            Path target,
            String sessionId,
            long size,
            long modifiedAt,
            boolean associatedDataPresent
    ) {
        public MaintenanceArtifact {
            if (size < 0) {
                throw new IllegalArgumentException("Invalid artifact size");
            }
        }
    }

    public record MaintenanceQuery(
            Set<ArtifactKind> kinds,
            Integer limit
    ) {
        public static final MaintenanceQuery ALL = new MaintenanceQuery(null, null);

        public MaintenanceQuery {
            kinds = kinds == null
                    ? Set.of(ArtifactKind.values()) : Set.copyOf(kinds);
            if (limit != null && limit <= 0) {
                throw new IllegalArgumentException("limit must be positive");
            }
        }
    }

    public record MaintenanceReport(
            List<MaintenanceArtifact> artifacts,
            Map<ArtifactKind, Long> counts,
            long matched,
            boolean truncated
    ) {
        public MaintenanceReport {
            artifacts = List.copyOf(artifacts);
            counts = Map.copyOf(counts);
            if (matched < artifacts.size()) {
                throw new IllegalArgumentException("matched is smaller than artifacts");
            }
        }

        public List<MaintenanceArtifact> artifacts(ArtifactKind kind) {
            return artifacts.stream().filter(value -> value.kind() == kind).toList();
        }

        public long count(ArtifactKind kind) {
            return counts.getOrDefault(kind, 0L);
        }
    }

    public record StagingCleanupPolicy(Duration minimumAge) {
        public static final StagingCleanupPolicy IMMEDIATE =
                new StagingCleanupPolicy(Duration.ZERO);

        public StagingCleanupPolicy {
            if (minimumAge == null || minimumAge.isNegative()) {
                throw new IllegalArgumentException(
                        "minimumAge must be non-negative"
                );
            }
        }
    }

    public record CleanupResult(int scanned, int deleted) {
        public CleanupResult {
            if (scanned < 0 || deleted < 0 || deleted > scanned) {
                throw new IllegalArgumentException("Invalid cleanup counts");
            }
        }
    }

    public record OperationSignalCleanupPolicy(Duration minimumAge) {
        public static final OperationSignalCleanupPolicy IMMEDIATE =
                new OperationSignalCleanupPolicy(Duration.ZERO);

        public OperationSignalCleanupPolicy {
            if (minimumAge == null || minimumAge.isNegative()) {
                throw new IllegalArgumentException(
                        "minimumAge must be non-negative"
                );
            }
        }
    }

    public record OperationSignalCleanupResult(
            int scanned,
            int deleted,
            int associated,
            int tooYoung
    ) {
        public OperationSignalCleanupResult {
            if (scanned < 0 || deleted < 0 || associated < 0 || tooYoung < 0
                    || deleted + associated + tooYoung > scanned) {
                throw new IllegalArgumentException(
                        "Invalid operation signal cleanup counts"
                );
            }
        }
    }

    private record OperationSignalTarget(
            Path marker,
            Path operationLeaseBase,
            Path sessionPath
    ) {
    }

    private record Listed(SessionMetadata metadata, long modifiedAt) {
    }
}
