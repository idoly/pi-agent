package io.github.idoly.pi.agent.session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlOperationAbortNotifierTest {
    @TempDir
    Path temporary;

    @Test
    void externalNotifierCancelsAnotherHandleWithoutMarkerPolling() throws Exception {
        TestNotifier notifier = new TestNotifier();
        JsonlSessionRepository ownerRepository = repository(notifier, false);
        AgentSession original = openRun(ownerRepository, "external-abort", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(ownerRepository.open(metadata));
        JsonlSessionRepository abortRepository = repository(notifier, false);
        AgentSession aborter = join(abortRepository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            assertEquals(1, cancellations.get());
            assertEquals(1, notifier.notifications.size());
            JsonlOperationAbortNotifier.Notification notification =
                    notifier.notifications.getFirst();
            assertEquals("main", notification.key().lane());
            assertEquals("run", notification.key().runId());
            List<String> lines = Files.readAllLines(
                    notification.key().generationPath(), StandardCharsets.UTF_8
            );
            SessionLogItem mutation = JsonlSessionCodec.decodeMutation(lines.getLast());
            assertEquals(notification.sequence(), mutation.sequence());
            assertTrue(mutation instanceof SessionLogItem.Record record
                    && record.record().value()
                    instanceof SessionRecordDraft.AbortRequested);
            assertTrue(Files.exists(operationMarker(notification.key())));
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
        notifier.publish(notifier.notifications.getFirst());
        assertEquals(1, cancellations.get(),
                "closed external subscription still delivered cancellation");
    }

    @Test
    void notifierFailuresArePassiveAndDurableMarkerRemains() throws Exception {
        JsonlOperationAbortNotifier failing = new JsonlOperationAbortNotifier() {
            @Override
            public void publish(Notification notification) {
                throw new IllegalStateException("publish");
            }

            @Override
            public AutoCloseable observe(Key key, Runnable cancellation) {
                throw new IllegalStateException("observe");
            }
        };
        JsonlSessionRepository repository = repository(failing, false);
        AgentSession session = openRun(repository, "failing-notifier", "run");
        assertTrue(session.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = session.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
            assertEquals(1, cancellations.get());
            assertTrue(session.rawState().hasAbortRequest("main", "run"));
            assertEquals(1, join(repository.inspectMaintenance()).count(
                    JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL
            ));
        } finally {
            registration.close();
            session.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void watchServiceFailureFallsBackToPollingAndStillCancels()
            throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary,
                Clock.systemUTC(), SessionIdGenerator.uuidV7(),
                JsonlOperationAbortNotifier.NONE,
                JsonlSessionRepository.MarkerObservationMode.WATCH_SERVICE,
                ignored -> { throw new java.io.IOException("unsupported"); }
        );
        AgentSession original = openRun(repository, "watch-fallback", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            awaitPolling(repository);
            JsonlSessionRepository.MarkerObservationDiagnostics fallback =
                    repository.markerObservationDiagnostics();
            assertEquals(1, fallback.watchServiceStarts());
            assertEquals(1, fallback.watchServiceFallbacks());
            assertEquals(1, fallback.pollingStarts());

            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            awaitCount(cancellations, 1);
            assertTrue(repository.markerObservationDiagnostics()
                    .durableAdvisoriesAccepted() >= 1);
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void markerCannotCancelWithoutMatchingDurableAbortAndWatcherContinues()
            throws Exception {
        JsonlSessionRepository repository = repository(
                JsonlOperationAbortNotifier.NONE,
                JsonlSessionRepository.MarkerObservationMode.WATCH_SERVICE
        );
        AgentSession original = openRun(repository, "forged-marker", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            Path generation = join(repository.inspectMaintenance()).artifacts().stream()
                    .filter(value -> value.kind()
                            == JsonlSessionRepository.ArtifactKind.SESSION)
                    .findFirst().orElseThrow().path();
            Path marker = operationMarker(new JsonlOperationAbortNotifier.Key(
                    generation, "main", "run"
            ));
            Files.createDirectories(marker.getParent());
            Files.writeString(marker, "forged", StandardCharsets.UTF_8);
            Thread.sleep(100);
            assertEquals(0, cancellations.get());
            assertFalse(owner.rawState().hasAbortRequest("main", "run"));

            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            awaitCount(cancellations, 1);
            JsonlSessionRepository.MarkerObservationDiagnostics diagnostics =
                    repository.markerObservationDiagnostics();
            assertEquals(1, diagnostics.watchServiceStarts());
            assertEquals(diagnostics.watchServiceFallbacks(),
                    diagnostics.pollingStarts());
            assertTrue(diagnostics.durableAdvisoriesRejected() >= 1);
            assertTrue(diagnostics.durableAdvisoriesAccepted() >= 1);
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void pollingDoesNotReverifyAnUnchangedRejectedMarker() throws Exception {
        JsonlSessionRepository repository = repository(
                JsonlOperationAbortNotifier.NONE,
                JsonlSessionRepository.MarkerObservationMode.POLLING
        );
        AgentSession original = openRun(repository, "polling-forged-marker", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            Path generation = join(repository.inspectMaintenance()).artifacts().stream()
                    .filter(value -> value.kind()
                            == JsonlSessionRepository.ArtifactKind.SESSION)
                    .findFirst().orElseThrow().path();
            Path marker = operationMarker(new JsonlOperationAbortNotifier.Key(
                    generation, "main", "run"
            ));
            Files.createDirectories(marker.getParent());
            Files.writeString(marker, "1", StandardCharsets.UTF_8);
            awaitRejected(repository, 1);
            Thread.sleep(200);
            JsonlSessionRepository.MarkerObservationDiagnostics rejected =
                    repository.markerObservationDiagnostics();
            assertEquals(1, rejected.pollingStarts());
            assertEquals(1, rejected.durableAdvisoriesRejected());
            assertEquals(0, rejected.durableAdvisoriesAccepted());
            assertEquals(0, cancellations.get());

            Files.writeString(marker, "1".repeat(100), StandardCharsets.UTF_8);
            awaitRejected(repository, 2);
            Thread.sleep(200);
            assertEquals(2, repository.markerObservationDiagnostics()
                    .durableAdvisoriesRejected());
            assertEquals(0, cancellations.get());

            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            awaitCount(cancellations, 1);
            JsonlSessionRepository.MarkerObservationDiagnostics accepted =
                    repository.markerObservationDiagnostics();
            assertEquals(2, accepted.durableAdvisoriesRejected());
            assertTrue(accepted.durableAdvisoriesAccepted() >= 1);
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void externalNotificationCannotCancelWithoutMatchingDurableAbort()
            throws Exception {
        TestNotifier notifier = new TestNotifier();
        JsonlSessionRepository repository = repository(notifier, false);
        AgentSession session = openRun(repository, "forged-notification", "run");
        assertTrue(session.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = session.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            Path generation = join(repository.inspectMaintenance()).artifacts().stream()
                    .filter(value -> value.kind()
                            == JsonlSessionRepository.ArtifactKind.SESSION)
                    .findFirst().orElseThrow().path();
            notifier.publish(new JsonlOperationAbortNotifier.Notification(
                    new JsonlOperationAbortNotifier.Key(
                            generation, "main", "run"
                    ),
                    1
            ));
            assertEquals(0, cancellations.get());
            assertFalse(session.rawState().hasAbortRequest("main", "run"));

            assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
            assertEquals(1, cancellations.get());
            assertTrue(session.rawState().hasAbortRequest("main", "run"));
            JsonlSessionRepository.MarkerObservationDiagnostics diagnostics =
                    repository.markerObservationDiagnostics();
            assertEquals(0, diagnostics.watchServiceStarts());
            assertEquals(0, diagnostics.pollingStarts());
            assertEquals(1, diagnostics.durableAdvisoriesRejected());
            assertEquals(1, diagnostics.durableAdvisoriesAccepted());
        } finally {
            registration.close();
            session.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void disabledBackgroundObservationStillFencesAbortBeforeRegistration()
            throws Exception {
        JsonlSessionRepository repository = repository(
                JsonlOperationAbortNotifier.NONE,
                JsonlSessionRepository.MarkerObservationMode.DISABLED
        );
        AgentSession original = openRun(repository, "disabled-existing", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));

        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            assertEquals(1, cancellations.get());
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void watchServiceObservesMarkerCreationAcrossIndependentHandles() throws Exception {
        JsonlSessionRepository repository = repository(
                JsonlOperationAbortNotifier.NONE,
                JsonlSessionRepository.MarkerObservationMode.WATCH_SERVICE
        );
        AgentSession original = openRun(repository, "watch-service", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            awaitCount(cancellations, 1);
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void watchServiceClosesExistingMarkerRegistrationRace() throws Exception {
        JsonlSessionRepository repository = repository(
                JsonlOperationAbortNotifier.NONE,
                JsonlSessionRepository.MarkerObservationMode.WATCH_SERVICE
        );
        AgentSession original = openRun(repository, "watch-existing", "run");
        SessionMetadata metadata = join(original.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));
        assertTrue(owner.rawState().claimOperationExecution("main", "run"));
        assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = owner.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            awaitCount(cancellations, 1);
        } finally {
            registration.close();
            owner.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void oneOperationSharesOneExternalSubscriptionAcrossCallbacks() throws Exception {
        TestNotifier notifier = new TestNotifier();
        JsonlSessionRepository repository = repository(notifier, false);
        AgentSession session = openRun(repository, "shared-subscription", "run");
        assertTrue(session.rawState().claimOperationExecution("main", "run"));
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        AutoCloseable firstRegistration = session.rawState()
                .registerOperationCancellation("main", "run", first::incrementAndGet);
        AutoCloseable secondRegistration = session.rawState()
                .registerOperationCancellation("main", "run", second::incrementAndGet);
        assertEquals(1, notifier.observes.get());
        try {
            assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
            assertEquals(1, first.get());
            assertEquals(1, second.get());
            assertEquals(0, notifier.closes.get());
        } finally {
            firstRegistration.close();
            secondRegistration.close();
            session.rawState().releaseOperationExecution("main", "run");
        }
        assertEquals(1, notifier.closes.get());
    }

    @Test
    void externalSubscriptionCloseFailureIsPassive() throws Exception {
        JsonlOperationAbortNotifier notifier = new JsonlOperationAbortNotifier() {
            @Override
            public void publish(Notification notification) {
            }

            @Override
            public AutoCloseable observe(Key key, Runnable cancellation) {
                return () -> { throw new IllegalStateException("close"); };
            }
        };
        JsonlSessionRepository repository = repository(notifier, false);
        AgentSession session = openRun(repository, "close-failure", "run");
        assertTrue(session.rawState().claimOperationExecution("main", "run"));
        AutoCloseable registration = session.rawState().registerOperationCancellation(
                "main", "run", () -> { }
        );
        registration.close();
        session.rawState().releaseOperationExecution("main", "run");
    }

    @Test
    void markerPollingCanRemainEnabledAlongsideExternalNotifierWithoutDuplicates()
            throws Exception {
        TestNotifier notifier = new TestNotifier();
        JsonlSessionRepository repository = repository(notifier, true);
        AgentSession session = openRun(repository, "both-notifiers", "run");
        assertTrue(session.rawState().claimOperationExecution("main", "run"));
        AtomicInteger cancellations = new AtomicInteger();
        AutoCloseable registration = session.rawState().registerOperationCancellation(
                "main", "run", cancellations::incrementAndGet
        );
        try {
            assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
            Thread.sleep(100);
            assertEquals(1, cancellations.get());
            assertEquals(1, notifier.notifications.size());
        } finally {
            registration.close();
            session.rawState().releaseOperationExecution("main", "run");
        }
    }

    private JsonlSessionRepository repository(
            JsonlOperationAbortNotifier notifier,
            boolean markerPolling
    ) {
        return new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary,
                Clock.systemUTC(), SessionIdGenerator.uuidV7(),
                notifier, markerPolling
        );
    }

    private JsonlSessionRepository repository(
            JsonlOperationAbortNotifier notifier,
            JsonlSessionRepository.MarkerObservationMode mode
    ) {
        return new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary,
                Clock.systemUTC(), SessionIdGenerator.uuidV7(),
                notifier, mode
        );
    }

    private static void awaitPolling(JsonlSessionRepository repository)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        while (repository.markerObservationDiagnostics().pollingStarts() == 0) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Timed out waiting for polling fallback");
            }
            Thread.sleep(10);
        }
    }

    private static void awaitRejected(
            JsonlSessionRepository repository,
            long expected
    ) throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        while (repository.markerObservationDiagnostics()
                .durableAdvisoriesRejected() != expected) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(
                        "Timed out waiting for rejected advisory " + expected
                );
            }
            Thread.sleep(10);
        }
    }

    private static void awaitCount(AtomicInteger value, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        while (value.get() != expected) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(
                        "Timed out waiting for count " + expected
                                + "; actual " + value.get()
                );
            }
            Thread.sleep(10);
        }
    }

    private static AgentSession openRun(
            JsonlSessionRepository repository,
            String id,
            String runId
    ) {
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions(id, null)
        ));
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                runId, "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        return session;
    }

    private static Path operationMarker(JsonlOperationAbortNotifier.Key key) {
        String operation = key.lane() + '\0' + key.runId();
        byte[] digest;
        try {
            digest = java.security.MessageDigest.getInstance("SHA-256").digest(
                    operation.getBytes(StandardCharsets.UTF_8)
            );
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
        return Path.of(key.generationPath() + ".operations").resolve(
                java.util.HexFormat.of().formatHex(digest) + ".abort"
        );
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private static final class TestNotifier
            implements JsonlOperationAbortNotifier.Sequenced {
        private final LinkedHashMap<Key, ArrayList<java.util.function.Consumer<Notification>>>
                listeners = new LinkedHashMap<>();
        private final ArrayList<Notification> notifications = new ArrayList<>();
        private final AtomicInteger observes = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public synchronized void publish(Notification notification) {
            notifications.add(notification);
            List<java.util.function.Consumer<Notification>> callbacks =
                    listeners.containsKey(notification.key())
                            ? List.copyOf(listeners.get(notification.key()))
                            : List.of();
            callbacks.forEach(callback -> callback.accept(notification));
        }

        @Override
        public synchronized AutoCloseable observeNotifications(
                Key key,
                java.util.function.Consumer<Notification> observer
        ) {
            observes.incrementAndGet();
            listeners.computeIfAbsent(key, ignored -> new ArrayList<>())
                    .add(observer);
            return () -> {
                closes.incrementAndGet();
                synchronized (TestNotifier.this) {
                    ArrayList<java.util.function.Consumer<Notification>> callbacks =
                            listeners.get(key);
                    if (callbacks != null) callbacks.remove(observer);
                }
            };
        }
    }
}
