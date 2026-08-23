package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.Usage;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

final class InMemorySessionState {
    private static final PersistenceSink NO_PERSISTENCE = new PersistenceSink() {
        @Override
        public void persist(SessionLogItem mutation) {
        }

        @Override
        public void persistBatch(List<SessionLogItem> mutations) {
        }
    };

    private final SessionMetadata metadata;
    private final Clock clock;
    private final PersistenceSink persistence;
    private final SessionRunQueueEventBus queueEvents =
            new SessionRunQueueEventBus();
    private final ArrayList<SessionEntry> entries = new ArrayList<>();
    private final LinkedHashMap<String, SessionEntry> entriesById = new LinkedHashMap<>();
    private final ArrayList<SessionUsageRow> usage = new ArrayList<>();
    private final ArrayList<SessionRecord> records = new ArrayList<>();
    private final LinkedHashMap<String, LinkedHashMap<String, SessionRecord>> openOperations =
            new LinkedHashMap<>();
    private final LinkedHashMap<String, TerminalOperation> latestTerminalByLane =
            new LinkedHashMap<>();
    private final Set<String> usedIds = new HashSet<>();
    private final Set<String> activeOperationExecutions = new HashSet<>();
    private final LinkedHashMap<String, ArrayList<Runnable>> operationCancellations =
            new LinkedHashMap<>();
    private final LinkedHashMap<String, AutoCloseable> operationExecutionLeases =
            new LinkedHashMap<>();
    private final LinkedHashMap<String, AutoCloseable> operationAbortSubscriptions =
            new LinkedHashMap<>();
    private final LinkedHashMap<String, String> lanes = new LinkedHashMap<>();
    private final LinkedHashMap<String, String> labels = new LinkedHashMap<>();
    private final ArrayList<SessionLogItem> log = new ArrayList<>();
    private long sequence;
    private String name;
    private ArrayList<SessionLogItem> transactionBuffer;

    InMemorySessionState(SessionMetadata metadata, Clock clock) {
        this(metadata, clock, NO_PERSISTENCE);
    }

    InMemorySessionState(
            SessionMetadata metadata,
            Clock clock,
            PersistenceSink persistence
    ) {
        this.metadata = metadata;
        this.clock = clock;
        this.persistence = persistence;
        lanes.put("main", null);
    }

    synchronized SessionMetadata metadata() {
        return metadata;
    }

    synchronized List<AgentSession.LanePointer> lanes() {
        return lanes.entrySet().stream()
                .map(entry -> new AgentSession.LanePointer(entry.getKey(), entry.getValue()))
                .toList();
    }

    synchronized String leaf(String lane) {
        requireLane(lane);
        return lanes.get(lane);
    }

    synchronized void createLane(String lane, String at) {
        requireLaneName(lane);
        if (lanes.containsKey(lane)) {
            throw error(SessionError.Code.ALREADY_EXISTS, "Lane already exists: " + lane);
        }
        validateTarget(at);
        long seq = nextSequence();
        SessionLogItem.Lane mutation = new SessionLogItem.Lane(seq, lane, at);
        persist(mutation);
        sequence = seq;
        lanes.put(lane, at);
        log.add(mutation);
    }

    synchronized void moveLane(String lane, String to) {
        requireLane(lane);
        validateTarget(to);
        long seq = nextSequence();
        SessionLogItem.Lane mutation = new SessionLogItem.Lane(seq, lane, to);
        persist(mutation);
        sequence = seq;
        lanes.put(lane, to);
        log.add(mutation);
    }

    synchronized SessionEntry append(SessionEntryDraft draft, String lane) {
        requireLane(lane);
        validateUnusedId(draft.id());
        validateDraft(draft);
        long seq = nextSequence();
        long timestamp = clock.millis();
        String parent = lanes.get(lane);
        SessionEntry entry = materialize(draft, parent, seq, timestamp);
        SessionLogItem.Entry mutation = new SessionLogItem.Entry(seq, entry, lane);
        persist(mutation);
        sequence = seq;
        usedIds.add(entry.id());
        entries.add(entry);
        entriesById.put(entry.id(), entry);
        lanes.put(lane, entry.id());
        log.add(mutation);
        return SessionCopies.entry(entry);
    }

    synchronized SessionUsageRow appendUsage(
            String id,
            Usage value,
            String entryId,
            boolean adjustment,
            JsonNode details
    ) {
        SessionRecord record = appendRecord(new SessionRecordDraft.UsageRecord(
                id, "main", adjustment ? "adjustment" : "assistant", value,
                null, entryId, null, null, null, details
        ));
        return usage.stream()
                .filter(row -> row.id().equals(record.id()))
                .findFirst()
                .map(SessionCopies::usage)
                .orElseThrow();
    }

    synchronized SessionRecord appendRecord(SessionRecordDraft input) {
        requireLane(input.lane());
        validateUnusedId(input.id());
        input = SessionRecords.copy(input);
        if (input instanceof SessionRecordDraft.OperationStarted started) {
            LinkedHashMap<String, SessionRecord> open = openOperations.get(started.lane());
            if (open != null && !open.isEmpty()) {
                throw error(
                        SessionError.Code.STORAGE,
                        "Lane " + started.lane() + " already has an open operation "
                                + open.values().iterator().next().id()
                );
            }
        }
        if (input instanceof SessionRecordDraft.UsageRecord value
                && value.entryId() != null && !entriesById.containsKey(value.entryId())) {
            throw error(SessionError.Code.NOT_FOUND, "Entry not found: " + value.entryId());
        }
        long seq = nextSequence();
        long timestamp = clock.millis();
        SessionRecord record = new SessionRecord(seq, timestamp, input);
        SessionLogItem.Record mutation = new SessionLogItem.Record(seq, record);
        persist(mutation);
        sequence = seq;
        usedIds.add(record.id());
        records.add(record);
        if (input instanceof SessionRecordDraft.OperationStarted) {
            openOperations.computeIfAbsent(input.lane(), ignored -> new LinkedHashMap<>())
                    .put(input.id(), record);
        } else if (input instanceof SessionRecordDraft.OperationFinished finished) {
            LinkedHashMap<String, SessionRecord> open = openOperations.get(input.lane());
            latestTerminalByLane.put(input.lane(), new TerminalOperation(
                    open == null ? null : open.get(finished.runId()),
                    record, lanes.get(input.lane())
            ));
            if (open != null) open.remove(finished.runId());
        } else if (input instanceof SessionRecordDraft.UsageRecord value) {
            SessionUsageRow row = new SessionUsageRow(
                    record.id(), seq, timestamp, value.usage(), value.entryId(),
                    "adjustment".equals(value.cause()), value.details()
            );
            usage.add(row);
        }
        log.add(mutation);
        return record.copy();
    }

    synchronized List<SessionRecord> findRecords(SessionRecordQuery query) {
        ArrayList<SessionRecord> ordered = new ArrayList<>(records);
        if (query.order() == SessionEntryQuery.Order.NEWEST_FIRST) {
            Collections.reverse(ordered);
        }
        ArrayList<SessionRecord> result = new ArrayList<>();
        for (SessionRecord record : ordered) {
            SessionRecordDraft value = record.storedValue();
            if (query.lane() != null && !query.lane().equals(record.lane())) continue;
            if (query.type() != null && query.type() != record.type()) continue;
            if (query.runId() != null && !query.runId().equals(operationId(value))) continue;
            if (query.operationKind() != null
                    && (!(value instanceof SessionRecordDraft.OperationStarted started)
                    || started.intent().kind() != query.operationKind())) continue;
            if (query.afterSequence() != null && record.sequence() <= query.afterSequence()) continue;
            result.add(record.copy());
            if (query.limit() != null && result.size() == query.limit()) break;
        }
        return List.copyOf(result);
    }

    SessionRunQueueEventBus queueEvents() {
        return queueEvents;
    }

    synchronized boolean hasAbortRequest(String lane, String runId) {
        return records.stream().anyMatch(record -> record.lane().equals(lane)
                && record.storedValue() instanceof SessionRecordDraft.AbortRequested aborted
                && aborted.runId().equals(runId));
    }

    synchronized boolean isOpenOperation(String lane, String runId) {
        LinkedHashMap<String, SessionRecord> open = openOperations.get(lane);
        return open != null && open.containsKey(runId);
    }

    boolean claimOperationExecution(String lane, String runId) {
        String key = lane + '\0' + runId;
        long expectedSequence;
        synchronized (this) {
            if (!isOpenOperation(lane, runId)) {
                throw error(SessionError.Code.NOT_FOUND,
                        "Open operation not found: " + runId);
            }
            if (!activeOperationExecutions.add(key)) return false;
            expectedSequence = sequence;
        }
        AutoCloseable lease;
        try {
            lease = persistence.acquireOperationExecution(
                    lane, runId, expectedSequence
            );
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                activeOperationExecutions.remove(key);
            }
            throw failure;
        }
        synchronized (this) {
            if (!isOpenOperation(lane, runId)) {
                closeLease(lease);
                activeOperationExecutions.remove(key);
                throw error(SessionError.Code.NOT_FOUND,
                        "Open operation not found: " + runId);
            }
            operationExecutionLeases.put(key, lease);
        }
        return true;
    }

    void releaseOperationExecution(String lane, String runId) {
        AutoCloseable lease;
        AutoCloseable abortSubscription;
        synchronized (this) {
            String key = lane + '\0' + runId;
            activeOperationExecutions.remove(key);
            operationCancellations.remove(key);
            abortSubscription = operationAbortSubscriptions.remove(key);
            lease = operationExecutionLeases.remove(key);
        }
        closeLease(abortSubscription);
        closeLease(lease);
    }

    private static void closeLease(AutoCloseable lease) {
        if (lease == null) return;
        try {
            lease.close();
        } catch (Exception ignored) {
            // Durable work has already settled before execution lease release.
        }
    }

    AutoCloseable registerOperationCancellation(
            String lane,
            String runId,
            Runnable cancellation
    ) {
        String key = lane + '\0' + runId;
        AtomicBoolean cancelled = new AtomicBoolean();
        Runnable once = () -> {
            if (cancelled.compareAndSet(false, true)) cancellation.run();
        };
        boolean cancelNow;
        synchronized (this) {
            if (!activeOperationExecutions.contains(key)) {
                throw error(SessionError.Code.STORAGE,
                        "Operation is not executing: " + runId);
            }
            operationCancellations.computeIfAbsent(
                    key, ignored -> new ArrayList<>()
            ).add(once);
            if (!operationAbortSubscriptions.containsKey(key)) {
                try {
                    AutoCloseable subscription =
                            persistence.observeOperationAbort(
                                    lane, runId,
                                    () -> cancelOperationExecution(lane, runId)
                            );
                    operationAbortSubscriptions.put(
                            key, subscription == null ? () -> { } : subscription
                    );
                } catch (RuntimeException | Error failure) {
                    operationCancellations.get(key).remove(once);
                    throw failure;
                }
            }
            cancelNow = hasAbortRequest(lane, runId);
        }
        if (cancelNow) once.run();
        return () -> {
            synchronized (InMemorySessionState.this) {
                ArrayList<Runnable> callbacks = operationCancellations.get(key);
                if (callbacks != null) callbacks.remove(once);
            }
        };
    }

    void reconcileOperationAbort(String lane, String runId) {
        long expectedSequence;
        synchronized (this) {
            if (hasAbortRequest(lane, runId)) return;
            expectedSequence = sequence;
        }
        List<SessionLogItem> suffix = persistence.reconcileOperationAbort(
                lane, runId, expectedSequence
        );
        if (suffix.isEmpty()) return;
        synchronized (this) {
            if (sequence != expectedSequence) {
                if (hasAbortRequest(lane, runId)) return;
                throw error(SessionError.Code.STORAGE,
                        "Session changed while reconciling abort for " + runId);
            }
            for (SessionLogItem mutation : suffix) replay(mutation);
        }
        cancelOperationExecution(lane, runId);
    }

    void cancelOperationExecution(String lane, String runId) {
        List<Runnable> callbacks;
        synchronized (this) {
            ArrayList<Runnable> registered = operationCancellations.get(
                    lane + '\0' + runId
            );
            callbacks = registered == null ? List.of() : List.copyOf(registered);
        }
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (RuntimeException ignored) {
                // Cancellation is advisory; the durable abort record is authoritative.
            }
        }
    }

    synchronized List<SessionRecord> findOpenOperations(String lane, Integer limit) {
        requireLane(lane);
        if (limit != null && limit <= 0) {
            throw error(SessionError.Code.INVALID_QUERY, "limit must be positive");
        }
        LinkedHashMap<String, SessionRecord> open = openOperations.get(lane);
        if (open == null) return List.of();
        ArrayList<SessionRecord> result = new ArrayList<>(open.values());
        Collections.reverse(result);
        if (limit != null && result.size() > limit) result.subList(limit, result.size()).clear();
        return result.stream().map(SessionRecord::copy).toList();
    }

    synchronized void replay(SessionLogItem mutation) {
        if (mutation.sequence() != sequence + 1) {
            throw error(
                    SessionError.Code.INVALID_ENTRY,
                    "Invalid session mutation: has non-consecutive seq "
                            + mutation.sequence()
            );
        }
        switch (mutation) {
            case SessionLogItem.Entry item -> replayEntry(item);
            case SessionLogItem.Record item -> replayRecord(item);
            case SessionLogItem.Lane item -> {
                validateTarget(item.leafId());
                sequence = item.sequence();
                lanes.put(item.lane(), item.leafId());
                log.add(item);
            }
            case SessionLogItem.Name item -> {
                sequence = item.sequence();
                name = item.name();
                log.add(item);
            }
            case SessionLogItem.Label item -> {
                validateTarget(item.targetId());
                sequence = item.sequence();
                if (item.label() == null) labels.remove(item.targetId());
                else labels.put(item.targetId(), item.label());
                log.add(item);
            }
            case SessionLogItem.Usage ignored -> throw error(
                    SessionError.Code.INVALID_ENTRY,
                    "Legacy usage log items cannot be replayed"
            );
        }
    }

    private void replayEntry(SessionLogItem.Entry item) {
        SessionEntry entry = SessionCopies.entry(item.entry());
        validateUnusedId(entry.id());
        if (entry.parentId() != null && !entriesById.containsKey(entry.parentId())) {
            throw error(
                    SessionError.Code.INVALID_ENTRY,
                    "Invalid session mutation: references missing parent " + entry.parentId()
            );
        }
        if (item.lane() != null) {
            requireLane(item.lane());
            if (!java.util.Objects.equals(lanes.get(item.lane()), entry.parentId())) {
                throw error(
                        SessionError.Code.INVALID_ENTRY,
                        "Invalid session mutation: does not chain to the lane leaf"
                );
            }
        }
        sequence = item.sequence();
        usedIds.add(entry.id());
        entries.add(entry);
        entriesById.put(entry.id(), entry);
        if (item.lane() != null) lanes.put(item.lane(), entry.id());
        log.add(new SessionLogItem.Entry(item.sequence(), entry, item.lane()));
    }

    private void replayRecord(SessionLogItem.Record item) {
        SessionRecord record = item.record().copy();
        requireLane(record.lane());
        validateUnusedId(record.id());
        sequence = item.sequence();
        usedIds.add(record.id());
        records.add(record);
        SessionRecordDraft value = record.storedValue();
        if (value instanceof SessionRecordDraft.OperationStarted) {
            openOperations.computeIfAbsent(record.lane(), ignored -> new LinkedHashMap<>())
                    .put(record.id(), record);
        } else if (value instanceof SessionRecordDraft.OperationFinished finished) {
            LinkedHashMap<String, SessionRecord> open = openOperations.get(record.lane());
            latestTerminalByLane.put(record.lane(), new TerminalOperation(
                    open == null ? null : open.get(finished.runId()),
                    record, lanes.get(record.lane())
            ));
            if (open != null) open.remove(finished.runId());
        } else if (value instanceof SessionRecordDraft.UsageRecord usageRecord) {
            if (usageRecord.entryId() != null
                    && !entriesById.containsKey(usageRecord.entryId())) {
                throw error(
                        SessionError.Code.INVALID_ENTRY,
                        "Invalid session mutation: references missing usage entry "
                                + usageRecord.entryId()
                );
            }
            usage.add(new SessionUsageRow(
                    record.id(), record.sequence(), record.timestamp(),
                    usageRecord.usage(), usageRecord.entryId(),
                    "adjustment".equals(usageRecord.cause()), usageRecord.details()
            ));
        }
        log.add(new SessionLogItem.Record(item.sequence(), record));
    }

    synchronized TerminalOperation latestTerminal(String lane) {
        requireLane(lane);
        TerminalOperation terminal = latestTerminalByLane.get(lane);
        return terminal == null ? null : terminal.copy();
    }

    synchronized SessionEntry getEntry(String id) {
        SessionEntry entry = entriesById.get(id);
        return entry == null ? null : SessionCopies.entry(entry);
    }

    synchronized List<SessionEntry> findEntries(SessionEntryQuery query) {
        return filter(ordered(entries, query.order()), query).stream()
                .map(SessionCopies::entry)
                .toList();
    }

    synchronized List<SessionEntry> findBranch(String defaultLane, SessionBranchQuery query) {
        String start = query.start() == null ? leaf(defaultLane) : query.start();
        if (start == null) return List.of();
        SessionEntry current = entriesById.get(start);
        if (current == null) throw error(SessionError.Code.NOT_FOUND, "Entry not found: " + start);
        ArrayList<SessionEntry> path = new ArrayList<>();
        HashSet<String> visited = new HashSet<>();
        while (current != null) {
            if (!visited.add(current.id())) {
                throw error(SessionError.Code.INVALID_ENTRY, "Session branch contains a cycle at " + current.id());
            }
            path.add(current);
            if (current.parentId() == null) break;
            current = entriesById.get(current.parentId());
            if (current == null) {
                throw error(SessionError.Code.INVALID_ENTRY, "Entry not found: " + path.getLast().parentId());
            }
        }
        if (query.entries().order() == SessionEntryQuery.Order.OLDEST_FIRST) {
            Collections.reverse(path);
        }
        ArrayList<SessionEntry> bounded = new ArrayList<>();
        for (SessionEntry entry : path) {
            bounded.add(entry);
            if (entry.id().equals(query.stopAtId()) || entry.type() == query.stopAtType()) break;
        }
        return filter(bounded, query.entries()).stream()
                .map(SessionCopies::entry)
                .toList();
    }

    synchronized String name() {
        return name;
    }

    synchronized void name(String value) {
        long seq = nextSequence();
        SessionLogItem.Name mutation = new SessionLogItem.Name(seq, value);
        persist(mutation);
        sequence = seq;
        name = value;
        log.add(mutation);
    }

    synchronized String label(String id) {
        return labels.get(id);
    }

    synchronized void label(String id, String value) {
        validateTarget(id);
        long seq = nextSequence();
        SessionLogItem.Label mutation = new SessionLogItem.Label(seq, id, value);
        persist(mutation);
        sequence = seq;
        if (value == null) labels.remove(id);
        else labels.put(id, value);
        log.add(mutation);
    }

    synchronized List<SessionUsageRow> usage() {
        return usage.stream().map(SessionCopies::usage).toList();
    }

    synchronized SessionStats stats() {
        long messages = entries.stream().filter(SessionEntry.Message.class::isInstance).count();
        long cached = 0;
        long uncached = 0;
        long total = 0;
        double cost = 0;
        for (SessionUsageRow row : usage) {
            cached += row.usage().cacheRead();
            uncached += row.usage().input() + row.usage().cacheWrite();
            total += row.usage().totalTokens();
            cost += row.usage().cost().total();
        }
        return new SessionStats(messages, cached, uncached, total, cost);
    }

    synchronized String leafAtSequence(String lane, long atSequence) {
        requireLane(lane);
        String leaf = null;
        for (SessionLogItem item : log) {
            if (item.sequence() > atSequence) break;
            if (item instanceof SessionLogItem.Entry entry
                    && lane.equals(entry.lane())) {
                leaf = entry.entry().id();
            } else if (item instanceof SessionLogItem.Lane moved
                    && lane.equals(moved.lane())) {
                leaf = moved.leafId();
            }
        }
        return leaf;
    }

    synchronized List<SessionLogItem> log(long afterSequence, Integer limit) {
        if (afterSequence < 0 || limit != null && limit <= 0) {
            throw error(SessionError.Code.INVALID_QUERY, "Invalid log bounds");
        }
        return log.stream()
                .filter(item -> item.sequence() > afterSequence)
                .limit(limit == null ? Long.MAX_VALUE : limit)
                .map(InMemorySessionState::copyLog)
                .toList();
    }

    synchronized InMemorySessionState retainedCopy(
            SessionMetadata destination,
            SessionRetainedCopyOptions options
    ) {
        InMemorySessionState target = new InMemorySessionState(destination, clock);
        HashSet<Long> found = new HashSet<>();
        long targetSequence = 0;
        for (SessionLogItem mutation : log) {
            if (!options.retainedSequences().contains(mutation.sequence())) continue;
            found.add(mutation.sequence());
            target.replay(withSequence(mutation, ++targetSequence));
        }
        if (found.size() != options.retainedSequences().size()) {
            HashSet<Long> missing = new HashSet<>(options.retainedSequences());
            missing.removeAll(found);
            throw error(
                    SessionError.Code.INVALID_QUERY,
                    "Retained mutation sequences not found: " + missing.stream().sorted().toList()
            );
        }
        List<SessionEntry> retainedEntries = target.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        ));
        for (AgentSession.LanePointer retainedLane : target.lanes()) {
            String lane = retainedLane.lane();
            SessionRecordLogValidator.validate(
                    lane,
                    target.findOpenOperations(lane, null),
                    target.findRecords(new SessionRecordQuery(
                            lane, null, null, null, null,
                            SessionEntryQuery.Order.OLDEST_FIRST, null
                    )),
                    retainedEntries
            );
        }
        return target;
    }

    synchronized InMemorySessionState fork(SessionMetadata destination, SessionForkOptions options) {
        InMemorySessionState target = new InMemorySessionState(destination, clock);
        List<SessionEntry> copied;
        LinkedHashMap<String, String> forkLanes = new LinkedHashMap<>();
        if (options.scope() == SessionForkOptions.Scope.TREE) {
            copied = List.copyOf(entries);
            forkLanes.putAll(lanes);
        } else {
            String selected = options.entryId() == null ? lanes.get("main") : options.entryId();
            String targetId = null;
            if (selected != null) {
                SessionEntry entry = entriesById.get(selected);
                if (!(entry instanceof SessionEntry.Message)) {
                    throw error(
                            SessionError.Code.INVALID_FORK_TARGET,
                            "Fork target is not a message entry: " + selected
                    );
                }
                targetId = options.position() == SessionForkOptions.Position.AT
                        ? entry.id()
                        : entry.parentId();
            }
            copied = targetId == null
                    ? List.of()
                    : oldestBranch(targetId);
            forkLanes.put("main", targetId);
        }
        target.entries.clear();
        target.entriesById.clear();
        target.usedIds.clear();
        target.lanes.clear();
        target.log.clear();
        target.sequence = 0;
        for (SessionEntry source : copied) {
            SessionEntry copiedEntry = withSequence(source, target.nextSequence());
            target.sequence = copiedEntry.sequence();
            target.entries.add(copiedEntry);
            target.entriesById.put(copiedEntry.id(), copiedEntry);
            target.usedIds.add(copiedEntry.id());
            target.log.add(new SessionLogItem.Entry(copiedEntry.sequence(), copiedEntry));
        }
        for (Map.Entry<String, String> lane : forkLanes.entrySet()) {
            long seq = target.nextSequence();
            target.sequence = seq;
            target.lanes.put(lane.getKey(), lane.getValue());
            target.log.add(new SessionLogItem.Lane(seq, lane.getKey(), lane.getValue()));
        }
        if (name != null) target.name(name);
        Set<String> copiedIds = target.entriesById.keySet();
        for (Map.Entry<String, String> label : labels.entrySet()) {
            if (copiedIds.contains(label.getKey())) target.label(label.getKey(), label.getValue());
        }
        return target;
    }

    private List<SessionEntry> oldestBranch(String start) {
        ArrayList<SessionEntry> result = new ArrayList<>();
        SessionEntry current = entriesById.get(start);
        while (current != null) {
            result.add(current);
            current = current.parentId() == null ? null : entriesById.get(current.parentId());
        }
        Collections.reverse(result);
        return result;
    }

    private List<SessionEntry> filter(List<SessionEntry> source, SessionEntryQuery query) {
        ArrayList<SessionEntry> result = new ArrayList<>();
        for (SessionEntry entry : source) {
            if (query.type() != null && entry.type() != query.type()) continue;
            if (query.customType() != null
                    && (!(entry instanceof SessionEntry.Custom custom)
                    || !custom.customType().equals(query.customType()))) continue;
            if (query.afterSequence() != null) {
                boolean accepted = query.order() == SessionEntryQuery.Order.OLDEST_FIRST
                        ? entry.sequence() > query.afterSequence()
                        : entry.sequence() < query.afterSequence();
                if (!accepted) continue;
            }
            result.add(entry);
            if (query.limit() != null && result.size() == query.limit()) break;
        }
        return result;
    }

    private static List<SessionEntry> ordered(
            List<SessionEntry> source,
            SessionEntryQuery.Order order
    ) {
        ArrayList<SessionEntry> result = new ArrayList<>(source);
        if (order == SessionEntryQuery.Order.NEWEST_FIRST) Collections.reverse(result);
        return result;
    }

    private SessionEntry materialize(
            SessionEntryDraft draft,
            String parent,
            long seq,
            long timestamp
    ) {
        return switch (draft) {
            case SessionEntryDraft.Message value -> new SessionEntry.Message(
                    value.id(), parent, seq, timestamp,
                    SessionCopies.message(value.message()), value.terminate()
            );
            case SessionEntryDraft.ModelChange value -> new SessionEntry.ModelChange(
                    value.id(), parent, seq, timestamp, value.provider(), value.modelId()
            );
            case SessionEntryDraft.ThinkingLevelChange value ->
                    new SessionEntry.ThinkingLevelChange(
                            value.id(), parent, seq, timestamp, value.thinkingLevel()
                    );
            case SessionEntryDraft.ActiveToolsChange value -> new SessionEntry.ActiveToolsChange(
                    value.id(), parent, seq, timestamp, value.activeToolNames()
            );
            case SessionEntryDraft.Compaction value -> new SessionEntry.Compaction(
                    value.id(), parent, seq, timestamp, value.summary(),
                    SessionCopies.messages(value.retainedTail()), value.tokensBefore(),
                    value.details(), value.usage()
            );
            case SessionEntryDraft.BranchSummary value -> new SessionEntry.BranchSummary(
                    value.id(), parent, seq, timestamp, value.fromId(), value.summary(),
                    value.details(), value.usage()
            );
            case SessionEntryDraft.Custom value -> new SessionEntry.Custom(
                    value.id(), parent, seq, timestamp, value.customType(), value.data()
            );
        };
    }

    private static SessionLogItem withSequence(SessionLogItem mutation, long sequence) {
        return switch (mutation) {
            case SessionLogItem.Entry value -> new SessionLogItem.Entry(
                    sequence, withSequence(value.entry(), sequence), value.lane()
            );
            case SessionLogItem.Lane value -> new SessionLogItem.Lane(
                    sequence, value.lane(), value.leafId()
            );
            case SessionLogItem.Name value -> new SessionLogItem.Name(sequence, value.name());
            case SessionLogItem.Label value -> new SessionLogItem.Label(
                    sequence, value.targetId(), value.label()
            );
            case SessionLogItem.Record value -> new SessionLogItem.Record(
                    sequence,
                    new SessionRecord(
                            sequence, value.record().timestamp(), value.record().storedValue()
                    )
            );
            case SessionLogItem.Usage ignored -> throw error(
                    SessionError.Code.INVALID_ENTRY,
                    "Legacy usage log items cannot be retained"
            );
        };
    }

    private static SessionEntry withSequence(SessionEntry entry, long sequence) {
        return switch (entry) {
            case SessionEntry.Message value -> new SessionEntry.Message(
                    value.id(), value.parentId(), sequence, value.timestamp(),
                    SessionCopies.message(value.message()), value.terminate()
            );
            case SessionEntry.ModelChange value -> new SessionEntry.ModelChange(
                    value.id(), value.parentId(), sequence, value.timestamp(),
                    value.provider(), value.modelId()
            );
            case SessionEntry.ThinkingLevelChange value -> new SessionEntry.ThinkingLevelChange(
                    value.id(), value.parentId(), sequence, value.timestamp(), value.thinkingLevel()
            );
            case SessionEntry.ActiveToolsChange value -> new SessionEntry.ActiveToolsChange(
                    value.id(), value.parentId(), sequence, value.timestamp(), value.activeToolNames()
            );
            case SessionEntry.Compaction value -> new SessionEntry.Compaction(
                    value.id(), value.parentId(), sequence, value.timestamp(), value.summary(),
                    SessionCopies.messages(value.retainedTail()), value.tokensBefore(),
                    value.details(), value.usage()
            );
            case SessionEntry.BranchSummary value -> new SessionEntry.BranchSummary(
                    value.id(), value.parentId(), sequence, value.timestamp(), value.fromId(),
                    value.summary(), value.details(), value.usage()
            );
            case SessionEntry.Custom value -> new SessionEntry.Custom(
                    value.id(), value.parentId(), sequence, value.timestamp(),
                    value.customType(), value.data()
            );
        };
    }

    private static SessionLogItem copyLog(SessionLogItem item) {
        return switch (item) {
            case SessionLogItem.Entry value -> new SessionLogItem.Entry(
                    value.sequence(), SessionCopies.entry(value.entry()), value.lane()
            );
            case SessionLogItem.Lane value -> value;
            case SessionLogItem.Name value -> value;
            case SessionLogItem.Label value -> value;
            case SessionLogItem.Record value -> new SessionLogItem.Record(
                    value.sequence(), value.record().copy()
            );
            case SessionLogItem.Usage value -> new SessionLogItem.Usage(
                    value.sequence(), SessionCopies.usage(value.row())
            );
        };
    }

    private static String operationId(SessionRecordDraft record) {
        return switch (record) {
            case SessionRecordDraft.OperationStarted value -> value.id();
            case SessionRecordDraft.AbortRequested value -> value.runId();
            case SessionRecordDraft.OperationFinished value -> value.runId();
            case SessionRecordDraft.StepAttempt value -> value.runId();
            case SessionRecordDraft.ToolStarted value -> value.runId();
            case SessionRecordDraft.QueueEnqueued value -> value.runId();
            case SessionRecordDraft.QueueCancelled value -> value.runId();
            case SessionRecordDraft.WriteDeferred value -> value.runId();
            case SessionRecordDraft.UsageRecord value -> value.runId();
        };
    }

    private static void validateDraft(SessionEntryDraft draft) {
        if (draft instanceof SessionEntryDraft.Message message) {
            validateMessage(message.message());
        } else if (draft instanceof SessionEntryDraft.Compaction compaction) {
            compaction.retainedTail().forEach(InMemorySessionState::validateMessage);
        }
    }

    private static void validateMessage(AgentMessage message) {
        if (message instanceof AssistantMessage assistant
                && assistant.stopReason() == StopReason.PENDING) {
            throw error(
                    SessionError.Code.INVALID_ENTRY,
                    "Pending assistant messages cannot be persisted"
            );
        }
        SessionCopies.message(message);
    }

    private void validateUnusedId(String id) {
        if (usedIds.contains(id)) {
            throw error(SessionError.Code.ALREADY_EXISTS, "Session id already exists: " + id);
        }
    }

    private void validateTarget(String id) {
        if (id != null && !entriesById.containsKey(id)) {
            throw error(SessionError.Code.NOT_FOUND, "Entry not found: " + id);
        }
    }

    private void requireLane(String lane) {
        if (!lanes.containsKey(lane)) {
            throw error(SessionError.Code.INVALID_LANE, "Lane not found: " + lane);
        }
    }

    private static void requireLaneName(String lane) {
        if (lane == null || lane.isBlank()) {
            throw error(SessionError.Code.INVALID_LANE, "Lane name must not be blank");
        }
    }

    synchronized <T> T transact(Supplier<T> operation) {
        if (transactionBuffer != null) {
            throw new IllegalStateException("Nested session transactions are not supported");
        }
        MutableSnapshot before = snapshot();
        transactionBuffer = new ArrayList<>();
        try {
            T result = operation.get();
            List<SessionLogItem> mutations = List.copyOf(transactionBuffer);
            persistence.persistBatch(mutations);
            transactionBuffer = null;
            return result;
        } catch (Throwable failure) {
            restore(before);
            transactionBuffer = null;
            throw failure;
        }
    }

    private void persist(SessionLogItem mutation) {
        if (transactionBuffer != null) transactionBuffer.add(copyLog(mutation));
        else persistence.persist(mutation);
    }

    private MutableSnapshot snapshot() {
        LinkedHashMap<String, LinkedHashMap<String, SessionRecord>> open = new LinkedHashMap<>();
        openOperations.forEach((lane, records) ->
                open.put(lane, new LinkedHashMap<>(records)));
        return new MutableSnapshot(
                new ArrayList<>(entries), new LinkedHashMap<>(entriesById),
                new ArrayList<>(usage), new ArrayList<>(records), open,
                new LinkedHashMap<>(latestTerminalByLane),
                new HashSet<>(usedIds), new LinkedHashMap<>(lanes),
                new LinkedHashMap<>(labels), new ArrayList<>(log), sequence, name
        );
    }

    private void restore(MutableSnapshot snapshot) {
        entries.clear();
        entries.addAll(snapshot.entries());
        entriesById.clear();
        entriesById.putAll(snapshot.entriesById());
        usage.clear();
        usage.addAll(snapshot.usage());
        records.clear();
        records.addAll(snapshot.records());
        openOperations.clear();
        snapshot.openOperations().forEach((lane, values) ->
                openOperations.put(lane, new LinkedHashMap<>(values)));
        latestTerminalByLane.clear();
        latestTerminalByLane.putAll(snapshot.latestTerminalByLane());
        usedIds.clear();
        usedIds.addAll(snapshot.usedIds());
        lanes.clear();
        lanes.putAll(snapshot.lanes());
        labels.clear();
        labels.putAll(snapshot.labels());
        log.clear();
        log.addAll(snapshot.log());
        sequence = snapshot.sequence();
        name = snapshot.name();
    }

    private long nextSequence() {
        return sequence + 1;
    }

    private record MutableSnapshot(
            ArrayList<SessionEntry> entries,
            LinkedHashMap<String, SessionEntry> entriesById,
            ArrayList<SessionUsageRow> usage,
            ArrayList<SessionRecord> records,
            LinkedHashMap<String, LinkedHashMap<String, SessionRecord>> openOperations,
            LinkedHashMap<String, TerminalOperation> latestTerminalByLane,
            Set<String> usedIds,
            LinkedHashMap<String, String> lanes,
            LinkedHashMap<String, String> labels,
            ArrayList<SessionLogItem> log,
            long sequence,
            String name
    ) {
    }

    record TerminalOperation(
            SessionRecord started,
            SessionRecord finished,
            String leafId
    ) {
        TerminalOperation {
            started = started == null ? null : started.copy();
            finished = finished.copy();
        }

        TerminalOperation copy() {
            return new TerminalOperation(started, finished, leafId);
        }
    }

    @FunctionalInterface
    interface PersistenceSink {
        void persist(SessionLogItem mutation);

        default AutoCloseable acquireOperationExecution(
                String lane,
                String runId,
                long expectedSequence
        ) {
            return () -> { };
        }

        default List<SessionLogItem> reconcileOperationAbort(
                String lane,
                String runId,
                long expectedSequence
        ) {
            return List.of();
        }

        default AutoCloseable observeOperationAbort(
                String lane,
                String runId,
                Runnable cancellation
        ) {
            return () -> { };
        }

        default void persistBatch(List<SessionLogItem> mutations) {
            if (mutations.isEmpty()) return;
            if (mutations.size() == 1) {
                persist(mutations.getFirst());
                return;
            }
            throw error(
                    SessionError.Code.STORAGE,
                    "Persistence backend does not support atomic mutation batches"
            );
        }
    }

    private static SessionError error(SessionError.Code code, String message) {
        return new SessionError(code, message);
    }
}
