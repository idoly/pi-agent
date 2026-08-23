package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/** Record-derived durable steer, follow-up, and next-run queues. */
@ExperimentalSessionApi
public final class SessionRunQueue {
    private SessionRunQueue() {
    }

    public static CompletionStage<Pending> enqueueMessage(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            AgentMessage message
    ) {
        Objects.requireNonNull(session, "session");
        return enqueue(
                session, queue, runId,
                new SessionEntryDraft.Message(
                        session.idGenerator().next(), message
                )
        );
    }

    public static CompletionStage<Pending> enqueue(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            SessionEntryDraft target
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(target, "target");
        validateRunId(queue, runId);
        return session.transaction(transaction -> {
            List<SessionRecord> records = records(transaction, session.lane());
            if (queue != SessionRecordDraft.Queue.NEXT_RUN) {
                requireOpenRun(transaction, records, runId);
                if (transaction.abortRequested(runId)) {
                    throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                            "Cannot enqueue after abort request for run " + runId);
                }
            }
            if (transaction.entry(target.id()) != null
                    || records.stream().anyMatch(record ->
                    record.id().equals(target.id())
                            || record.value() instanceof SessionRecordDraft.QueueEnqueued value
                            && value.target().id().equals(target.id()))) {
                throw new SessionError(SessionError.Code.ALREADY_EXISTS,
                        "Queue target id already exists: " + target.id());
            }
            SessionRecord record = transaction.appendRecord(
                    new SessionRecordDraft.QueueEnqueued(
                            session.idGenerator().next(), session.lane(),
                            queue, runId, target
                    )
            );
            return pending(record);
        }).thenApply(pending -> {
            session.queueEvents().emit(new SessionRunQueueEvent.Enqueued(pending));
            return pending;
        });
    }

    public static CompletionStage<Boolean> cancel(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            String entryId
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(entryId, "entryId");
        validateRunId(queue, runId);
        return session.transaction(transaction -> {
            List<SessionRecord> records = records(transaction, session.lane());
            SessionRecord enqueue = records.stream()
                    .filter(record -> record.value()
                            instanceof SessionRecordDraft.QueueEnqueued value
                            && value.queue() == queue
                            && Objects.equals(value.runId(), runId)
                            && value.target().id().equals(entryId))
                    .findFirst().orElse(null);
            if (enqueue == null) {
                throw new SessionError(SessionError.Code.NOT_FOUND,
                        "Pending queue entry not found: " + entryId);
            }
            if (transaction.entry(entryId) != null) {
                throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                        "Queue entry is already consumed: " + entryId);
            }
            boolean canceled = records.stream().anyMatch(record ->
                    record.value() instanceof SessionRecordDraft.QueueCancelled value
                            && value.entryId().equals(entryId)
                            && Objects.equals(value.runId(), runId));
            if (canceled) return false;
            transaction.appendRecord(new SessionRecordDraft.QueueCancelled(
                    session.idGenerator().next(), session.lane(), runId, entryId
            ));
            return true;
        }).thenApply(changed -> {
            if (changed) session.queueEvents().emit(
                    new SessionRunQueueEvent.Cancelled(queue, runId, entryId)
            );
            return changed;
        });
    }

    public static CompletionStage<List<Pending>> pending(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(queue, "queue");
        validateRunId(queue, runId);
        validateLimit(limit);
        return session.transaction(transaction -> projectPending(
                transaction, session.lane(), queue, runId, limit
        ));
    }

    public static SessionRunQueueEventBus.WatchHandle<List<Pending>> watch(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit
    ) {
        return watch(session, queue, runId, limit,
                SnapshotEventBus.DEFAULT_BUFFER_CAPACITY);
    }

    public static SessionRunQueueEventBus.WatchHandle<List<Pending>> watch(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit,
            int maxBufferedEvents
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(queue, "queue");
        validateRunId(queue, runId);
        validateLimit(limit);
        return session.queueEvents().watch(() -> pending(
                session, queue, runId, limit
        ).toCompletableFuture().join(), maxBufferedEvents);
    }

    public static CompletionStage<List<SessionEntry>> drain(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(queue, "queue");
        validateRunId(queue, runId);
        validateLimit(limit);
        return session.transaction(transaction -> drain(
                transaction, session.lane(), queue, runId, limit
        )).thenApply(entries -> {
            emitConsumed(session, queue, runId, entries);
            return entries;
        });
    }

    static void emitConsumed(
            AgentSession session,
            SessionRecordDraft.Queue queue,
            String runId,
            List<? extends SessionEntry> entries
    ) {
        if (entries.isEmpty()) return;
        session.queueEvents().emit(new SessionRunQueueEvent.Consumed(
                queue, runId, entries.stream().map(SessionEntry::id).toList()
        ));
    }

    static List<SessionEntry> drain(
            SessionTransaction transaction,
            String lane,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit
    ) {
        if (queue != SessionRecordDraft.Queue.NEXT_RUN) {
            List<SessionRecord> records = records(transaction, lane);
            requireOpenRun(transaction, records, runId);
            if (transaction.abortRequested(runId)) return List.of();
        }
        List<Pending> pending = projectPending(
                transaction, lane, queue, runId, limit
        );
        ArrayList<SessionEntry> entries = new ArrayList<>();
        for (Pending item : pending) {
            entries.add(transaction.append(item.target()));
        }
        return List.copyOf(entries);
    }

    static List<Pending> pending(
            SessionTransaction transaction,
            String lane,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit
    ) {
        validateRunId(queue, runId);
        validateLimit(limit);
        return projectPending(transaction, lane, queue, runId, limit);
    }

    private static List<Pending> projectPending(
            SessionTransaction transaction,
            String lane,
            SessionRecordDraft.Queue queue,
            String runId,
            Integer limit
    ) {
        List<SessionRecord> records = records(transaction, lane);
        LinkedHashMap<String, SessionRecord> enqueued = new LinkedHashMap<>();
        Set<String> canceled = new HashSet<>();
        for (SessionRecord record : records) {
            if (record.value() instanceof SessionRecordDraft.QueueEnqueued value
                    && value.queue() == queue
                    && Objects.equals(value.runId(), runId)) {
                enqueued.putIfAbsent(value.target().id(), record);
            } else if (record.value() instanceof SessionRecordDraft.QueueCancelled value
                    && Objects.equals(value.runId(), runId)) {
                canceled.add(value.entryId());
            }
        }
        ArrayList<Pending> result = new ArrayList<>();
        for (SessionRecord record : enqueued.values()) {
            SessionRecordDraft.QueueEnqueued value =
                    (SessionRecordDraft.QueueEnqueued) record.value();
            if (canceled.contains(value.target().id())) continue;
            if (transaction.entry(value.target().id()) != null) continue;
            result.add(pending(record));
            if (limit != null && result.size() == limit) break;
        }
        return List.copyOf(result);
    }

    private static Pending pending(SessionRecord record) {
        SessionRecordDraft.QueueEnqueued value =
                (SessionRecordDraft.QueueEnqueued) record.value();
        return new Pending(
                record.id(), record.sequence(), value.queue(), value.runId(),
                SessionCopies.draft(value.target())
        );
    }

    private static List<SessionRecord> records(
            SessionTransaction transaction,
            String lane
    ) {
        return transaction.findRecords(new SessionRecordQuery(
                lane, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        ));
    }

    private static void requireOpenRun(
            SessionTransaction transaction,
            List<SessionRecord> records,
            String runId
    ) {
        if (!transaction.operationOpen(runId)) {
            throw new SessionError(SessionError.Code.NOT_FOUND,
                    "Open operation not found: " + runId);
        }
        boolean run = records.stream().anyMatch(record ->
                record.id().equals(runId)
                        && record.value() instanceof SessionRecordDraft.OperationStarted started
                        && started.intent() instanceof SessionRecordDraft.OperationIntent.Run);
        if (!run) {
            throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                    "Open operation is not a run: " + runId);
        }
    }

    private static void validateRunId(
            SessionRecordDraft.Queue queue,
            String runId
    ) {
        if ((queue == SessionRecordDraft.Queue.NEXT_RUN) == (runId != null)) {
            throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                    "runId is required for steer/follow-up and forbidden for next-run");
        }
    }

    private static void validateLimit(Integer limit) {
        if (limit != null && limit <= 0) {
            throw new SessionError(SessionError.Code.INVALID_QUERY,
                    "limit must be positive");
        }
    }

    public record Pending(
            String enqueueRecordId,
            long enqueueSequence,
            SessionRecordDraft.Queue queue,
            String runId,
            SessionEntryDraft target
    ) {
        public Pending {
            Objects.requireNonNull(enqueueRecordId, "enqueueRecordId");
            Objects.requireNonNull(queue, "queue");
            target = SessionCopies.draft(Objects.requireNonNull(target, "target"));
        }

        @Override
        public SessionEntryDraft target() {
            return SessionCopies.draft(target);
        }
    }
}
