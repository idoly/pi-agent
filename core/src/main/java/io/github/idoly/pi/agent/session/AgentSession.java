package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

/** Lane-bound typed view over one durable session tree. */
public final class AgentSession {
    private final InMemorySessionState state;
    private final SessionIdGenerator idGenerator;
    private final String lane;
    private final SessionLifecycle lifecycle;

    AgentSession(
            InMemorySessionState state,
            SessionIdGenerator idGenerator,
            String lane
    ) {
        this(state, idGenerator, lane, new SessionLifecycle());
    }

    private AgentSession(
            InMemorySessionState state,
            SessionIdGenerator idGenerator,
            String lane,
            SessionLifecycle lifecycle
    ) {
        this.state = Objects.requireNonNull(state, "state");
        this.idGenerator = Objects.requireNonNull(idGenerator, "idGenerator");
        this.lane = Objects.requireNonNull(lane, "lane");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    }

    public CompletionStage<SessionMetadata> metadata() {
        return stage(state::metadata);
    }

    public String lane() {
        return lane;
    }

    public AgentSession view(String lane) {
        lifecycle.requireOpen();
        state.leaf(lane);
        return new AgentSession(state, idGenerator, lane, lifecycle);
    }

    public CompletionStage<Void> close() {
        lifecycle.close();
        return CompletableFuture.completedFuture(null);
    }

    public boolean isClosed() {
        return lifecycle.isClosed();
    }

    public CompletionStage<String> leafId() {
        return stage(() -> state.leaf(lane));
    }

    public CompletionStage<List<LanePointer>> lanes() {
        return stage(state::lanes);
    }

    public CompletionStage<Void> createLane(String lane, String at) {
        return stage(() -> {
            state.createLane(lane, at);
            return null;
        });
    }

    public CompletionStage<Void> moveLane(String to) {
        return stage(() -> {
            state.moveLane(lane, to);
            return null;
        });
    }

    public CompletionStage<SessionEntry> append(SessionEntryDraft entry) {
        return stage(() -> state.append(entry, lane));
    }

    public <T> CompletionStage<T> transaction(Function<SessionTransaction, T> operation) {
        Objects.requireNonNull(operation, "operation");
        return stage(() -> state.transact(() -> {
            SessionTransaction transaction = new SessionTransaction(state, idGenerator, lane);
            try {
                return operation.apply(transaction);
            } finally {
                transaction.close();
            }
        }));
    }

    public CompletionStage<String> appendMessage(AgentMessage message) {
        return append(new SessionEntryDraft.Message(nextId(), message))
                .thenApply(SessionEntry::id);
    }

    public CompletionStage<String> appendCustom(String customType, JsonNode data) {
        return append(new SessionEntryDraft.Custom(nextId(), customType, data))
                .thenApply(SessionEntry::id);
    }

    public CompletionStage<SessionEntry.Compaction> appendCompaction(
            String summary,
            List<AgentMessage> retainedTail,
            long tokensBefore,
            JsonNode details,
            Usage usage
    ) {
        return append(new SessionEntryDraft.Compaction(
                nextId(), summary, retainedTail, tokensBefore, details, usage
        )).thenApply(SessionEntry.Compaction.class::cast);
    }

    public CompletionStage<SessionEntry.BranchSummary> appendBranchSummary(
            String fromId,
            String summary,
            JsonNode details,
            Usage usage
    ) {
        return append(new SessionEntryDraft.BranchSummary(
                nextId(), fromId, summary, details, usage
        )).thenApply(SessionEntry.BranchSummary.class::cast);
    }

    public CompletionStage<SessionRecord> appendRecord(SessionRecordDraft record) {
        return stage(() -> state.appendRecord(record));
    }

    public CompletionStage<List<SessionRecord>> findRecords(SessionRecordQuery query) {
        return stage(() -> state.findRecords(
                query == null ? SessionRecordQuery.all() : query
        ));
    }

    public CompletionStage<List<SessionRecord>> findRecords() {
        return findRecords(SessionRecordQuery.all());
    }

    public CompletionStage<List<SessionRecord>> findOpenOperations(String lane, Integer limit) {
        return stage(() -> state.findOpenOperations(lane, limit));
    }

    public CompletionStage<Void> validateRecordLog(String lane) {
        CompletionStage<List<SessionRecord>> open = findOpenOperations(lane, 2);
        CompletionStage<List<SessionRecord>> records = findRecords(new SessionRecordQuery(
                lane, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        ));
        CompletionStage<List<SessionEntry>> entries = findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        ));
        return open.thenCombine(records, ValidationInputs::new)
                .thenCombine(entries, (inputs, sessionEntries) -> {
                    SessionRecordLogValidator.validate(
                            lane, inputs.open(), inputs.records(), sessionEntries
                    );
                    return null;
                });
    }

    public CompletionStage<SessionUsageRow> recordUsage(
            Usage usage,
            String entryId,
            boolean adjustment,
            JsonNode details
    ) {
        Objects.requireNonNull(usage, "usage");
        return stage(() -> state.appendUsage(
                nextId(), usage, entryId, adjustment, details
        ));
    }

    public CompletionStage<SessionEntry> entry(String id) {
        return stage(() -> state.getEntry(id));
    }

    public CompletionStage<List<SessionEntry>> findEntries(SessionEntryQuery query) {
        return stage(() -> state.findEntries(query == null ? SessionEntryQuery.ALL : query));
    }

    public CompletionStage<List<SessionEntry>> findEntries() {
        return findEntries(SessionEntryQuery.ALL);
    }

    public CompletionStage<SessionEntry> findEntry(SessionEntryQuery query) {
        SessionEntryQuery effective = query == null ? SessionEntryQuery.ALL : query;
        SessionEntryQuery one = new SessionEntryQuery(
                effective.type(), effective.customType(), effective.order(), 1,
                effective.afterSequence()
        );
        return findEntries(one).thenApply(entries -> entries.isEmpty() ? null : entries.getFirst());
    }

    public CompletionStage<List<SessionEntry>> findEntriesOnBranch(SessionBranchQuery query) {
        return stage(() -> state.findBranch(
                lane, query == null ? SessionBranchQuery.current() : query
        ));
    }

    public CompletionStage<SessionEntry> findEntryOnBranch(SessionBranchQuery query) {
        SessionBranchQuery effective = query == null ? SessionBranchQuery.current() : query;
        SessionEntryQuery entries = effective.entries();
        SessionBranchQuery one = new SessionBranchQuery(
                effective.start(), effective.stopAtId(), effective.stopAtType(),
                new SessionEntryQuery(
                        entries.type(), entries.customType(), entries.order(), 1,
                        entries.afterSequence()
                )
        );
        return findEntriesOnBranch(one)
                .thenApply(values -> values.isEmpty() ? null : values.getFirst());
    }

    public CompletionStage<String> name() {
        return stage(state::name);
    }

    public CompletionStage<Void> name(String value) {
        return stage(() -> {
            state.name(value);
            return null;
        });
    }

    public CompletionStage<String> label(String entryId) {
        return stage(() -> state.label(entryId));
    }

    public CompletionStage<Void> label(String entryId, String value) {
        return stage(() -> {
            state.label(entryId, value);
            return null;
        });
    }

    public CompletionStage<List<SessionUsageRow>> usage() {
        return stage(state::usage);
    }

    public CompletionStage<SessionStats> stats() {
        return stage(state::stats);
    }

    public CompletionStage<SessionOperationInspector.LastResult> lastResult() {
        return SessionOperationInspector.lastResult(this);
    }

    public CompletionStage<List<SessionLogItem>> log(long afterSequence, Integer limit) {
        return stage(() -> state.log(afterSequence, limit));
    }

    public SessionRunQueueEventBus queueEvents() {
        lifecycle.requireOpen();
        return state.queueEvents();
    }

    public CompletionStage<SessionContext> context() {
        return findEntriesOnBranch(new SessionBranchQuery(
                null, null, null,
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
        )).thenApply(SessionContextBuilder::build);
    }

    SessionIdGenerator idGenerator() {
        lifecycle.requireOpen();
        return idGenerator;
    }

    InMemorySessionState state() {
        lifecycle.requireOpen();
        return state;
    }

    InMemorySessionState rawState() {
        return state;
    }

    private String nextId() {
        lifecycle.requireOpen();
        return idGenerator.next();
    }

    private <T> CompletionStage<T> stage(Supplier<T> operation) {
        try {
            lifecycle.requireOpen();
            return CompletableFuture.completedFuture(operation.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private record ValidationInputs(
            List<SessionRecord> open,
            List<SessionRecord> records
    ) {
    }

    public record LanePointer(String lane, String leafId) {
    }
}
