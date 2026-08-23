package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Objects;

/** Synchronous mutation view used inside {@link AgentSession#transaction}. */
public final class SessionTransaction {
    private final InMemorySessionState state;
    private final SessionIdGenerator idGenerator;
    private final String lane;
    private boolean active = true;

    SessionTransaction(
            InMemorySessionState state,
            SessionIdGenerator idGenerator,
            String lane
    ) {
        this.state = state;
        this.idGenerator = idGenerator;
        this.lane = lane;
    }

    public String lane() {
        return lane;
    }

    public String leafId() {
        requireActive();
        return state.leaf(lane);
    }

    public SessionEntry entry(String id) {
        requireActive();
        return state.getEntry(Objects.requireNonNull(id, "id"));
    }

    public List<SessionEntry> findEntriesOnBranch(SessionBranchQuery query) {
        requireActive();
        return state.findBranch(
                lane, query == null ? SessionBranchQuery.current() : query
        );
    }

    public void moveLane(String to) {
        requireActive();
        state.moveLane(lane, to);
    }

    public SessionEntry append(SessionEntryDraft draft) {
        requireActive();
        return state.append(Objects.requireNonNull(draft, "draft"), lane);
    }

    public SessionEntry.Message appendMessage(AgentMessage message) {
        return (SessionEntry.Message) append(new SessionEntryDraft.Message(
                idGenerator.next(), message
        ));
    }

    public SessionEntry.Compaction appendCompaction(
            String summary,
            List<AgentMessage> retainedTail,
            long tokensBefore,
            JsonNode details,
            Usage usage
    ) {
        return (SessionEntry.Compaction) append(new SessionEntryDraft.Compaction(
                idGenerator.next(), summary, retainedTail, tokensBefore, details, usage
        ));
    }

    public SessionEntry.BranchSummary appendBranchSummary(
            String fromId,
            String summary,
            JsonNode details,
            Usage usage
    ) {
        return (SessionEntry.BranchSummary) append(new SessionEntryDraft.BranchSummary(
                idGenerator.next(), fromId, summary, details, usage
        ));
    }

    public SessionRecord appendRecord(SessionRecordDraft draft) {
        requireActive();
        return state.appendRecord(Objects.requireNonNull(draft, "draft"));
    }

    public List<SessionRecord> findRecords(SessionRecordQuery query) {
        requireActive();
        return state.findRecords(
                query == null ? SessionRecordQuery.all() : query
        );
    }

    public boolean operationOpen(String runId) {
        requireActive();
        return state.isOpenOperation(lane, Objects.requireNonNull(runId, "runId"));
    }

    public boolean abortRequested(String runId) {
        requireActive();
        return state.hasAbortRequest(lane, Objects.requireNonNull(runId, "runId"));
    }

    public void name(String value) {
        requireActive();
        state.name(value);
    }

    public void label(String entryId, String value) {
        requireActive();
        state.label(entryId, value);
    }

    void close() {
        active = false;
    }

    private void requireActive() {
        if (!active) {
            throw new IllegalStateException("Session transaction is no longer active");
        }
    }
}
