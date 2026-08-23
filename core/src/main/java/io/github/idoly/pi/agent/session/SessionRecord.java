package io.github.idoly.pi.agent.session;

import java.util.Objects;

public final class SessionRecord {
    private final long sequence;
    private final long timestamp;
    private final SessionRecordDraft draft;

    SessionRecord(long sequence, long timestamp, SessionRecordDraft draft) {
        if (sequence <= 0) throw new IllegalArgumentException("sequence must be positive");
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.draft = SessionRecords.copy(draft);
    }

    public long sequence() { return sequence; }

    public long timestamp() { return timestamp; }

    public String id() { return draft.id(); }

    public String lane() { return draft.lane(); }

    public SessionRecordDraft.Type type() { return draft.type(); }

    public SessionRecordDraft value() { return SessionRecords.copy(draft); }

    SessionRecord copy() { return new SessionRecord(sequence, timestamp, draft); }

    SessionRecordDraft storedValue() { return draft; }

    @Override
    public boolean equals(Object other) {
        return other instanceof SessionRecord record
                && sequence == record.sequence
                && timestamp == record.timestamp
                && draft.equals(record.draft);
    }

    @Override
    public int hashCode() { return Objects.hash(sequence, timestamp, draft); }

    @Override
    public String toString() {
        return "SessionRecord[sequence=" + sequence + ", timestamp=" + timestamp
                + ", value=" + draft + "]";
    }
}
