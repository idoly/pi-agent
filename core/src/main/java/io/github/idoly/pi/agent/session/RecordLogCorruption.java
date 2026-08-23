package io.github.idoly.pi.agent.session;

import java.util.Objects;

public final class RecordLogCorruption extends RuntimeException {
    private final Reason reason;

    public RecordLogCorruption(Reason reason, String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public Reason reason() { return reason; }

    public enum Reason {
        MULTIPLE_OPEN_OPERATIONS,
        UNKNOWN_OPERATION,
        RECORD_AFTER_FINISH,
        NON_CONSECUTIVE_ATTEMPT,
        INVALID_COMPACTION_REASON,
        QUEUE_AFTER_ABORT,
        INVALID_QUEUE_CANCELLATION,
        INCONSISTENT_STEP,
        TOOL_CALL_MISMATCH,
        DUPLICATE_TOOL_INVOCATION,
        PROVISIONED_ENTRY_MISMATCH
    }
}
