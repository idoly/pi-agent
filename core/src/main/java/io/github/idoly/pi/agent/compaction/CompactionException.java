package io.github.idoly.pi.agent.compaction;

public final class CompactionException extends RuntimeException {
    private final Code code;

    public CompactionException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code code() { return code; }

    public enum Code { ABORTED, SUMMARIZATION_FAILED }
}
