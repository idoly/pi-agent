package io.github.idoly.pi.agent.session;

import java.util.Objects;

public final class SessionError extends RuntimeException {
    private final Code code;

    public SessionError(Code code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public Code code() {
        return code;
    }

    public enum Code {
        NOT_FOUND,
        ALREADY_EXISTS,
        INVALID_ENTRY,
        INVALID_PAYLOAD,
        INVALID_LANE,
        INVALID_QUERY,
        INVALID_FORK_TARGET,
        CLOSED,
        STORAGE
    }
}
