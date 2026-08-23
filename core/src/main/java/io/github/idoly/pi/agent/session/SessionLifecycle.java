package io.github.idoly.pi.agent.session;

import java.util.concurrent.atomic.AtomicBoolean;

final class SessionLifecycle {
    private final AtomicBoolean closed = new AtomicBoolean();

    boolean close() {
        return closed.compareAndSet(false, true);
    }

    boolean isClosed() {
        return closed.get();
    }

    void requireOpen() {
        if (closed.get()) {
            throw new SessionError(
                    SessionError.Code.CLOSED,
                    "Session is closed"
            );
        }
    }
}
