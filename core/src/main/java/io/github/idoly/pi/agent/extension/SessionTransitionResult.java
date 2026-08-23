package io.github.idoly.pi.agent.extension;

/** Extension arbitration result for a host session transition. */
public record SessionTransitionResult(boolean cancel, String reason) {
    public static SessionTransitionResult allow() {
        return new SessionTransitionResult(false, null);
    }

    public static SessionTransitionResult cancel(String reason) {
        return new SessionTransitionResult(true, reason);
    }
}
