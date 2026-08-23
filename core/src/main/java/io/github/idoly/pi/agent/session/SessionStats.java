package io.github.idoly.pi.agent.session;

public record SessionStats(
        long messageCount,
        long cachedTokens,
        long uncachedTokens,
        long totalTokens,
        double costTotal
) {
    public static final SessionStats ZERO = new SessionStats(0, 0, 0, 0, 0);
}
