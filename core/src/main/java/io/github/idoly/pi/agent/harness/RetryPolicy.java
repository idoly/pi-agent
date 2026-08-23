package io.github.idoly.pi.agent.harness;

public record RetryPolicy(boolean enabled, int maxRetries, long baseDelayMillis) {
    public static final RetryPolicy DISABLED = new RetryPolicy(false, 0, 1_000);

    public RetryPolicy {
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be non-negative");
        if (baseDelayMillis < 0) throw new IllegalArgumentException("baseDelayMillis must be non-negative");
    }
}
