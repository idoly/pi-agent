package io.github.idoly.pi.agent.harness;

public record CompactionSettings(boolean enabled, long reserveTokens, long keepRecentTokens) {
    public static final CompactionSettings DEFAULT = new CompactionSettings(
            true, 16_384, 20_000
    );

    public CompactionSettings {
        if (reserveTokens < 0) throw new IllegalArgumentException("reserveTokens must be non-negative");
        if (keepRecentTokens < 0) throw new IllegalArgumentException("keepRecentTokens must be non-negative");
    }
}
