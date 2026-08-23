package io.github.idoly.pi.ai;

import java.util.Objects;

public record Usage(
        long input,
        long output,
        long cacheRead,
        long cacheWrite,
        long reasoning,
        long totalTokens,
        Cost cost
) {
    public static final Usage ZERO = new Usage(0, 0, 0, 0, 0, 0, Cost.ZERO);

    public Usage(long input, long output, long cacheRead, long cacheWrite, long totalTokens, Cost cost) {
        this(input, output, cacheRead, cacheWrite, 0, totalTokens, cost);
    }

    public Usage {
        Objects.requireNonNull(cost, "cost");
    }
}
