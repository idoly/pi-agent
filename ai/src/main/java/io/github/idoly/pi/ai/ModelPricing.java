package io.github.idoly.pi.ai;

import java.util.Comparator;
import java.util.List;

/** Per-million-token model prices with optional whole-request input tiers. */
public record ModelPricing(
        double input,
        double output,
        double cacheRead,
        double cacheWrite,
        List<Tier> tiers
) {
    public static final ModelPricing ZERO = new ModelPricing(
            0, 0, 0, 0, List.of()
    );

    public ModelPricing(
            double input,
            double output,
            double cacheRead,
            double cacheWrite
    ) {
        this(input, output, cacheRead, cacheWrite, List.of());
    }

    public ModelPricing {
        requireNonNegative(input, "input");
        requireNonNegative(output, "output");
        requireNonNegative(cacheRead, "cacheRead");
        requireNonNegative(cacheWrite, "cacheWrite");
        tiers = tiers == null ? List.of() : tiers.stream()
                .sorted(Comparator.comparingLong(Tier::inputTokensAbove))
                .toList();
    }

    public ModelPricing forInputTokens(long tokens) {
        ModelPricing selected = this;
        for (Tier tier : tiers) {
            if (tokens > tier.inputTokensAbove()) {
                selected = new ModelPricing(
                        tier.input(), tier.output(), tier.cacheRead(),
                        tier.cacheWrite(), List.of()
                );
            }
        }
        return selected;
    }

    private static void requireNonNegative(double value, String name) {
        if (!Double.isFinite(value) || value < 0) {
            throw new IllegalArgumentException(name + " price must be non-negative");
        }
    }

    public record Tier(
            long inputTokensAbove,
            double input,
            double output,
            double cacheRead,
            double cacheWrite
    ) {
        public Tier {
            if (inputTokensAbove < 0) {
                throw new IllegalArgumentException(
                        "inputTokensAbove must be non-negative"
                );
            }
            requireNonNegative(input, "input");
            requireNonNegative(output, "output");
            requireNonNegative(cacheRead, "cacheRead");
            requireNonNegative(cacheWrite, "cacheWrite");
        }
    }
}
