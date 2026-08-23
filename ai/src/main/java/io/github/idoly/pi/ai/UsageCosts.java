package io.github.idoly.pi.ai;

/** Provider-neutral model cost calculation. */
public final class UsageCosts {
    private UsageCosts() {
    }

    public static Usage calculate(Model model, Usage usage) {
        ModelPricing pricing = model.pricing().forInputTokens(
                usage.input() + usage.cacheRead() + usage.cacheWrite()
        );
        double input = usage.input() * pricing.input() / 1_000_000d;
        double output = usage.output() * pricing.output() / 1_000_000d;
        double cacheRead = usage.cacheRead() * pricing.cacheRead() / 1_000_000d;
        double cacheWrite = usage.cacheWrite() * pricing.cacheWrite() / 1_000_000d;
        return new Usage(
                usage.input(), usage.output(), usage.cacheRead(),
                usage.cacheWrite(), usage.reasoning(), usage.totalTokens(),
                new Cost(
                        input, output, cacheRead, cacheWrite,
                        input + output + cacheRead + cacheWrite
                )
        );
    }
}
