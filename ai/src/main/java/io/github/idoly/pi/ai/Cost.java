package io.github.idoly.pi.ai;

public record Cost(double input, double output, double cacheRead, double cacheWrite, double total) {
    public static final Cost ZERO = new Cost(0, 0, 0, 0, 0);
}
