package io.github.idoly.pi.ai;

public record StreamOptions(
        String sessionId,
        String apiKey,
        String thinkingLevel,
        CancellationSignal cancellation
) {
}
