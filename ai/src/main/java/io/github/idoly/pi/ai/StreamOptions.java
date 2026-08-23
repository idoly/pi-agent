package io.github.idoly.pi.ai;

public record StreamOptions(
        String sessionId,
        String apiKey,
        String thinkingLevel,
        CancellationSignal cancellation,
        java.util.Map<String, String> headers
) {
    public StreamOptions(
            String sessionId,
            String apiKey,
            String thinkingLevel,
            CancellationSignal cancellation
    ) {
        this(sessionId, apiKey, thinkingLevel, cancellation, java.util.Map.of());
    }

    public StreamOptions {
        cancellation = cancellation == null
                ? CancellationSignal.NONE : cancellation;
        headers = headers == null ? java.util.Map.of() : java.util.Map.copyOf(headers);
    }
}
