package io.github.idoly.pi.ai;

public record StreamOptions(
        String sessionId,
        String apiKey,
        String thinkingLevel,
        CancellationSignal cancellation,
        java.util.Map<String, String> headers,
        ProviderRequestHooks requestHooks
) {
    public StreamOptions(
            String sessionId,
            String apiKey,
            String thinkingLevel,
            CancellationSignal cancellation,
            java.util.Map<String, String> headers
    ) {
        this(
                sessionId, apiKey, thinkingLevel, cancellation, headers,
                ProviderRequestHooks.NONE
        );
    }

    public StreamOptions(
            String sessionId,
            String apiKey,
            String thinkingLevel,
            CancellationSignal cancellation
    ) {
        this(
                sessionId, apiKey, thinkingLevel, cancellation,
                java.util.Map.of(), ProviderRequestHooks.NONE
        );
    }

    public StreamOptions {
        cancellation = cancellation == null
                ? CancellationSignal.NONE : cancellation;
        headers = headers == null ? java.util.Map.of() : java.util.Map.copyOf(headers);
        requestHooks = requestHooks == null
                ? ProviderRequestHooks.NONE : requestHooks;
    }

    public StreamOptions withRequestHooks(ProviderRequestHooks hooks) {
        return new StreamOptions(
                sessionId, apiKey, thinkingLevel, cancellation, headers, hooks
        );
    }
}
