package io.github.idoly.pi.ai;

import java.util.Map;

/** Resolved authentication and request metadata for one provider invocation. */
public record ProviderAuth(
        String apiKey,
        Map<String, String> headers,
        String source
) {
    public static final ProviderAuth NONE = new ProviderAuth(null, Map.of(), null);

    public ProviderAuth {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
