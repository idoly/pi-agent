package io.github.idoly.pi.vertx.internal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Case-insensitive layered merge for provider HTTP headers. */
public final class ProviderHeaders {
    private ProviderHeaders() {
    }

    @SafeVarargs
    public static LinkedHashMap<String, String> merge(
            Map<String, String>... layers
    ) {
        LinkedHashMap<String, String> merged = new LinkedHashMap<>();
        for (Map<String, String> layer : layers) {
            Objects.requireNonNull(layer, "header layer").forEach(
                    (name, value) -> put(merged, name, value)
            );
        }
        return merged;
    }

    private static void put(
            Map<String, String> headers,
            String name,
            String value
    ) {
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(value, "value");
        headers.keySet().removeIf(existing -> existing.equalsIgnoreCase(name));
        headers.put(name, value);
    }
}
