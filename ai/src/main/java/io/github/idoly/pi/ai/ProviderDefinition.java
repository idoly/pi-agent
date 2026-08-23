package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Parsed headless provider configuration independent of an HTTP transport. */
public record ProviderDefinition(
        String id,
        String name,
        String baseUrl,
        String api,
        String apiKey,
        Map<String, String> headers,
        boolean authHeader,
        List<Model> models,
        Map<String, Object> compatibility
) {
    public ProviderDefinition {
        Objects.requireNonNull(id, "id");
        name = name == null ? id : name;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        models = models == null ? List.of() : List.copyOf(models);
        compatibility = compatibility == null
                ? Map.of() : Map.copyOf(compatibility);
    }
}
