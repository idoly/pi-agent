package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Objects;

public record Model(
        String id,
        String name,
        String api,
        String provider,
        String baseUrl,
        boolean reasoning,
        List<String> input,
        int contextWindow,
        int maxTokens,
        ThinkingLevelMap thinkingLevelMap
) {
    public Model(
            String id,
            String name,
            String api,
            String provider,
            String baseUrl,
            boolean reasoning,
            List<String> input,
            int contextWindow,
            int maxTokens
    ) {
        this(
                id, name, api, provider, baseUrl, reasoning, input,
                contextWindow, maxTokens, null
        );
    }

    public Model {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(api, "api");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(baseUrl, "baseUrl");
        input = List.copyOf(Objects.requireNonNull(input, "input"));
    }
}
