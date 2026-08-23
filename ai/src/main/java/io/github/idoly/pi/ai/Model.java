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
        ThinkingLevelMap thinkingLevelMap,
        ModelPricing pricing
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
                contextWindow, maxTokens, null, ModelPricing.ZERO
        );
    }

    public Model(
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
        this(
                id, name, api, provider, baseUrl, reasoning, input,
                contextWindow, maxTokens, thinkingLevelMap, ModelPricing.ZERO
        );
    }

    public Model {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(api, "api");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(baseUrl, "baseUrl");
        input = List.copyOf(Objects.requireNonNull(input, "input"));
        pricing = pricing == null ? ModelPricing.ZERO : pricing;
    }
}
