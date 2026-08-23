package io.github.idoly.pi.vertx;

import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ProviderDefinition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ProviderModelCatalogTest {
    @Test
    void loadsEveryImplementedProviderWithPricing() {
        ProviderModelCatalog catalog = ProviderModelCatalog.bundled();
        assertEquals(276, catalog.models().size());
        Map<String, Long> counts = catalog.models().stream().collect(
                Collectors.groupingBy(Model::provider, Collectors.counting())
        );
        assertEquals(13, counts.get("anthropic"));
        assertEquals(22, counts.get("google"));
        assertEquals(13, counts.get("google-vertex"));
        assertEquals(114, counts.get("amazon-bedrock"));
        assertEquals(31, counts.get("mistral"));
        Model claude = catalog.models("anthropic").getFirst();
        assertTrue(claude.pricing().output() > 0);
        assertTrue(catalog.find(claude.provider(), claude.id()).isPresent());
    }

    @Test
    void unifiedRegistryExposesAllModelsAndFiveProtocolRouters() {
        try (VertxModelProviders providers = new VertxModelProviders()) {
            assertEquals(276, providers.models().size());
            assertEquals(5, providers.registry().providers().size());
            for (Model model : providers.models()) {
                assertTrue(providers.registry().providers().stream()
                        .anyMatch(provider -> provider.supports(model)),
                        model.provider() + '/' + model.id());
            }
            Model local = new Model(
                    "local", "Local", "openai-completions", "local-provider",
                    "http://localhost:11434/v1", false,
                    java.util.List.of("text"), 128_000, 4_096
            );
            providers.register(new ProviderDefinition(
                    "local-provider", "Local", local.baseUrl(), local.api(),
                    "key", java.util.Map.of(), true,
                    java.util.List.of(local), java.util.Map.of()
            ), expression -> java.util.concurrent.CompletableFuture
                    .completedFuture(expression));
            assertEquals(java.util.List.of(local), providers.registry()
                    .find("local-provider").orElseThrow()
                    .models(new io.github.idoly.pi.ai.ProviderContext(
                            null, null, null, null
                    )).toCompletableFuture().join());
            assertTrue(providers.unregister("local-provider") != null);
        }
    }
}
