package io.github.idoly.pi.vertx;

import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.ProviderDefinition;
import io.github.idoly.pi.ai.ProviderRequestHooks;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.UserMessage;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
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
    void containsDefaultLiveSmokeModels() {
        ProviderModelCatalog catalog = ProviderModelCatalog.bundled();
        Map.of(
                "openai", "gpt-4.1-mini",
                "anthropic", "claude-haiku-4-5",
                "google", "gemini-2.5-flash-lite",
                "google-vertex", "gemini-2.5-flash-lite",
                "mistral", "ministral-3b-latest",
                "amazon-bedrock", "amazon.nova-micro-v1:0"
        ).forEach((provider, model) -> assertTrue(
                catalog.find(provider, model).isPresent(),
                provider + '/' + model
        ));
    }

    @Test
    void configuredProvidersPreservePerRequestHeadersAndHooks() {
        AtomicBoolean called = new AtomicBoolean();
        java.util.concurrent.atomic.AtomicReference<Map<String, String>> seen =
                new java.util.concurrent.atomic.AtomicReference<>();
        ProviderRequestHooks hooks = new ProviderRequestHooks() {
            @Override
            public java.util.concurrent.CompletionStage<Map<String, String>>
            beforeHeaders(
                    Model model, Map<String, String> headers,
                    CancellationSignal cancellation
            ) {
                called.set(true);
                seen.set(headers);
                return CompletableFuture.failedFuture(
                        new IllegalStateException("hook-marker")
                );
            }
        };
        Model local = new Model(
                "local", "Local", "openai-completions", "local-provider",
                "http://127.0.0.1:1/v1", false, List.of("text"),
                8_192, 1_024
        );
        try (VertxModelProviders providers = new VertxModelProviders()) {
            providers.register(new ProviderDefinition(
                    "local-provider", "Local", local.baseUrl(), local.api(),
                    "key", Map.of("x-config", "configured"),
                    true, List.of(local), Map.of()
            ), expression -> CompletableFuture.completedFuture(expression));
            RuntimeException failure = assertThrows(RuntimeException.class, () ->
                    Multi.createFrom().publisher(providers.stream(
                            local,
                            new ModelContext("", List.of(
                                    UserMessage.text("hello", 1)
                            )),
                            new StreamOptions(
                                    null, null, "off", CancellationSignal.NONE,
                                    Map.of("x-request", "request"), hooks
                            )
                    )).collect().asList().await()
                            .atMost(Duration.ofSeconds(3))
            );
            assertTrue(called.get());
            assertEquals("configured", seen.get().get("x-config"));
            assertEquals("request", seen.get().get("x-request"));
            assertEquals("hook-marker", rootCause(failure).getMessage());
        }
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

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current;
    }
}
