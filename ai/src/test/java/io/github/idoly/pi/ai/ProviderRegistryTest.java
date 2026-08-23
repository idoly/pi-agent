package io.github.idoly.pi.ai;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProviderRegistryTest {
    @Test
    void calculatesBaseAndTieredModelCosts() {
        Model model = new Model(
                "model", "Model", "test", "provider", "https://example.test",
                false, List.of("text"), 1_000_000, 100_000, null,
                new ModelPricing(
                        2, 4, 0.2, 1,
                        List.of(new ModelPricing.Tier(
                                100, 3, 6, 0.3, 1.5
                        ))
                )
        );
        Usage base = UsageCosts.calculate(model, new Usage(
                50, 10, 20, 5, 85, Cost.ZERO
        ));
        assertEquals(0.0001, base.cost().input(), 0.0000001);
        Usage tier = UsageCosts.calculate(model, new Usage(
                90, 10, 20, 5, 125, Cost.ZERO
        ));
        assertEquals(0.00027, tier.cost().input(), 0.0000001);
        assertEquals(0.00006, tier.cost().output(), 0.0000001);
    }

    @Test
    void routesDiscoveryStreamingReplacementAndReverseClose() {
        ProviderRegistry registry = new ProviderRegistry();
        AtomicInteger closes = new AtomicInteger();
        Model model = model("first");
        TestProvider first = new TestProvider("first", model, closes);
        TestProvider second = new TestProvider("second", model("second"), closes);
        registry.register(first);
        registry.register(second);

        assertEquals(List.of("first", "second"), registry.providers().stream()
                .map(ModelProvider::id).toList());
        assertEquals(List.of("first", "second"), registry.models(context()).stream()
                .map(Model::provider).toList());
        assertEquals(first.publisher, registry.stream(
                model, new ModelContext("", List.of(), List.of()),
                new StreamOptions(null, null, "off", CancellationSignal.NONE)
        ));

        TestProvider replacement = new TestProvider("first", model, closes);
        registry.register(replacement);
        assertEquals(replacement, registry.find("first").orElseThrow());
        assertEquals(second, registry.unregister("second"));
        assertThrows(IllegalArgumentException.class, () -> registry.stream(
                model("missing"), new ModelContext("", List.of(), List.of()),
                new StreamOptions(null, null, "off", CancellationSignal.NONE)
        ));
        registry.close();
        assertEquals(1, closes.get());
    }

    private static ProviderContext context() {
        return new ProviderContext(
                ProviderAuth.NONE, null, CancellationSignal.NONE, null
        );
    }

    private static Model model(String provider) {
        return new Model(
                "model", "Model", "test", provider, "https://example.test",
                false, List.of("text"), 1000, 100
        );
    }

    private static final class TestProvider
            implements ModelProvider, AutoCloseable {
        private final String id;
        private final Model model;
        private final AtomicInteger closes;
        private final Flow.Publisher<AssistantStreamEvent> publisher =
                subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long n) { }
                    @Override public void cancel() { }
                });

        private TestProvider(String id, Model model, AtomicInteger closes) {
            this.id = id;
            this.model = model;
            this.closes = closes;
        }

        @Override public String id() { return id; }
        @Override public java.util.concurrent.CompletionStage<List<Model>> models(
                ProviderContext context
        ) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    List.of(model)
            );
        }
        @Override public Flow.Publisher<AssistantStreamEvent> stream(
                Model model, ModelContext context, StreamOptions options
        ) { return publisher; }
        @Override public void close() { closes.incrementAndGet(); }
    }
}
