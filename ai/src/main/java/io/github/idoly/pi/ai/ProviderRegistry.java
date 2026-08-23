package io.github.idoly.pi.ai;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Thread-safe ordered registry and ModelStream router for native Java providers. */
public final class ProviderRegistry implements ModelStream, AutoCloseable {
    private final Object lock = new Object();
    private final LinkedHashMap<String, ModelProvider> providers =
            new LinkedHashMap<>();

    public void register(ModelProvider provider) {
        Objects.requireNonNull(provider, "provider");
        if (provider.id().isBlank()) {
            throw new IllegalArgumentException("provider id must not be blank");
        }
        synchronized (lock) {
            providers.put(provider.id(), provider);
        }
    }

    public ModelProvider unregister(String id) {
        Objects.requireNonNull(id, "id");
        synchronized (lock) {
            return providers.remove(id);
        }
    }

    public Optional<ModelProvider> find(String id) {
        Objects.requireNonNull(id, "id");
        synchronized (lock) {
            return Optional.ofNullable(providers.get(id));
        }
    }

    public List<ModelProvider> providers() {
        synchronized (lock) {
            return List.copyOf(providers.values());
        }
    }

    public Map<String, ModelProvider> byId() {
        synchronized (lock) {
            return Map.copyOf(providers);
        }
    }

    public CompletionStage<List<Model>> discoverModels(
            ProviderContext context
    ) {
        return collectModels(context, false);
    }

    public CompletionStage<List<Model>> refreshModels(
            ProviderContext context
    ) {
        return collectModels(context, true);
    }

    /** Blocking convenience for CLI and worker threads. */
    public List<Model> models(ProviderContext context) {
        return discoverModels(context).toCompletableFuture().join();
    }

    private CompletionStage<List<Model>> collectModels(
            ProviderContext context,
            boolean refresh
    ) {
        CompletionStage<List<Model>> stage =
                CompletableFuture.completedFuture(List.of());
        for (ModelProvider provider : providers()) {
            stage = stage.thenCombine(
                    refresh ? provider.refreshModels(context)
                            : provider.models(context),
                    (current, discovered) -> {
                        ArrayList<Model> combined = new ArrayList<>(current);
                        combined.addAll(provider.filterModels(
                                discovered, context
                        ));
                        return List.copyOf(combined);
                    }
            );
        }
        return stage;
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        Objects.requireNonNull(model, "model");
        ModelProvider provider = find(model.provider()).orElseGet(() ->
                providers().stream().filter(value -> value.supports(model))
                        .findFirst().orElseThrow(() ->
                                new IllegalArgumentException(
                                        "No provider registered for "
                                                + model.provider() + '/'
                                                + model.api()
                                )
                        )
        );
        return provider.stream(model, context, options);
    }

    @Override
    public void close() {
        List<ModelProvider> snapshot;
        synchronized (lock) {
            snapshot = List.copyOf(providers.values());
            providers.clear();
        }
        RuntimeException failure = null;
        for (ModelProvider provider : snapshot.reversed()) {
            if (!(provider instanceof AutoCloseable closeable)) continue;
            try {
                closeable.close();
            } catch (Exception error) {
                if (failure == null) {
                    failure = new IllegalStateException(
                            "Failed to close provider " + provider.id(), error
                    );
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure != null) throw failure;
    }
}
