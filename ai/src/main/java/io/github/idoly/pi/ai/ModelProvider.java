package io.github.idoly.pi.ai;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Provider-level model discovery, authentication, filtering, and streaming SPI. */
public interface ModelProvider extends ModelStream {
    String id();

    default String name() {
        return id();
    }

    default boolean supports(Model model) {
        return model.provider().equals(id());
    }

    default CompletionStage<ProviderAuth> resolveAuth(ProviderContext context) {
        return CompletableFuture.completedFuture(context.auth());
    }

    default CompletionStage<List<Model>> models(ProviderContext context) {
        return CompletableFuture.completedFuture(List.of());
    }

    default CompletionStage<List<Model>> refreshModels(ProviderContext context) {
        return models(context);
    }

    default List<Model> filterModels(List<Model> models, ProviderContext context) {
        return List.copyOf(models);
    }
}
