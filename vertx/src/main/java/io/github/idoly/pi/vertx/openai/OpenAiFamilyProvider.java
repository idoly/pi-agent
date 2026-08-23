package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.ProviderModelCatalog;
import io.github.idoly.pi.vertx.VertxSseHttpClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** ModelProvider adapter for all supported OpenAI-family API identifiers. */
public final class OpenAiFamilyProvider implements ModelProvider {
    private final OpenAiModelStream stream;
    private final ProviderModelCatalog catalog;

    public OpenAiFamilyProvider(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            ProviderModelCatalog catalog
    ) {
        this.stream = new OpenAiModelStream(
                transport, mapper, OpenAiModelCatalog.bundled()
        );
        this.catalog = catalog;
    }

    @Override
    public String id() {
        return "openai-family";
    }

    @Override
    public boolean supports(Model model) {
        return switch (model.api()) {
            case "openai-completions", "openai-responses",
                 "azure-openai-responses", "openai-codex-responses" -> true;
            default -> false;
        };
    }

    @Override
    public CompletionStage<List<Model>> models(ProviderContext context) {
        return CompletableFuture.completedFuture(catalog.models().stream()
                .filter(this::supports).toList());
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        return stream.stream(model, context, options);
    }
}
