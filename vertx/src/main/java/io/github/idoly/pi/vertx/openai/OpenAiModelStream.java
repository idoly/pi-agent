package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.vertx.VertxSseHttpClient;

import java.util.Objects;
import java.util.concurrent.Flow;

/** Routes OpenAI-family model APIs over one shared Vert.x transport and connection pool. */
public final class OpenAiModelStream implements ModelStream, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final ObjectMapper mapper;
    private final OpenAiModelCatalog catalog;
    private final OpenAiCompatibility completionsOverride;
    private final OpenAiResponsesCompatibility responsesOverride;
    private final boolean ownsTransport;

    public OpenAiModelStream() {
        this(
                new VertxSseHttpClient(), new ObjectMapper(),
                OpenAiModelCatalog.bundled(), null, null, true
        );
    }

    public OpenAiModelStream(VertxSseHttpClient transport, ObjectMapper mapper) {
        this(transport, mapper, OpenAiModelCatalog.bundled(), null, null, false);
    }

    public OpenAiModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiModelCatalog catalog
    ) {
        this(transport, mapper, catalog, null, null, false);
    }

    /** Uses explicit profiles for every model, bypassing catalog compatibility metadata. */
    public OpenAiModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiCompatibility completionsCompatibility,
            OpenAiResponsesCompatibility responsesCompatibility
    ) {
        this(
                transport, mapper, OpenAiModelCatalog.bundled(),
                Objects.requireNonNull(completionsCompatibility, "completionsCompatibility"),
                Objects.requireNonNull(responsesCompatibility, "responsesCompatibility"),
                false
        );
    }

    private OpenAiModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiModelCatalog catalog,
            OpenAiCompatibility completionsOverride,
            OpenAiResponsesCompatibility responsesOverride,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.completionsOverride = completionsOverride;
        this.responsesOverride = responsesOverride;
        this.ownsTransport = ownsTransport;
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        Objects.requireNonNull(model, "model");
        return switch (model.api()) {
            case "openai-completions" -> new OpenAiCompatibleModelStream(
                    transport, mapper, completionsCompatibility(model)
            ).stream(model, context, options);
            case "openai-responses", "azure-openai-responses", "openai-codex-responses" ->
                    new OpenAiResponsesModelStream(
                            transport, mapper, responsesCompatibility(model)
                    ).stream(model, context, options);
            default -> Multi.createFrom().failure(new IllegalArgumentException(
                    "Unsupported OpenAI-family model API: " + model.api()
            ));
        };
    }

    OpenAiCompatibility completionsCompatibility(Model model) {
        if (completionsOverride != null) {
            return completionsOverride;
        }
        return catalog.find(model)
                .map(OpenAiModelCatalog.Entry::completionsCompatibility)
                .orElse(OpenAiCompatibility.DEFAULT);
    }

    OpenAiResponsesCompatibility responsesCompatibility(Model model) {
        if (responsesOverride != null) {
            return responsesOverride;
        }
        return catalog.find(model)
                .map(OpenAiModelCatalog.Entry::responsesCompatibility)
                .orElse(OpenAiResponsesCompatibility.DEFAULT);
    }

    @Override
    public void close() {
        if (ownsTransport) {
            transport.close();
        }
    }
}
