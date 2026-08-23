package io.github.idoly.pi.vertx;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.anthropic.AnthropicMessagesModelStream;
import io.github.idoly.pi.vertx.bedrock.AwsCredentials;
import io.github.idoly.pi.vertx.bedrock.AwsCredentialsProvider;
import io.github.idoly.pi.vertx.bedrock.BedrockConverseModelStream;
import io.github.idoly.pi.vertx.google.GoogleGenerativeModelStream;
import io.github.idoly.pi.vertx.mistral.MistralConversationsModelStream;
import io.github.idoly.pi.vertx.openai.OpenAiFamilyProvider;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Shared-transport registry for every protocol implemented by pi-agent-vertx. */
public final class VertxModelProviders implements ModelStream, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final ProviderRegistry registry;
    private final ProviderModelCatalog catalog;
    private final List<ModelProvider> protocols;
    private final boolean ownsTransport;

    public VertxModelProviders() {
        this(
                new VertxSseHttpClient(), new ObjectMapper(),
                ProviderModelCatalog.bundled(), AwsCredentials::fromEnvironment,
                true
        );
    }

    public VertxModelProviders(
            VertxSseHttpClient transport,
            ObjectMapper mapper
    ) {
        this(
                transport, mapper, ProviderModelCatalog.bundled(),
                AwsCredentials::fromEnvironment, false
        );
    }

    public VertxModelProviders(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            ProviderModelCatalog catalog,
            AwsCredentialsProvider credentials
    ) {
        this(transport, mapper, catalog, credentials, false);
    }

    private VertxModelProviders(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            ProviderModelCatalog catalog,
            AwsCredentialsProvider credentials,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(mapper, "mapper");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.ownsTransport = ownsTransport;
        this.registry = new ProviderRegistry();
        this.protocols = List.of(
                new OpenAiFamilyProvider(transport, mapper, catalog),
                new AnthropicMessagesModelStream(transport, mapper),
                new GoogleGenerativeModelStream(transport, mapper),
                new MistralConversationsModelStream(transport, mapper),
                new BedrockConverseModelStream(transport, mapper, credentials)
        );
        protocols.forEach(provider -> registry.register(
                catalogProvider(provider, catalog)
        ));
    }

    public ProviderRegistry registry() {
        return registry;
    }

    public ProviderModelCatalog catalog() {
        return catalog;
    }

    public List<Model> models() {
        return catalog.models();
    }

    public ModelProvider register(
            ProviderDefinition definition,
            ConfigValueResolver resolver
    ) {
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(resolver, "resolver");
        ModelProvider configured = configuredProvider(definition, resolver);
        registry.register(configured);
        return configured;
    }

    public ModelProvider unregister(String providerId) {
        return registry.unregister(providerId);
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        return registry.stream(model, context, options);
    }

    @Override
    public void close() {
        registry.close();
        if (ownsTransport) transport.close();
    }

    private ModelProvider configuredProvider(
            ProviderDefinition definition,
            ConfigValueResolver resolver
    ) {
        return new ModelProvider() {
            @Override public String id() { return definition.id(); }
            @Override public String name() { return definition.name(); }
            @Override public boolean supports(Model model) {
                return model.provider().equals(definition.id());
            }
            @Override public CompletionStage<List<Model>> models(
                    ProviderContext context
            ) {
                return CompletableFuture.completedFuture(definition.models());
            }
            @Override public Flow.Publisher<AssistantStreamEvent> stream(
                    Model model, ModelContext context, StreamOptions options
            ) {
                ModelProvider protocol = protocols.stream()
                        .filter(value -> value.supports(model))
                        .findFirst().orElseThrow(() ->
                                new IllegalArgumentException(
                                        "No protocol implementation for " + model.api()
                                )
                        );
                CompletionStage<String> key = options.apiKey() != null
                        && !options.apiKey().isBlank()
                        ? CompletableFuture.completedFuture(options.apiKey())
                        : resolver.resolve(definition.apiKey());
                CompletionStage<Map<String, String>> headers =
                        resolveHeaders(definition.headers(), resolver);
                return io.smallrye.mutiny.Uni.createFrom()
                        .completionStage(key.thenCombine(headers, (apiKey, resolved) ->
                                new StreamOptions(
                                        options.sessionId(), apiKey,
                                        options.thinkingLevel(),
                                        options.cancellation(), resolved
                                )
                        )).toMulti().onItem().transformToMultiAndConcatenate(
                                effective -> io.smallrye.mutiny.Multi.createFrom()
                                        .publisher(protocol.stream(
                                                model, context, effective
                                        ))
                        );
            }
        };
    }

    private static CompletionStage<Map<String, String>> resolveHeaders(
            Map<String, String> configured,
            ConfigValueResolver resolver
    ) {
        CompletionStage<Map<String, String>> stage =
                CompletableFuture.completedFuture(Map.of());
        for (Map.Entry<String, String> entry : configured.entrySet()) {
            stage = stage.thenCombine(
                    resolver.resolve(entry.getValue()),
                    (current, value) -> {
                        LinkedHashMap<String, String> result =
                                new LinkedHashMap<>(current);
                        if (value != null) result.put(entry.getKey(), value);
                        return Map.copyOf(result);
                    }
            );
        }
        return stage;
    }

    private static ModelProvider catalogProvider(
            ModelProvider delegate,
            ProviderModelCatalog catalog
    ) {
        return new ModelProvider() {
            @Override public String id() { return delegate.id(); }
            @Override public String name() { return delegate.name(); }
            @Override public boolean supports(Model model) {
                return delegate.supports(model);
            }
            @Override public CompletionStage<ProviderAuth> resolveAuth(
                    ProviderContext context
            ) { return delegate.resolveAuth(context); }
            @Override public CompletionStage<List<Model>> models(
                    ProviderContext context
            ) {
                return CompletableFuture.completedFuture(catalog.models().stream()
                        .filter(delegate::supports).toList());
            }
            @Override public Flow.Publisher<AssistantStreamEvent> stream(
                    Model model, ModelContext context, StreamOptions options
            ) { return delegate.stream(model, context, options); }
        };
    }
}
