package io.github.idoly.pi.vertx.anthropic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.internal.ProviderHeaders;
import io.github.idoly.pi.vertx.internal.ProviderHttpHooks;
import io.github.idoly.pi.vertx.SseHttpRequest;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.smallrye.mutiny.Multi;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

/** Anthropic Messages API over the shared Vert.x SSE transport. */
public final class AnthropicMessagesModelStream
        implements ModelProvider, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final ObjectMapper mapper;
    private final AnthropicMessagesCodec codec;
    private final boolean ownsTransport;

    public AnthropicMessagesModelStream() {
        this(new VertxSseHttpClient(), new ObjectMapper(), true);
    }

    public AnthropicMessagesModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper
    ) {
        this(transport, mapper, false);
    }

    private AnthropicMessagesModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.codec = new AnthropicMessagesCodec(mapper);
        this.ownsTransport = ownsTransport;
    }

    @Override
    public String id() {
        return "anthropic-messages";
    }

    @Override
    public boolean supports(Model model) {
        return model.api().equals("anthropic-messages");
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        Objects.requireNonNull(model, "model");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(options, "options");
        if (!supports(model)) {
            return Multi.createFrom().failure(new IllegalArgumentException(
                    "Unsupported Anthropic API " + model.api()
            ));
        }
        if (options.apiKey() == null || options.apiKey().isBlank()) {
            return Multi.createFrom().failure(new IllegalArgumentException(
                    "No API key for provider: " + model.provider()
            ));
        }
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("content-type", "application/json");
        headers.put("accept", "text/event-stream");
        String authToken = System.getenv("ANTHROPIC_AUTH_TOKEN");
        String oauthToken = System.getenv("ANTHROPIC_OAUTH_TOKEN");
        if (options.apiKey().equals(authToken)
                || options.apiKey().equals(oauthToken)) {
            headers.put("authorization", "Bearer " + options.apiKey());
        } else {
            headers.put("x-api-key", options.apiKey());
        }
        headers.put("anthropic-version", "2023-06-01");
        if (options.thinkingLevel() != null
                && !options.thinkingLevel().equals("off")) {
            headers.put(
                    "anthropic-beta", "interleaved-thinking-2025-05-14"
            );
        }
        headers = ProviderHeaders.merge(headers, options.headers());
        return ProviderHttpHooks.prepare(
                mapper, model,
                codec.encodeRequest(
                        model, context, options.thinkingLevel(),
                        options.cacheRetention()
                ),
                headers, options
        ).toMulti().onItem().transformToMultiAndConcatenate(prepared -> {
            byte[] body;
            try {
                body = mapper.writeValueAsBytes(prepared.payload());
            } catch (JsonProcessingException failure) {
                return Multi.createFrom().failure(failure);
            }
            return ProviderHttpHooks.observeSse(transport.execute(
                    SseHttpRequest.post(
                            messagesUri(model.baseUrl()), prepared.headers(), body
                    ),
                    options.cancellation()
            ), model, options).toMulti()
                    .onItem().transformToMultiAndConcatenate(response ->
                            codec.decode(response.events(), model)
                    );
        });
    }

    static URI messagesUri(String baseUrl) {
        String normalized = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        if (normalized.endsWith("/v1/messages")
                || normalized.endsWith("/messages")) {
            return URI.create(normalized);
        }
        return URI.create(normalized + "/v1/messages");
    }

    @Override
    public void close() {
        if (ownsTransport) transport.close();
    }
}
