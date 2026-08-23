package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.vertx.SseHttpRequest;
import io.github.idoly.pi.vertx.VertxSseHttpClient;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

/** OpenAI-compatible Chat Completions implementation of the runtime-neutral ModelStream API. */
public final class OpenAiCompatibleModelStream implements ModelStream, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final OpenAiChatCodec codec;
    private final OpenAiCompatibility compatibility;
    private final ObjectMapper mapper;
    private final boolean ownsTransport;

    public OpenAiCompatibleModelStream() {
        this(
                new VertxSseHttpClient(),
                new ObjectMapper(),
                OpenAiCompatibility.DEFAULT,
                true
        );
    }

    public OpenAiCompatibleModelStream(VertxSseHttpClient transport, ObjectMapper mapper) {
        this(transport, mapper, OpenAiCompatibility.DEFAULT, false);
    }

    public OpenAiCompatibleModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiCompatibility compatibility
    ) {
        this(transport, mapper, compatibility, false);
    }

    private OpenAiCompatibleModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiCompatibility compatibility,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.compatibility = Objects.requireNonNull(compatibility, "compatibility");
        this.codec = new OpenAiChatCodec(mapper, compatibility);
        this.ownsTransport = ownsTransport;
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
        Map<String, String> headers = new LinkedHashMap<>(options.headers());
        headers.put("content-type", "application/json");
        if (options.apiKey() != null && !options.apiKey().isBlank()) {
            headers.put("authorization", "Bearer " + options.apiKey());
        }
        Map<String, OpenAiGrammar.Grammar> grammars;
        byte[] body;
        try {
            grammars = OpenAiGrammar.resolveAll(
                    mapper, context.tools(), compatibility.supportsGrammarTools()
            );
            body = mapper.writeValueAsBytes(codec.encodeRequest(
                    model, context, options.thinkingLevel()
            ));
        } catch (JsonProcessingException failure) {
            return Multi.createFrom().failure(failure);
        }
        SseHttpRequest request = SseHttpRequest.post(
                chatCompletionsUri(model.baseUrl()), headers, body
        );
        return transport.execute(request, options.cancellation())
                .toMulti()
                .onItem().transformToMultiAndConcatenate(response ->
                        codec.decode(response.events(), model, grammars)
                );
    }

    static URI chatCompletionsUri(String baseUrl) {
        String normalized = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        if (normalized.endsWith("/chat/completions")) {
            return URI.create(normalized);
        }
        return URI.create(normalized + "/chat/completions");
    }

    @Override
    public void close() {
        if (ownsTransport) {
            transport.close();
        }
    }
}
