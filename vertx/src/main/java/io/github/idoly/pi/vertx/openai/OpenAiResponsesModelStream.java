package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

/** OpenAI Responses API implementation of the runtime-neutral ModelStream contract. */
public final class OpenAiResponsesModelStream implements ModelStream, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final OpenAiResponsesCodec codec;
    private final OpenAiResponsesCompatibility compatibility;
    private final ObjectMapper mapper;
    private final boolean ownsTransport;

    public OpenAiResponsesModelStream() {
        this(
                new VertxSseHttpClient(), new ObjectMapper(),
                OpenAiResponsesCompatibility.DEFAULT, true
        );
    }

    public OpenAiResponsesModelStream(VertxSseHttpClient transport, ObjectMapper mapper) {
        this(transport, mapper, OpenAiResponsesCompatibility.DEFAULT, false);
    }

    public OpenAiResponsesModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiResponsesCompatibility compatibility
    ) {
        this(transport, mapper, compatibility, false);
    }

    private OpenAiResponsesModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            OpenAiResponsesCompatibility compatibility,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.compatibility = Objects.requireNonNull(compatibility, "compatibility");
        this.codec = new OpenAiResponsesCodec(mapper, compatibility);
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
        String sessionId = options.sessionId();
        if (sessionId != null && !sessionId.isBlank()) {
            headers.putAll(sessionAffinityHeaders(model, compatibility, sessionId));
        }
        Map<String, OpenAiGrammar.Grammar> grammars;
        byte[] body;
        try {
            grammars = OpenAiGrammar.resolveAll(
                    mapper, context.tools(), compatibility.supportsGrammarTools()
            );
            ObjectNode request = codec.encodeRequest(model, context, options.thinkingLevel());
            if (sessionId != null && !sessionId.isBlank()) {
                request.put("prompt_cache_key", sessionId.substring(0, Math.min(64, sessionId.length())));
            }
            body = mapper.writeValueAsBytes(request);
        } catch (JsonProcessingException failure) {
            return Multi.createFrom().failure(failure);
        }
        SseHttpRequest request = SseHttpRequest.post(
                responsesUri(model.baseUrl()), headers, body
        );
        return transport.execute(request, options.cancellation())
                .toMulti()
                .onItem().transformToMultiAndConcatenate(response ->
                        codec.decode(response.events(), model, grammars)
                );
    }

    static Map<String, String> sessionAffinityHeaders(
            Model model,
            OpenAiResponsesCompatibility compatibility,
            String sessionId
    ) {
        Map<String, String> headers = new LinkedHashMap<>();
        OpenAiResponsesCompatibility.SessionAffinityFormat format =
                compatibility.sessionAffinityFormat();
        if (format == OpenAiResponsesCompatibility.SessionAffinityFormat.AUTO) {
            if (model.provider().equals("openrouter")
                    || model.baseUrl().contains("openrouter.ai")) {
                format = OpenAiResponsesCompatibility.SessionAffinityFormat.OPENROUTER;
            } else if (model.provider().equals("opencode")) {
                format = OpenAiResponsesCompatibility.SessionAffinityFormat.OPENAI_NO_SESSION;
            } else {
                format = OpenAiResponsesCompatibility.SessionAffinityFormat.OPENAI;
            }
        }
        if (format == OpenAiResponsesCompatibility.SessionAffinityFormat.OPENROUTER) {
            headers.put("x-session-id", sessionId);
        } else {
            headers.put("x-client-request-id", sessionId);
            if (format == OpenAiResponsesCompatibility.SessionAffinityFormat.OPENAI) {
                headers.put("session_id", sessionId);
            }
        }
        return Map.copyOf(headers);
    }

    static URI responsesUri(String baseUrl) {
        String normalized = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        if (normalized.endsWith("/responses")) {
            return URI.create(normalized);
        }
        return URI.create(normalized + "/responses");
    }

    @Override
    public void close() {
        if (ownsTransport) {
            transport.close();
        }
    }
}
