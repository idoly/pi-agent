package io.github.idoly.pi.vertx.mistral;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.SseHttpRequest;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.github.idoly.pi.vertx.openai.OpenAiChatCodec;
import io.smallrye.mutiny.Multi;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

/** Native Mistral Conversations streaming over the Mistral chat wire format. */
public final class MistralConversationsModelStream
        implements ModelProvider, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final ObjectMapper mapper;
    private final OpenAiChatCodec codec;
    private final boolean ownsTransport;

    public MistralConversationsModelStream() {
        this(new VertxSseHttpClient(), new ObjectMapper(), true);
    }

    public MistralConversationsModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper
    ) {
        this(transport, mapper, false);
    }

    private MistralConversationsModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.codec = new OpenAiChatCodec(mapper);
        this.ownsTransport = ownsTransport;
    }

    @Override
    public String id() {
        return "mistral-conversations";
    }

    @Override
    public boolean supports(Model model) {
        return model.api().equals("mistral-conversations");
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        if (!supports(model)) {
            return Multi.createFrom().failure(new IllegalArgumentException(
                    "Unsupported Mistral API " + model.api()
            ));
        }
        if (options.apiKey() == null || options.apiKey().isBlank()) {
            return Multi.createFrom().failure(new IllegalArgumentException(
                    "No API key for provider: " + model.provider()
            ));
        }
        ObjectNode request = codec.encodeRequest(
                model, context, options.thinkingLevel()
        );
        normalizeToolIds(request);
        if (model.reasoning() && options.thinkingLevel() != null
                && !options.thinkingLevel().equals("off")) {
            if (model.id().startsWith("magistral")) {
                request.put("prompt_mode", "reasoning");
            } else {
                request.put("reasoning_effort", "high");
            }
        }
        if (options.sessionId() != null && !options.sessionId().isBlank()) {
            request.put("prompt_cache_key", options.sessionId());
        }
        Map<String, String> headers = new LinkedHashMap<>(options.headers());
        headers.put("content-type", "application/json");
        headers.put("accept", "text/event-stream");
        headers.put("authorization", "Bearer " + options.apiKey());
        if (options.sessionId() != null && !options.sessionId().isBlank()) {
            headers.put("x-affinity", options.sessionId());
        }
        byte[] body;
        try {
            body = mapper.writeValueAsBytes(request);
        } catch (JsonProcessingException failure) {
            return Multi.createFrom().failure(failure);
        }
        return transport.execute(
                SseHttpRequest.post(uri(model.baseUrl()), headers, body),
                options.cancellation()
        ).toMulti().onItem().transformToMultiAndConcatenate(response ->
                codec.decode(response.events(), model)
        );
    }

    private void normalizeToolIds(ObjectNode request) {
        Map<String, String> ids = new LinkedHashMap<>();
        JsonNode messages = request.path("messages");
        if (!messages.isArray()) return;
        for (JsonNode message : messages) {
            JsonNode calls = message.path("tool_calls");
            if (calls.isArray()) {
                for (JsonNode call : calls) {
                    if (!(call instanceof ObjectNode object)) continue;
                    String id = object.path("id").asText();
                    object.put("id", ids.computeIfAbsent(
                            id, MistralConversationsModelStream::toolId
                    ));
                }
            }
            if (message instanceof ObjectNode object
                    && object.path("tool_call_id").isTextual()) {
                String id = object.path("tool_call_id").asText();
                object.put("tool_call_id", ids.computeIfAbsent(
                        id, MistralConversationsModelStream::toolId
                ));
            }
        }
    }

    static String toolId(String input) {
        String normalized = input.replaceAll("[^A-Za-z0-9]", "");
        if (normalized.length() == 9) return normalized;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    input.getBytes(StandardCharsets.UTF_8)
            );
            String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            StringBuilder value = new StringBuilder(9);
            for (int index = 0; index < 9; index++) {
                value.append(alphabet.charAt(
                        Byte.toUnsignedInt(digest[index]) % alphabet.length()
                ));
            }
            return value.toString();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    static URI uri(String baseUrl) {
        String normalized = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        if (normalized.endsWith("/v1/chat/completions")
                || normalized.endsWith("/chat/completions")) {
            return URI.create(normalized);
        }
        return URI.create(normalized + "/v1/chat/completions");
    }

    @Override
    public void close() {
        if (ownsTransport) transport.close();
    }
}
