package io.github.idoly.pi.vertx.mistral;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.internal.ProviderHeaders;
import io.github.idoly.pi.vertx.internal.ProviderHttpHooks;
import io.github.idoly.pi.vertx.SseHttpRequest;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.github.idoly.pi.vertx.openai.OpenAiChatCodec;
import io.smallrye.mutiny.Multi;

import java.net.URI;
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
        ObjectNode request = encodeRequest(model, context, options);
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("content-type", "application/json");
        headers.put("accept", "text/event-stream");
        headers.put("authorization", "Bearer " + options.apiKey());
        if (options.sessionId() != null && !options.sessionId().isBlank()) {
            headers.put("x-affinity", options.sessionId());
        }
        headers = ProviderHeaders.merge(headers, options.headers());
        return ProviderHttpHooks.prepare(
                mapper, model, request, headers, options
        ).toMulti().onItem().transformToMultiAndConcatenate(prepared -> {
            byte[] body;
            try {
                body = mapper.writeValueAsBytes(prepared.payload());
            } catch (JsonProcessingException failure) {
                return Multi.createFrom().failure(failure);
            }
            return ProviderHttpHooks.observeSse(transport.execute(
                    SseHttpRequest.post(
                            uri(model.baseUrl()), prepared.headers(), body
                    ),
                    options.cancellation()
            ), model, options).toMulti()
                    .onItem().transformToMultiAndConcatenate(response ->
                            codec.decode(response.events(), model)
                    );
        });
    }

    ObjectNode encodeRequest(
            Model model, ModelContext context, StreamOptions options
    ) {
        ObjectNode request = mapper.createObjectNode()
                .put("model", model.id())
                .put("stream", true);
        ArrayNode messages = request.putArray("messages");
        if (!context.systemPrompt().isBlank()) {
            messages.addObject().put("role", "system")
                    .put("content", context.systemPrompt());
        }
        Map<String, String> toolIds = new LinkedHashMap<>();
        for (Message message : context.messages()) {
            switch (message) {
                case UserMessage user -> encodeUserMessage(
                        messages, user, model.input().contains("image")
                );
                case AssistantMessage assistant -> encodeAssistantMessage(
                        messages, assistant, model, toolIds
                );
                case ToolResultMessage result -> encodeToolResult(
                        messages, result, model.input().contains("image"),
                        toolIds
                );
            }
        }
        if (!context.tools().isEmpty()) {
            ArrayNode tools = request.putArray("tools");
            for (ToolDefinition tool : context.tools()) {
                ObjectNode function = tools.addObject()
                        .put("type", "function")
                        .putObject("function")
                        .put("name", tool.name())
                        .put("description", tool.description());
                function.set(
                        "parameters", mapper.valueToTree(tool.parameters())
                );
                function.put("strict", false);
            }
        }
        request.put("max_tokens", model.maxTokens());
        if (model.reasoning() && options.thinkingLevel() != null
                && !options.thinkingLevel().equals("off")) {
            if (usesReasoningEffort(model.id())) {
                String effort = "high";
                ThinkingLevelMap mapping = model.thinkingLevelMap();
                if (mapping != null) {
                    String mapped = mapping.providerValue(
                            options.thinkingLevel()
                    );
                    if (mapped != null) effort = mapped;
                }
                request.put("reasoning_effort", effort);
            } else {
                request.put("prompt_mode", "reasoning");
            }
        }
        if (options.sessionId() != null && !options.sessionId().isBlank()) {
            request.put("prompt_cache_key", options.sessionId());
        }
        return request;
    }

    private void encodeUserMessage(
            ArrayNode messages, UserMessage user, boolean supportsImages
    ) {
        boolean textOnly = user.content().stream()
                .allMatch(TextContent.class::isInstance);
        if (textOnly) {
            String text = user.content().stream()
                    .map(TextContent.class::cast)
                    .map(TextContent::text)
                    .reduce((left, right) -> left + "\n" + right)
                    .orElse("");
            messages.addObject().put("role", "user").put("content", text);
            return;
        }
        ArrayNode content = mapper.createArrayNode();
        boolean hadImages = false;
        for (ContentBlock block : user.content()) {
            if (block instanceof TextContent text) {
                content.addObject().put("type", "text")
                        .put("text", text.text());
            } else if (block instanceof ImageContent image) {
                hadImages = true;
                if (supportsImages) {
                    content.addObject().put("type", "image_url")
                            .put("image_url", "data:" + image.mimeType()
                                    + ";base64," + image.data());
                }
            }
        }
        if (!content.isEmpty()) {
            messages.addObject().put("role", "user").set("content", content);
        } else if (hadImages) {
            messages.addObject().put("role", "user")
                    .put("content", "(image omitted: model does not support images)");
        }
    }

    private void encodeAssistantMessage(
            ArrayNode messages, AssistantMessage assistant, Model model,
            Map<String, String> toolIds
    ) {
        ArrayNode content = mapper.createArrayNode();
        ArrayNode calls = mapper.createArrayNode();
        boolean same = assistant.provider().equals(model.provider())
                && assistant.model().equals(model.id());
        for (ContentBlock block : assistant.content()) {
            switch (block) {
                case TextContent text -> {
                    if (!text.text().isBlank()) {
                        content.addObject().put("type", "text")
                                .put("text", text.text());
                    }
                }
                case ThinkingContent thinking -> {
                    if (!thinking.thinking().isBlank()) {
                        content.addObject().put("type", "thinking")
                                .putArray("thinking").addObject()
                                .put("type", "text")
                                .put("text", thinking.thinking());
                    }
                }
                case ToolCallContent call -> {
                    String id = same ? call.id() : toolId(call.id());
                    toolIds.put(call.id(), id);
                    ObjectNode encoded = calls.addObject()
                            .put("id", id)
                            .put("type", "function");
                    encoded.putObject("function")
                            .put("name", call.name())
                            .put("arguments", json(call.arguments()));
                    encoded.put("index", 0);
                }
                case ImageContent ignored -> {
                }
            }
        }
        if (content.isEmpty() && calls.isEmpty()) return;
        ObjectNode encoded = messages.addObject()
                .put("role", "assistant")
                .put("prefix", false);
        if (!content.isEmpty()) encoded.set("content", content);
        if (!calls.isEmpty()) encoded.set("tool_calls", calls);
    }

    private void encodeToolResult(
            ArrayNode messages, ToolResultMessage result,
            boolean supportsImages, Map<String, String> toolIds
    ) {
        ArrayNode content = mapper.createArrayNode();
        String text = result.content().stream()
                .filter(TextContent.class::isInstance)
                .map(TextContent.class::cast)
                .map(TextContent::text)
                .reduce((left, right) -> left + "\n" + right)
                .orElse("").trim();
        boolean hasImages = result.content().stream()
                .anyMatch(ImageContent.class::isInstance);
        String rendered;
        if (!text.isEmpty()) {
            rendered = (result.error() ? "[tool error] " : "") + text
                    + (hasImages && !supportsImages
                    ? "\n[tool image omitted: model does not support images]" : "");
        } else if (hasImages) {
            rendered = result.error()
                    ? "[tool error] (see attached image)"
                    : "(see attached image)";
            if (!supportsImages) {
                rendered = result.error()
                        ? "[tool error] (image omitted: model does not support images)"
                        : "(image omitted: model does not support images)";
            }
        } else {
            rendered = result.error()
                    ? "[tool error] (no tool output)" : "(no tool output)";
        }
        content.addObject().put("type", "text").put("text", rendered);
        if (supportsImages) {
            for (ContentBlock block : result.content()) {
                if (block instanceof ImageContent image) {
                    content.addObject().put("type", "image_url")
                            .put("image_url", "data:" + image.mimeType()
                                    + ";base64," + image.data());
                }
            }
        }
        messages.addObject().put("role", "tool")
                .put("name", result.toolName())
                .set("content", content);
        ((ObjectNode) messages.get(messages.size() - 1)).put(
                "tool_call_id", toolIds.getOrDefault(
                        result.toolCallId(), toolId(result.toolCallId())
                )
        );
    }

    private String json(Map<String, Object> value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Invalid tool arguments", failure);
        }
    }

    private static boolean usesReasoningEffort(String modelId) {
        return modelId.equals("mistral-small-2603")
                || modelId.equals("mistral-small-latest")
                || modelId.equals("mistral-medium-3.5");
    }

    static String toolId(String input) {
        String normalized = input.replaceAll("[^A-Za-z0-9]", "");
        if (normalized.length() == 9) return normalized;
        String seed = normalized.isEmpty() ? input : normalized;
        int h1 = 0xdeadbeef;
        int h2 = 0x41c6ce57;
        for (int index = 0; index < seed.length(); index++) {
            int character = seed.charAt(index);
            h1 = (h1 ^ character) * (int) 2_654_435_761L;
            h2 = (h2 ^ character) * 1_597_334_677;
        }
        h1 = ((h1 ^ (h1 >>> 16)) * (int) 2_246_822_507L)
                ^ ((h2 ^ (h2 >>> 13)) * (int) 3_266_489_909L);
        h2 = ((h2 ^ (h2 >>> 16)) * (int) 2_246_822_507L)
                ^ ((h1 ^ (h1 >>> 13)) * (int) 3_266_489_909L);
        String hash = Long.toString(Integer.toUnsignedLong(h2), 36)
                + Long.toString(Integer.toUnsignedLong(h1), 36);
        return hash.substring(0, 9);
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
