package io.github.idoly.pi.vertx.anthropic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.SseEvent;
import io.smallrye.mutiny.Multi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Structured Anthropic Messages request and streaming event codec. */
public final class AnthropicMessagesCodec {
    private final ObjectMapper mapper;

    public AnthropicMessagesCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public ObjectNode encodeRequest(
            Model model,
            ModelContext context,
            String thinkingLevel
    ) {
        ObjectNode request = mapper.createObjectNode()
                .put("model", model.id())
                .put("max_tokens", model.maxTokens())
                .put("stream", true);
        if (!context.systemPrompt().isBlank()) {
            request.putArray("system").addObject()
                    .put("type", "text")
                    .put("text", context.systemPrompt())
                    .putObject("cache_control")
                    .put("type", "ephemeral");
        }
        request.set("messages", encodeMessages(model, context.messages()));
        if (!context.tools().isEmpty()) {
            ArrayNode tools = request.putArray("tools");
            for (ToolDefinition tool : context.tools()) {
                ObjectNode encoded = tools.addObject()
                        .put("name", tool.name())
                        .put("description", tool.description())
                        .put("eager_input_streaming", true);
                encoded.set("input_schema", mapper.valueToTree(tool.parameters()));
                encoded.putObject("cache_control")
                        .put("type", "ephemeral");
            }
        }
        if (model.reasoning() && thinkingLevel != null
                && !thinkingLevel.equals("off")) {
            int budget = switch (thinkingLevel) {
                case "minimal" -> 1_024;
                case "low" -> 2_048;
                case "medium" -> 8_192;
                case "high", "xhigh", "max" -> Math.min(
                        32_768, Math.max(1_024, model.maxTokens() - 1)
                );
                default -> throw new IllegalArgumentException(
                        "Unsupported thinking level " + thinkingLevel
                );
            };
            request.putObject("thinking")
                    .put("type", "enabled")
                    .put("budget_tokens", budget)
                    .put("display", "summarized");
        }
        return request;
    }

    public Multi<AssistantStreamEvent> decode(
            Multi<SseEvent> events,
            Model model
    ) {
        State state = new State(model, mapper);
        AtomicBoolean stopped = new AtomicBoolean();
        Multi<AssistantStreamEvent> decoded = events.onItem()
                .transformToMultiAndConcatenate(event -> {
                    try {
                        List<AssistantStreamEvent> output = state.accept(event);
                        if (output.stream().anyMatch(value ->
                                value instanceof AssistantStreamEvent.Done
                                        || value instanceof AssistantStreamEvent.Error)) {
                            stopped.set(true);
                        }
                        return Multi.createFrom().iterable(output);
                    } catch (RuntimeException failure) {
                        return Multi.createFrom().failure(failure);
                    }
                });
        return Multi.createBy().concatenating().streams(
                Multi.createFrom().item(new AssistantStreamEvent.Start(
                        state.snapshot()
                )),
                decoded
        ).onCompletion().call(() -> stopped.get()
                ? io.smallrye.mutiny.Uni.createFrom().voidItem()
                : io.smallrye.mutiny.Uni.createFrom().failure(
                        new IllegalStateException(
                                "Anthropic stream ended before message_stop"
                        )
                ));
    }

    private ArrayNode encodeMessages(Model model, List<Message> messages) {
        ArrayNode result = mapper.createArrayNode();
        for (Message message : messages) {
            switch (message) {
                case UserMessage user -> result.addObject()
                        .put("role", "user")
                        .set("content", encodeUserContent(user.content()));
                case AssistantMessage assistant -> {
                    ArrayNode content = mapper.createArrayNode();
                    boolean same = assistant.provider().equals(model.provider())
                            && assistant.model().equals(model.id());
                    for (ContentBlock block : assistant.content()) {
                        switch (block) {
                            case TextContent text -> content.addObject()
                                    .put("type", "text")
                                    .put("text", text.text());
                            case ThinkingContent thinking -> {
                                if (same && thinking.signature() != null) {
                                    content.addObject()
                                            .put("type", "thinking")
                                            .put("thinking", thinking.thinking())
                                            .put("signature", thinking.signature());
                                } else if (!thinking.thinking().isBlank()) {
                                    content.addObject()
                                            .put("type", "text")
                                            .put("text", thinking.thinking());
                                }
                            }
                            case ToolCallContent call -> {
                                ObjectNode tool = content.addObject()
                                        .put("type", "tool_use")
                                        .put("id", call.id())
                                        .put("name", call.name());
                                tool.set("input", mapper.valueToTree(call.arguments()));
                            }
                            case ImageContent ignored -> {
                                // Assistant images are not part of the Anthropic message contract.
                            }
                        }
                    }
                    if (!content.isEmpty()) {
                        result.addObject().put("role", "assistant")
                                .set("content", content);
                    }
                }
                case ToolResultMessage toolResult -> {
                    ObjectNode block = mapper.createObjectNode()
                            .put("type", "tool_result")
                            .put("tool_use_id", toolResult.toolCallId())
                            .put("is_error", toolResult.error());
                    block.set("content", encodeUserContent(toolResult.content()));
                    block.putObject("cache_control")
                            .put("type", "ephemeral");
                    result.addObject().put("role", "user")
                            .set("content", mapper.createArrayNode().add(block));
                }
            }
        }
        return result;
    }

    private JsonNode encodeUserContent(List<ContentBlock> blocks) {
        boolean textOnly = blocks.stream().allMatch(block ->
                block instanceof TextContent || block instanceof ThinkingContent
        );
        if (textOnly) {
            String text = blocks.stream().map(block -> switch (block) {
                case TextContent value -> value.text();
                case ThinkingContent value -> value.thinking();
                default -> "";
            }).reduce((left, right) -> left + "\n" + right).orElse("");
            return mapper.getNodeFactory().textNode(text);
        }
        ArrayNode result = mapper.createArrayNode();
        for (ContentBlock block : blocks) {
            switch (block) {
                case TextContent text -> result.addObject()
                        .put("type", "text").put("text", text.text());
                case ImageContent image -> result.addObject()
                        .put("type", "image")
                        .putObject("source")
                        .put("type", "base64")
                        .put("media_type", image.mimeType())
                        .put("data", image.data());
                case ThinkingContent thinking -> result.addObject()
                        .put("type", "text").put("text", thinking.thinking());
                case ToolCallContent ignored -> {
                }
            }
        }
        if (result.isEmpty()) {
            result.addObject().put("type", "text").put("text", "");
        }
        return result;
    }

    private static final class State {
        private final Model model;
        private final ObjectMapper mapper;
        private final ArrayList<ContentBlock> blocks = new ArrayList<>();
        private final Map<Integer, StringBuilder> toolJson = new LinkedHashMap<>();
        private Usage usage = Usage.ZERO;
        private StopReason stopReason = StopReason.PENDING;
        private String responseId;
        private String rawStopReason;
        private boolean started;

        private State(Model model, ObjectMapper mapper) {
            this.model = model;
            this.mapper = mapper;
        }

        private List<AssistantStreamEvent> accept(SseEvent event) {
            if (event.data() == null || event.data().isBlank()) return List.of();
            JsonNode value;
            try {
                value = mapper.readTree(event.data());
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException(
                        "Invalid Anthropic SSE event " + event.event(), failure
                );
            }
            String type = value.path("type").asText(event.event());
            return switch (type) {
                case "ping" -> List.of();
                case "error" -> List.of(new AssistantStreamEvent.Error(
                        error(event.data())
                ));
                case "message_start" -> messageStart(value);
                case "content_block_start" -> contentStart(value);
                case "content_block_delta" -> contentDelta(value);
                case "content_block_stop" -> contentStop(value);
                case "message_delta" -> messageDelta(value);
                case "message_stop" -> List.of(
                        new AssistantStreamEvent.Done(snapshot())
                );
                default -> List.of();
            };
        }

        private List<AssistantStreamEvent> messageStart(JsonNode value) {
            JsonNode message = value.path("message");
            responseId = textOrNull(message, "id");
            JsonNode inputUsage = message.path("usage");
            usage = usage(
                    inputUsage.path("input_tokens").asLong(), 0,
                    inputUsage.path("cache_read_input_tokens").asLong(),
                    inputUsage.path("cache_creation_input_tokens").asLong()
            );
            started = true;
            return List.of();
        }

        private List<AssistantStreamEvent> contentStart(JsonNode value) {
            requireStarted();
            int index = value.path("index").asInt();
            JsonNode block = value.path("content_block");
            ContentKind kind;
            ContentBlock content;
            switch (block.path("type").asText()) {
                case "text" -> {
                    kind = ContentKind.TEXT;
                    content = new TextContent(block.path("text").asText());
                }
                case "thinking", "redacted_thinking" -> {
                    kind = ContentKind.THINKING;
                    content = new ThinkingContent(
                            block.path("thinking").asText(),
                            textOrNull(block, "signature")
                    );
                }
                case "tool_use" -> {
                    kind = ContentKind.TOOL_CALL;
                    content = new ToolCallContent(
                            block.path("id").asText(),
                            block.path("name").asText(),
                            objectMap(block.path("input")), null
                    );
                    toolJson.put(index, new StringBuilder());
                }
                default -> throw new IllegalArgumentException(
                        "Unknown Anthropic content block " + block.path("type")
                );
            }
            set(index, content);
            return List.of(new AssistantStreamEvent.ContentStart(
                    kind, index, snapshot()
            ));
        }

        private List<AssistantStreamEvent> contentDelta(JsonNode value) {
            int index = value.path("index").asInt();
            JsonNode delta = value.path("delta");
            String type = delta.path("type").asText();
            ContentKind kind;
            String text;
            switch (type) {
                case "text_delta" -> {
                    kind = ContentKind.TEXT;
                    text = delta.path("text").asText();
                    TextContent old = (TextContent) blocks.get(index);
                    set(index, new TextContent(
                            old.text() + text, old.signature()
                    ));
                }
                case "thinking_delta" -> {
                    kind = ContentKind.THINKING;
                    text = delta.path("thinking").asText();
                    ThinkingContent old = (ThinkingContent) blocks.get(index);
                    set(index, new ThinkingContent(
                            old.thinking() + text, old.signature()
                    ));
                }
                case "signature_delta" -> {
                    kind = ContentKind.THINKING;
                    text = delta.path("signature").asText();
                    ThinkingContent old = (ThinkingContent) blocks.get(index);
                    set(index, new ThinkingContent(old.thinking(), text));
                }
                case "input_json_delta" -> {
                    kind = ContentKind.TOOL_CALL;
                    text = delta.path("partial_json").asText();
                    toolJson.computeIfAbsent(index, ignored -> new StringBuilder())
                            .append(text);
                }
                default -> { return List.of(); }
            }
            return List.of(new AssistantStreamEvent.ContentDelta(
                    kind, index, text, snapshot()
            ));
        }

        private List<AssistantStreamEvent> contentStop(JsonNode value) {
            int index = value.path("index").asInt();
            ContentBlock current = blocks.get(index);
            ContentKind kind = switch (current) {
                case TextContent ignored -> ContentKind.TEXT;
                case ThinkingContent ignored -> ContentKind.THINKING;
                case ToolCallContent call -> {
                    String json = toolJson.getOrDefault(
                            index, new StringBuilder()
                    ).toString();
                    if (!json.isBlank()) {
                        set(index, new ToolCallContent(
                                call.id(), call.name(), objectMap(parse(json)),
                                call.signature()
                        ));
                    }
                    yield ContentKind.TOOL_CALL;
                }
                case ImageContent ignored -> throw new IllegalStateException();
            };
            return List.of(new AssistantStreamEvent.ContentEnd(
                    kind, index, snapshot()
            ));
        }

        private List<AssistantStreamEvent> messageDelta(JsonNode value) {
            JsonNode delta = value.path("delta");
            rawStopReason = textOrNull(delta, "stop_reason");
            stopReason = mapStop(rawStopReason);
            JsonNode outputUsage = value.path("usage");
            usage = usage(
                    usage.input(), outputUsage.path("output_tokens").asLong(),
                    usage.cacheRead(), usage.cacheWrite()
            );
            return List.of();
        }

        private AssistantMessage snapshot() {
            return new AssistantMessage(
                    blocks, model.api(), model.provider(), model.id(),
                    UsageCosts.calculate(model, usage),
                    stopReason, null, System.currentTimeMillis(),
                    responseId, rawStopReason
            );
        }

        private AssistantMessage error(String message) {
            return new AssistantMessage(
                    List.copyOf(blocks), model.api(), model.provider(),
                    model.id(), UsageCosts.calculate(model, usage),
                    StopReason.ERROR, message, System.currentTimeMillis(),
                    responseId, rawStopReason
            );
        }

        private void requireStarted() {
            if (!started) throw new IllegalStateException(
                    "Anthropic content arrived before message_start"
            );
        }

        private void set(int index, ContentBlock value) {
            while (blocks.size() <= index) blocks.add(new TextContent(""));
            blocks.set(index, value);
        }

        private JsonNode parse(String json) {
            try {
                return mapper.readTree(json);
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException(
                        "Invalid Anthropic tool input JSON", failure
                );
            }
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> objectMap(JsonNode value) {
            if (!value.isObject()) return Map.of();
            return mapper.convertValue(value, LinkedHashMap.class);
        }

        private static Usage usage(
                long input, long output, long cacheRead, long cacheWrite
        ) {
            return new Usage(
                    input, output, cacheRead, cacheWrite, 0,
                    input + output + cacheRead + cacheWrite, Cost.ZERO
            );
        }

        private static StopReason mapStop(String value) {
            if (value == null) return StopReason.PENDING;
            return switch (value) {
                case "end_turn", "stop_sequence", "pause_turn",
                     "refusal" -> StopReason.STOP;
                case "max_tokens", "model_context_window_exceeded" ->
                        StopReason.LENGTH;
                case "tool_use" -> StopReason.TOOL_USE;
                default -> StopReason.ERROR;
            };
        }

        private static String textOrNull(JsonNode node, String field) {
            JsonNode value = node.get(field);
            return value == null || value.isNull() ? null : value.asText();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> objectMap(JsonNode value) {
        if (!value.isObject()) return Map.of();
        return mapper.convertValue(value, LinkedHashMap.class);
    }
}
