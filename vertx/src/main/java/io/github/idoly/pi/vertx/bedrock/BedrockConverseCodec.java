package io.github.idoly.pi.vertx.bedrock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.*;
import io.smallrye.mutiny.Multi;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** AWS Bedrock ConverseStream request and Smithy event mapping. */
public final class BedrockConverseCodec {
    private static final String EMPTY = "<empty>";
    private final ObjectMapper mapper;

    public BedrockConverseCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public ObjectNode encodeRequest(
            Model model,
            ModelContext context,
            String thinkingLevel
    ) {
        ObjectNode request = mapper.createObjectNode();
        if (!context.systemPrompt().isBlank()) {
            request.putArray("system").addObject()
                    .put("text", context.systemPrompt());
        }
        request.set("messages", encodeMessages(model, context.messages()));
        request.putObject("inferenceConfig").put("maxTokens", model.maxTokens());
        if (!context.tools().isEmpty()) {
            ArrayNode tools = request.putObject("toolConfig").putArray("tools");
            for (ToolDefinition tool : context.tools()) {
                ObjectNode spec = tools.addObject().putObject("toolSpec")
                        .put("name", tool.name())
                        .put("description", tool.description());
                spec.putObject("inputSchema").set(
                        "json", mapper.valueToTree(tool.parameters())
                );
            }
        }
        if (model.reasoning() && thinkingLevel != null
                && !thinkingLevel.equals("off")) {
            int budget = switch (thinkingLevel) {
                case "minimal" -> 1_024;
                case "low" -> 2_048;
                case "medium" -> 8_192;
                default -> 32_768;
            };
            request.putObject("additionalModelRequestFields")
                    .putObject("thinking")
                    .put("type", "enabled")
                    .put("budget_tokens", Math.min(
                            budget, Math.max(1, model.maxTokens() - 1_024)
                    ));
        }
        return request;
    }

    public Multi<AssistantStreamEvent> decode(
            Multi<byte[]> chunks,
            Model model
    ) {
        AwsEventStreamDecoder decoder = new AwsEventStreamDecoder();
        State state = new State(model, mapper);
        AtomicBoolean terminal = new AtomicBoolean();
        return chunks.onItem().transformToMultiAndConcatenate(chunk -> {
            ArrayList<AssistantStreamEvent> output = new ArrayList<>();
            for (AwsEventStreamDecoder.Event event : decoder.decode(chunk)) {
                output.addAll(state.accept(event));
            }
            if (output.stream().anyMatch(value ->
                    value instanceof AssistantStreamEvent.Done
                            || value instanceof AssistantStreamEvent.Error)) {
                terminal.set(true);
            }
            return Multi.createFrom().iterable(output);
        }).onCompletion().invoke(decoder::finish)
                .onCompletion().call(() -> terminal.get()
                        ? io.smallrye.mutiny.Uni.createFrom().voidItem()
                        : io.smallrye.mutiny.Uni.createFrom().failure(
                                new IllegalStateException(
                                        "Bedrock stream ended without a stop reason"
                                )
                        ));
    }

    private ArrayNode encodeMessages(Model model, List<Message> messages) {
        ArrayNode result = mapper.createArrayNode();
        int index = 0;
        while (index < messages.size()) {
            Message message = messages.get(index);
            if (message instanceof ToolResultMessage) {
                ArrayNode content = mapper.createArrayNode();
                while (index < messages.size()
                        && messages.get(index) instanceof ToolResultMessage tool) {
                    ObjectNode encoded = content.addObject().putObject("toolResult")
                            .put("toolUseId", normalizeId(tool.toolCallId()))
                            .put("status", tool.error() ? "error" : "success");
                    encoded.set("content", encodeContent(tool.content(), true));
                    index++;
                }
                result.addObject().put("role", "user").set("content", content);
                continue;
            }
            if (message instanceof UserMessage user) {
                result.addObject().put("role", "user")
                        .set("content", encodeContent(user.content(), true));
            } else if (message instanceof AssistantMessage assistant) {
                ArrayNode content = mapper.createArrayNode();
                boolean anthropic = model.id().toLowerCase().contains("anthropic")
                        || model.id().toLowerCase().contains("claude");
                for (ContentBlock block : assistant.content()) {
                    switch (block) {
                        case TextContent text -> {
                            if (!text.text().isBlank()) {
                                content.addObject().put("text", text.text());
                            }
                        }
                        case ThinkingContent thinking -> {
                            if (thinking.thinking().isBlank()) continue;
                            ObjectNode reasoning = content.addObject()
                                    .putObject("reasoningContent")
                                    .putObject("reasoningText")
                                    .put("text", thinking.thinking());
                            if (anthropic && thinking.signature() != null
                                    && !thinking.signature().isBlank()) {
                                reasoning.put("signature", thinking.signature());
                            }
                        }
                        case ToolCallContent call -> {
                            ObjectNode use = content.addObject()
                                    .putObject("toolUse")
                                    .put("toolUseId", normalizeId(call.id()))
                                    .put("name", call.name());
                            use.set("input", mapper.valueToTree(call.arguments()));
                        }
                        case ImageContent ignored -> {
                        }
                    }
                }
                if (!content.isEmpty()) {
                    result.addObject().put("role", "assistant")
                            .set("content", content);
                }
            }
            index++;
        }
        return result;
    }

    private ArrayNode encodeContent(List<ContentBlock> blocks, boolean required) {
        ArrayNode content = mapper.createArrayNode();
        for (ContentBlock block : blocks) {
            switch (block) {
                case TextContent text -> {
                    if (!text.text().isBlank()) {
                        content.addObject().put("text", text.text());
                    }
                }
                case ImageContent image -> {
                    String format = image.mimeType().substring(
                            image.mimeType().indexOf('/') + 1
                    ).replace("jpeg", "jpeg");
                    content.addObject().putObject("image")
                            .put("format", format)
                            .putObject("source").put("bytes", image.data());
                }
                case ThinkingContent thinking -> {
                    if (!thinking.thinking().isBlank()) {
                        content.addObject().put("text", thinking.thinking());
                    }
                }
                case ToolCallContent ignored -> {
                }
            }
        }
        if (required && content.isEmpty()) {
            content.addObject().put("text", EMPTY);
        }
        return content;
    }

    private static String normalizeId(String id) {
        String normalized = id.replaceAll("[^A-Za-z0-9_-]", "_");
        return normalized.substring(0, Math.min(64, normalized.length()));
    }

    private static final class State {
        private final Model model;
        private final ObjectMapper mapper;
        private final ArrayList<Block> blocks = new ArrayList<>();
        private Usage usage = Usage.ZERO;
        private StopReason stop = StopReason.PENDING;
        private String rawStop;
        private boolean started;
        private boolean terminal;

        private State(Model model, ObjectMapper mapper) {
            this.model = model;
            this.mapper = mapper;
        }

        private List<AssistantStreamEvent> accept(AwsEventStreamDecoder.Event event) {
            String messageType = event.header(":message-type");
            String eventType = event.header(":event-type");
            JsonNode value;
            try {
                value = event.payload().length == 0
                        ? mapper.createObjectNode()
                        : mapper.readTree(new String(
                                event.payload(), StandardCharsets.UTF_8
                        ));
            } catch (Exception failure) {
                throw new IllegalArgumentException(
                        "Invalid Bedrock event payload", failure
                );
            }
            if ("exception".equals(messageType)) {
                terminal = true;
                String message = value.path("message")
                        .asText(eventType == null ? "Bedrock error" : eventType);
                return List.of(new AssistantStreamEvent.Error(error(message)));
            }
            return switch (eventType == null ? "" : eventType) {
                case "messageStart" -> {
                    started = true;
                    yield List.of(new AssistantStreamEvent.Start(snapshot()));
                }
                case "contentBlockStart" -> blockStart(value);
                case "contentBlockDelta" -> blockDelta(value);
                case "contentBlockStop" -> blockStop(value);
                case "messageStop" -> {
                    rawStop = value.path("stopReason").asText();
                    stop = mapStop(rawStop);
                    yield List.of();
                }
                case "metadata" -> metadata(value);
                default -> List.of();
            };
        }

        private List<AssistantStreamEvent> blockStart(JsonNode value) {
            requireStarted();
            int providerIndex = value.path("contentBlockIndex").asInt();
            JsonNode tool = value.path("start").path("toolUse");
            if (!tool.isObject()) return List.of();
            Block block = new Block(providerIndex, ContentKind.TOOL_CALL,
                    new ToolCallContent(
                            tool.path("toolUseId").asText(),
                            tool.path("name").asText(), Map.of()
                    ));
            blocks.add(block);
            return List.of(new AssistantStreamEvent.ContentStart(
                    ContentKind.TOOL_CALL, blocks.size() - 1, snapshot()
            ));
        }

        private List<AssistantStreamEvent> blockDelta(JsonNode value) {
            requireStarted();
            int providerIndex = value.path("contentBlockIndex").asInt();
            JsonNode delta = value.path("delta");
            Block block = find(providerIndex);
            ContentKind kind;
            String text;
            if (delta.has("text")) {
                kind = ContentKind.TEXT;
                text = delta.path("text").asText();
                if (block == null) block = add(providerIndex, kind,
                        new TextContent(""));
                TextContent old = (TextContent) block.content;
                block.content = new TextContent(old.text() + text, old.signature());
            } else if (delta.path("toolUse").has("input")) {
                kind = ContentKind.TOOL_CALL;
                text = delta.path("toolUse").path("input").asText();
                if (block == null) throw new IllegalStateException(
                        "Bedrock tool delta has no start"
                );
                block.partial.append(text);
            } else if (delta.path("reasoningContent").isObject()) {
                kind = ContentKind.THINKING;
                JsonNode reasoning = delta.path("reasoningContent");
                text = reasoning.path("text").asText();
                if (block == null) block = add(providerIndex, kind,
                        new ThinkingContent(""));
                ThinkingContent old = (ThinkingContent) block.content;
                String signature = reasoning.path("signature").isTextual()
                        ? old.signature() == null
                        ? reasoning.path("signature").asText()
                        : old.signature() + reasoning.path("signature").asText()
                        : old.signature();
                block.content = new ThinkingContent(
                        old.thinking() + text, signature
                );
            } else return List.of();
            int index = blocks.indexOf(block);
            return List.of(new AssistantStreamEvent.ContentDelta(
                    kind, index, text, snapshot()
            ));
        }

        private List<AssistantStreamEvent> blockStop(JsonNode value) {
            Block block = find(value.path("contentBlockIndex").asInt());
            if (block == null) return List.of();
            if (block.kind == ContentKind.TOOL_CALL && !block.partial.isEmpty()) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> arguments = mapper.convertValue(
                            mapper.readTree(block.partial.toString()),
                            LinkedHashMap.class
                    );
                    ToolCallContent call = (ToolCallContent) block.content;
                    block.content = new ToolCallContent(
                            call.id(), call.name(), arguments, call.signature()
                    );
                } catch (Exception failure) {
                    throw new IllegalArgumentException(
                            "Invalid Bedrock tool input JSON", failure
                    );
                }
            }
            return List.of(new AssistantStreamEvent.ContentEnd(
                    block.kind, blocks.indexOf(block), snapshot()
            ));
        }

        private List<AssistantStreamEvent> metadata(JsonNode value) {
            JsonNode data = value.path("usage");
            long input = data.path("inputTokens").asLong();
            long output = data.path("outputTokens").asLong();
            long cacheRead = data.path("cacheReadInputTokens").asLong();
            long cacheWrite = data.path("cacheWriteInputTokens").asLong();
            usage = new Usage(
                    input, output, cacheRead, cacheWrite,
                    data.path("totalTokens").asLong(
                            input + output + cacheRead + cacheWrite
                    ), Cost.ZERO
            );
            if (stop == StopReason.PENDING) return List.of();
            terminal = true;
            return List.of(stop == StopReason.ERROR
                    ? new AssistantStreamEvent.Error(error(
                            "Provider stopped with: " + rawStop
                    )) : new AssistantStreamEvent.Done(snapshot()));
        }

        private Block add(int providerIndex, ContentKind kind, ContentBlock content) {
            Block block = new Block(providerIndex, kind, content);
            blocks.add(block);
            return block;
        }

        private Block find(int providerIndex) {
            return blocks.stream().filter(value ->
                    value.providerIndex == providerIndex
            ).findFirst().orElse(null);
        }

        private AssistantMessage snapshot() {
            return new AssistantMessage(
                    blocks.stream().map(value -> value.content).toList(),
                    model.api(), model.provider(), model.id(),
                    UsageCosts.calculate(model, usage), stop,
                    null, System.currentTimeMillis(), null, rawStop
            );
        }

        private AssistantMessage error(String message) {
            return new AssistantMessage(
                    blocks.stream().map(value -> value.content).toList(),
                    model.api(), model.provider(), model.id(),
                    UsageCosts.calculate(model, usage), StopReason.ERROR,
                    message, System.currentTimeMillis(),
                    null, rawStop
            );
        }

        private void requireStarted() {
            if (!started) throw new IllegalStateException(
                    "Bedrock content arrived before messageStart"
            );
        }

        private static StopReason mapStop(String reason) {
            return switch (reason) {
                case "end_turn", "stop_sequence" -> StopReason.STOP;
                case "max_tokens" -> StopReason.LENGTH;
                case "tool_use" -> StopReason.TOOL_USE;
                default -> StopReason.ERROR;
            };
        }
    }

    private static final class Block {
        private final int providerIndex;
        private final ContentKind kind;
        private final StringBuilder partial = new StringBuilder();
        private ContentBlock content;

        private Block(int providerIndex, ContentKind kind, ContentBlock content) {
            this.providerIndex = providerIndex;
            this.kind = kind;
            this.content = content;
        }
    }
}
