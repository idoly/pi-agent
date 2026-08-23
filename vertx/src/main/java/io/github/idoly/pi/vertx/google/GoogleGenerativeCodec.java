package io.github.idoly.pi.vertx.google;

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
import java.util.concurrent.atomic.AtomicLong;

/** Shared Google Generative AI and Vertex request/stream codec. */
public final class GoogleGenerativeCodec {
    private static final AtomicLong TOOL_IDS = new AtomicLong();
    private final ObjectMapper mapper;

    public GoogleGenerativeCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public ObjectNode encodeRequest(
            Model model,
            ModelContext context,
            String thinkingLevel
    ) {
        ObjectNode request = mapper.createObjectNode();
        if (!context.systemPrompt().isBlank()) {
            request.putObject("systemInstruction")
                    .putArray("parts").addObject()
                    .put("text", context.systemPrompt());
        }
        request.set("contents", encodeMessages(model, context.messages()));
        ObjectNode config = request.putObject("generationConfig")
                .put("maxOutputTokens", model.maxTokens());
        if (model.reasoning()) {
            ObjectNode thinking = config.putObject("thinkingConfig");
            if (thinkingLevel == null || thinkingLevel.equals("off")) {
                thinking.put("thinkingBudget", 0);
            } else {
                thinking.put("includeThoughts", true);
                if (model.id().toLowerCase().contains("gemini-3")) {
                    thinking.put("thinkingLevel", switch (thinkingLevel) {
                        case "minimal" -> "MINIMAL";
                        case "low" -> "LOW";
                        case "medium" -> "MEDIUM";
                        default -> "HIGH";
                    });
                } else {
                    thinking.put("thinkingBudget", switch (thinkingLevel) {
                        case "minimal" -> 128;
                        case "low" -> 2_048;
                        case "medium" -> 8_192;
                        default -> 32_768;
                    });
                }
            }
        }
        if (!context.tools().isEmpty()) {
            ArrayNode declarations = request.putArray("tools")
                    .addObject().putArray("functionDeclarations");
            for (ToolDefinition tool : context.tools()) {
                ObjectNode declaration = declarations.addObject()
                        .put("name", tool.name())
                        .put("description", tool.description());
                declaration.set(
                        "parametersJsonSchema",
                        mapper.valueToTree(tool.parameters())
                );
            }
        }
        return request;
    }

    public Multi<AssistantStreamEvent> decode(
            Multi<SseEvent> events,
            Model model
    ) {
        State state = new State(model, mapper);
        AtomicBoolean terminal = new AtomicBoolean();
        Multi<AssistantStreamEvent> start = Multi.createFrom().item(
                new AssistantStreamEvent.Start(state.snapshot())
        );
        Multi<AssistantStreamEvent> updates = events
                .onItem().transformToMultiAndConcatenate(event -> {
                    List<AssistantStreamEvent> decoded = state.accept(event);
                    if (decoded.stream().anyMatch(value ->
                            value instanceof AssistantStreamEvent.Done
                                    || value instanceof AssistantStreamEvent.Error)) {
                        terminal.set(true);
                    }
                    return Multi.createFrom().iterable(decoded);
                })
                .onCompletion().call(() -> terminal.get()
                        ? io.smallrye.mutiny.Uni.createFrom().voidItem()
                        : io.smallrye.mutiny.Uni.createFrom().failure(
                                new IllegalStateException(
                                        "Google stream ended without a finish reason"
                                )
                        ));
        return Multi.createBy().concatenating().streams(start, updates);
    }

    private ArrayNode encodeMessages(Model model, List<Message> messages) {
        ArrayNode contents = mapper.createArrayNode();
        for (Message message : messages) {
            switch (message) {
                case UserMessage user -> contents.addObject()
                        .put("role", "user")
                        .set("parts", encodeUserParts(user.content()));
                case AssistantMessage assistant -> {
                    boolean same = assistant.provider().equals(model.provider())
                            && assistant.model().equals(model.id());
                    ArrayNode parts = mapper.createArrayNode();
                    for (ContentBlock block : assistant.content()) {
                        switch (block) {
                            case TextContent text -> {
                                if (text.text().isBlank() && text.signature() == null) continue;
                                ObjectNode part = parts.addObject().put("text", text.text());
                                if (same && validSignature(text.signature())) {
                                    part.put("thoughtSignature", text.signature());
                                }
                            }
                            case ThinkingContent thinking -> {
                                if (!same) {
                                    if (!thinking.thinking().isBlank()) {
                                        parts.addObject().put("text", thinking.thinking());
                                    }
                                    continue;
                                }
                                if (thinking.thinking().isBlank()
                                        && thinking.signature() == null) continue;
                                ObjectNode part = parts.addObject()
                                        .put("thought", true)
                                        .put("text", thinking.thinking());
                                if (validSignature(thinking.signature())) {
                                    part.put("thoughtSignature", thinking.signature());
                                }
                            }
                            case ToolCallContent call -> {
                                ObjectNode part = parts.addObject();
                                ObjectNode function = part.putObject("functionCall")
                                        .put("name", call.name());
                                function.set("args", mapper.valueToTree(call.arguments()));
                                if (requiresToolId(model.id())) {
                                    function.put("id", normalizeToolId(call.id()));
                                }
                                if (same && validSignature(call.signature())) {
                                    part.put("thoughtSignature", call.signature());
                                }
                            }
                            case ImageContent ignored -> {
                            }
                        }
                    }
                    if (!parts.isEmpty()) {
                        contents.addObject().put("role", "model").set("parts", parts);
                    }
                }
                case ToolResultMessage result -> {
                    ObjectNode functionResponse = mapper.createObjectNode();
                    ObjectNode response = functionResponse.putObject("functionResponse")
                            .put("name", result.toolName());
                    if (requiresToolId(model.id())) {
                        response.put("id", normalizeToolId(result.toolCallId()));
                    }
                    String text = result.content().stream()
                            .filter(TextContent.class::isInstance)
                            .map(TextContent.class::cast)
                            .map(TextContent::text)
                            .reduce((left, right) -> left + "\n" + right)
                            .orElse("");
                    response.putObject("response")
                            .put(result.error() ? "error" : "output", text);
                    JsonNode last = contents.isEmpty()
                            ? null : contents.get(contents.size() - 1);
                    if (last != null && last.path("role").asText().equals("user")
                            && last.path("parts").isArray()
                            && java.util.stream.StreamSupport.stream(
                            last.path("parts").spliterator(), false
                    ).anyMatch(part -> part.has("functionResponse"))) {
                        ((ArrayNode) last.path("parts")).add(functionResponse);
                    } else {
                        contents.addObject().put("role", "user")
                                .set("parts", mapper.createArrayNode()
                                        .add(functionResponse));
                    }
                }
            }
        }
        return contents;
    }

    private ArrayNode encodeUserParts(List<ContentBlock> blocks) {
        ArrayNode parts = mapper.createArrayNode();
        for (ContentBlock block : blocks) {
            switch (block) {
                case TextContent text -> parts.addObject().put("text", text.text());
                case ImageContent image -> parts.addObject()
                        .putObject("inlineData")
                        .put("mimeType", image.mimeType())
                        .put("data", image.data());
                case ThinkingContent thinking -> parts.addObject()
                        .put("text", thinking.thinking());
                case ToolCallContent ignored -> {
                }
            }
        }
        return parts;
    }

    private static boolean requiresToolId(String modelId) {
        String lower = modelId.toLowerCase();
        if (lower.startsWith("claude-") || lower.startsWith("gpt-oss-")) return true;
        java.util.regex.Matcher matcher = java.util.regex.Pattern
                .compile("^gemini(?:-live)?-(\\d+)").matcher(lower);
        return matcher.find() && Integer.parseInt(matcher.group(1)) >= 3;
    }

    private static String normalizeToolId(String id) {
        String normalized = id.replaceAll("[^A-Za-z0-9_-]", "_");
        return normalized.substring(0, Math.min(64, normalized.length()));
    }

    private static boolean validSignature(String signature) {
        if (signature == null || signature.isBlank()
                || signature.length() % 4 != 0) return false;
        return signature.matches("^[A-Za-z0-9+/]+={0,2}$");
    }

    private static final class State {
        private final Model model;
        private final ObjectMapper mapper;
        private final ArrayList<ContentBlock> blocks = new ArrayList<>();
        private Usage usage = Usage.ZERO;
        private StopReason stop = StopReason.PENDING;
        private String responseId;
        private String rawStop;
        private Integer activeIndex;
        private ContentKind activeKind;
        private boolean terminal;

        private State(Model model, ObjectMapper mapper) {
            this.model = model;
            this.mapper = mapper;
        }

        private List<AssistantStreamEvent> accept(SseEvent event) {
            if (terminal || event.data() == null || event.data().isBlank()) {
                return List.of();
            }
            JsonNode root;
            try {
                root = mapper.readTree(event.data());
            } catch (Exception failure) {
                throw new IllegalArgumentException("Invalid Google SSE JSON", failure);
            }
            if (root.path("error").isObject()) {
                terminal = true;
                return List.of(new AssistantStreamEvent.Error(error(
                        root.path("error").path("message").asText("Google API error")
                )));
            }
            if (responseId == null && root.path("responseId").isTextual()) {
                responseId = root.path("responseId").asText();
            }
            ArrayList<AssistantStreamEvent> output = new ArrayList<>();
            JsonNode candidate = root.path("candidates").path(0);
            JsonNode parts = candidate.path("content").path("parts");
            if (parts.isArray()) {
                for (JsonNode part : parts) appendPart(part, output);
            }
            updateUsage(root.path("usageMetadata"));
            if (candidate.path("finishReason").isTextual()) {
                closeActive(output);
                rawStop = candidate.path("finishReason").asText();
                stop = mapStop(rawStop);
                if (stop == StopReason.STOP && blocks.stream()
                        .anyMatch(ToolCallContent.class::isInstance)) {
                    stop = StopReason.TOOL_USE;
                }
                terminal = true;
                output.add(stop == StopReason.ERROR
                        ? new AssistantStreamEvent.Error(error(
                                "Provider stopped with: " + rawStop
                        ))
                        : new AssistantStreamEvent.Done(snapshot()));
            }
            return output;
        }

        private void appendPart(
                JsonNode part,
                List<AssistantStreamEvent> output
        ) {
            if (part.has("text")) {
                boolean thinking = part.path("thought").asBoolean(false);
                ContentKind kind = thinking
                        ? ContentKind.THINKING : ContentKind.TEXT;
                String text = part.path("text").asText();
                String signature = part.path("thoughtSignature").isTextual()
                        ? part.path("thoughtSignature").asText() : null;
                if (activeKind != kind) {
                    closeActive(output);
                    activeIndex = blocks.size();
                    activeKind = kind;
                    blocks.add(thinking
                            ? new ThinkingContent("", signature)
                            : new TextContent("", signature));
                    output.add(new AssistantStreamEvent.ContentStart(
                            kind, activeIndex, snapshot()
                    ));
                }
                int index = activeIndex;
                if (thinking) {
                    ThinkingContent old = (ThinkingContent) blocks.get(index);
                    blocks.set(index, new ThinkingContent(
                            old.thinking() + text,
                            signature == null ? old.signature() : signature
                    ));
                } else {
                    TextContent old = (TextContent) blocks.get(index);
                    blocks.set(index, new TextContent(
                            old.text() + text,
                            signature == null ? old.signature() : signature
                    ));
                }
                if (!text.isEmpty()) output.add(
                        new AssistantStreamEvent.ContentDelta(
                                kind, index, text, snapshot()
                        )
                );
            }
            if (part.path("functionCall").isObject()) {
                closeActive(output);
                JsonNode function = part.path("functionCall");
                String id = function.path("id").asText();
                if (id.isBlank()) {
                    id = function.path("name").asText() + '_'
                            + System.currentTimeMillis() + '_'
                            + TOOL_IDS.incrementAndGet();
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> arguments = function.path("args").isObject()
                        ? mapper.convertValue(
                                function.path("args"), LinkedHashMap.class
                        ) : Map.of();
                int index = blocks.size();
                String signature = part.path("thoughtSignature").isTextual()
                        ? part.path("thoughtSignature").asText() : null;
                blocks.add(new ToolCallContent(
                        id, function.path("name").asText(), arguments, signature
                ));
                output.add(new AssistantStreamEvent.ContentStart(
                        ContentKind.TOOL_CALL, index, snapshot()
                ));
                output.add(new AssistantStreamEvent.ContentDelta(
                        ContentKind.TOOL_CALL, index,
                        function.path("args").toString(), snapshot()
                ));
                output.add(new AssistantStreamEvent.ContentEnd(
                        ContentKind.TOOL_CALL, index, snapshot()
                ));
            }
        }

        private void closeActive(List<AssistantStreamEvent> output) {
            if (activeIndex == null) return;
            output.add(new AssistantStreamEvent.ContentEnd(
                    activeKind, activeIndex, snapshot()
            ));
            activeIndex = null;
            activeKind = null;
        }

        private void updateUsage(JsonNode value) {
            if (!value.isObject()) return;
            long cache = value.path("cachedContentTokenCount").asLong();
            long thoughts = value.path("thoughtsTokenCount").asLong();
            long input = Math.max(0,
                    value.path("promptTokenCount").asLong() - cache
            );
            long output = value.path("candidatesTokenCount").asLong() + thoughts;
            usage = new Usage(
                    input, output, cache, 0, thoughts,
                    value.path("totalTokenCount").asLong(
                            input + output + cache
                    ), Cost.ZERO
            );
        }

        private AssistantMessage snapshot() {
            return new AssistantMessage(
                    blocks, model.api(), model.provider(), model.id(),
                    UsageCosts.calculate(model, usage), stop, null,
                    System.currentTimeMillis(), responseId, rawStop
            );
        }

        private AssistantMessage error(String message) {
            return new AssistantMessage(
                    blocks, model.api(), model.provider(), model.id(),
                    UsageCosts.calculate(model, usage), StopReason.ERROR,
                    message, System.currentTimeMillis(),
                    responseId, rawStop
            );
        }

        private static StopReason mapStop(String reason) {
            return switch (reason) {
                case "STOP" -> StopReason.STOP;
                case "MAX_TOKENS" -> StopReason.LENGTH;
                default -> StopReason.ERROR;
            };
        }
    }
}
