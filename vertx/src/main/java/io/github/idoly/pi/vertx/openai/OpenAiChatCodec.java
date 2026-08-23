package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Multi;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.ContentKind;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.Message;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ThinkingLevels;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.SseEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Structured request and streaming response codec for OpenAI-compatible Chat Completions APIs. */
public final class OpenAiChatCodec {
    private static final SseEvent END_OF_STREAM = new SseEvent("pi-eof", "", null, null);

    private final ObjectMapper mapper;
    private final OpenAiCompatibility compatibility;

    public OpenAiChatCodec(ObjectMapper mapper) {
        this(mapper, OpenAiCompatibility.DEFAULT);
    }

    public OpenAiChatCodec(ObjectMapper mapper, OpenAiCompatibility compatibility) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.compatibility = Objects.requireNonNull(compatibility, "compatibility");
    }

    public ObjectNode encodeRequest(
            Model model,
            ModelContext context,
            String thinkingLevel
    ) {
        ObjectNode root = mapper.createObjectNode();
        root.put("model", model.id());
        root.put("stream", true);
        if (compatibility.maxTokensField() == OpenAiCompatibility.MaxTokensField.MAX_TOKENS) {
            root.put("max_tokens", model.maxTokens());
        } else {
            root.put("max_completion_tokens", model.maxTokens());
        }
        if (compatibility.supportsStreamingUsage()) {
            root.putObject("stream_options").put("include_usage", true);
        }
        encodeReasoning(root, model, thinkingLevel);

        Map<String, OpenAiGrammar.Grammar> grammars = OpenAiGrammar.resolveAll(
                mapper, context.tools(), compatibility.supportsGrammarTools()
        );
        ArrayNode messages = root.putArray("messages");
        if (!context.systemPrompt().isBlank()) {
            boolean developer = model.reasoning() && compatibility.supportsDeveloperRole();
            messages.addObject()
                    .put("role", developer ? "developer" : "system")
                    .put("content", context.systemPrompt());
        }
        context.messages().forEach(message -> messages.add(encodeMessage(message, grammars)));
        if (!context.tools().isEmpty()) {
            ArrayNode tools = root.putArray("tools");
            context.tools().forEach(tool -> tools.add(encodeTool(tool, grammars.get(tool.name()))));
        }
        return root;
    }

    private void encodeReasoning(ObjectNode root, Model model, String thinkingLevel) {
        if (!model.reasoning()) {
            return;
        }
        String requested = thinkingLevel == null || thinkingLevel.isBlank()
                ? "off"
                : thinkingLevel;
        String level = ThinkingLevels.clamp(model, requested);
        boolean enabled = !level.equals("off");
        String effort = ThinkingLevels.providerValue(model, level);
        String offEffort = effort.equals("off") ? "none" : effort;
        switch (compatibility.reasoningFormat()) {
            case NONE -> { }
            case STANDARD -> {
                if (enabled && compatibility.supportsReasoningEffort()) {
                    root.put("reasoning_effort", effort);
                }
            }
            case QWEN -> {
                root.put("enable_thinking", enabled);
                if (enabled && compatibility.supportsReasoningEffort()) {
                    root.put("reasoning_effort", effort);
                }
            }
            case QWEN_CHAT_TEMPLATE -> root.putObject("chat_template_kwargs")
                    .put("enable_thinking", enabled)
                    .put("preserve_thinking", true);
            case ZAI -> {
                ObjectNode thinking = root.putObject("thinking");
                thinking.put("type", enabled ? "enabled" : "disabled");
                if (enabled) {
                    thinking.put("clear_thinking", false);
                }
                if (enabled && compatibility.supportsReasoningEffort()) {
                    root.put("reasoning_effort", effort);
                }
            }
            case DEEPSEEK -> {
                root.putObject("thinking").put("type", enabled ? "enabled" : "disabled");
                if (enabled && compatibility.supportsReasoningEffort()) {
                    root.put("reasoning_effort", effort);
                }
            }
            case OPENROUTER -> root.putObject("reasoning")
                    .put("effort", enabled ? effort : offEffort);
            case TOGETHER -> {
                root.putObject("reasoning").put("enabled", enabled);
                if (enabled && compatibility.supportsReasoningEffort()) {
                    root.put("reasoning_effort", effort);
                }
            }
            case STRING_THINKING -> root.put(
                    "thinking", enabled ? effort : offEffort
            );
        }
    }

    public Multi<AssistantStreamEvent> decode(Multi<SseEvent> source, Model model) {
        return decode(source, model, Map.of());
    }

    Multi<AssistantStreamEvent> decode(
            Multi<SseEvent> source,
            Model model,
            Map<String, OpenAiGrammar.Grammar> grammars
    ) {
        Decoder decoder = new Decoder(model, grammars);
        Multi<AssistantStreamEvent> start = Multi.createFrom().item(decoder.start());
        Multi<AssistantStreamEvent> updates = source
                .onCompletion().continueWith(END_OF_STREAM)
                .onItem().transformToMultiAndConcatenate(event ->
                        Multi.createFrom().iterable(decoder.accept(event))
                );
        return Multi.createBy().concatenating().streams(start, updates);
    }

    private ObjectNode encodeMessage(
            Message message,
            Map<String, OpenAiGrammar.Grammar> grammars
    ) {
        ObjectNode encoded = mapper.createObjectNode();
        if (message instanceof UserMessage user) {
            encoded.put("role", "user");
            encoded.set("content", encodeUserContent(user.content()));
        } else if (message instanceof AssistantMessage assistant) {
            encoded.put("role", "assistant");
            encodeAssistantContent(encoded, assistant.content(), grammars);
        } else if (message instanceof ToolResultMessage result) {
            encoded.put("role", "tool");
            encoded.put("tool_call_id", result.toolCallId());
            encoded.put("content", text(result.content()));
        } else {
            throw new IllegalArgumentException(
                    "OpenAI Chat Completions does not support message type " + message.getClass()
            );
        }
        return encoded;
    }

    private JsonNode encodeUserContent(List<ContentBlock> content) {
        if (content.size() == 1 && content.getFirst() instanceof TextContent text) {
            return mapper.getNodeFactory().textNode(text.text());
        }
        ArrayNode encoded = mapper.createArrayNode();
        for (ContentBlock block : content) {
            if (block instanceof TextContent text) {
                encoded.addObject().put("type", "text").put("text", text.text());
            } else if (block instanceof ImageContent image) {
                encoded.addObject()
                        .put("type", "image_url")
                        .putObject("image_url")
                        .put("url", "data:" + image.mimeType() + ";base64," + image.data());
            }
        }
        return encoded;
    }

    private void encodeAssistantContent(
            ObjectNode encoded,
            List<ContentBlock> content,
            Map<String, OpenAiGrammar.Grammar> grammars
    ) {
        StringBuilder text = new StringBuilder();
        StringBuilder reasoning = new StringBuilder();
        ArrayNode reasoningDetails = mapper.createArrayNode();
        String reasoningField = null;
        ArrayNode toolCalls = mapper.createArrayNode();
        for (ContentBlock block : content) {
            if (block instanceof TextContent value) {
                text.append(value.text());
            } else if (block instanceof ThinkingContent value) {
                reasoning.append(value.thinking());
                String signature = value.signature();
                if (signature != null && signature.startsWith("[")) {
                    try {
                        JsonNode parsed = mapper.readTree(signature);
                        if (parsed.isArray()) {
                            parsed.forEach(reasoningDetails::add);
                        }
                    } catch (JsonProcessingException failure) {
                        throw new IllegalArgumentException("Invalid reasoning details signature", failure);
                    }
                } else if (isReasoningField(signature) && reasoningField == null) {
                    reasoningField = signature;
                }
            } else if (block instanceof ToolCallContent call) {
                ObjectNode toolCall = toolCalls.addObject();
                toolCall.put("id", call.id());
                OpenAiGrammar.Grammar grammar = grammars.get(call.name());
                if (grammar == null) {
                    toolCall.put("type", "function");
                    ObjectNode function = toolCall.putObject("function");
                    function.put("name", call.name());
                    function.put("arguments", writeJson(call.arguments()));
                } else {
                    toolCall.put("type", "custom");
                    toolCall.putObject("custom")
                            .put("name", call.name())
                            .put("input", OpenAiGrammar.input(
                                    call.name(), call.arguments(), grammar.inputProperty()
                            ));
                }
            }
        }
        encoded.put("content", text.isEmpty() ? null : text.toString());
        if (!reasoningDetails.isEmpty()) {
            encoded.set("reasoning_details", reasoningDetails);
        } else if (!reasoning.isEmpty()) {
            encoded.put(reasoningField == null ? "reasoning_content" : reasoningField,
                    reasoning.toString());
        }
        if (!toolCalls.isEmpty()) {
            encoded.set("tool_calls", toolCalls);
        }
    }

    private static boolean isReasoningField(String value) {
        return "reasoning_content".equals(value)
                || "reasoning".equals(value)
                || "reasoning_text".equals(value);
    }

    private ObjectNode encodeTool(
            ToolDefinition tool,
            OpenAiGrammar.Grammar grammar
    ) {
        ObjectNode encoded = mapper.createObjectNode();
        if (grammar != null) {
            encoded.put("type", "custom");
            ObjectNode custom = encoded.putObject("custom");
            custom.put("name", tool.name());
            custom.put("description", tool.description());
            custom.putObject("format")
                    .put("type", "grammar")
                    .putObject("grammar")
                    .put("syntax", grammar.syntax())
                    .put("definition", grammar.definition());
            return encoded;
        }
        encoded.put("type", "function");
        ObjectNode function = encoded.putObject("function");
        function.put("name", tool.name());
        function.put("description", tool.description());
        OpenAiStrictJsonSchema.Resolution schema = OpenAiStrictJsonSchema.resolve(
                mapper, tool, compatibility.supportsStrictMode()
        );
        function.set("parameters", schema.parameters());
        if (schema.includeStrict()) {
            function.put("strict", schema.strict());
        }
        return encoded;
    }

    private String writeJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Unable to encode tool arguments", failure);
        }
    }

    private static String text(List<ContentBlock> content) {
        return content.stream()
                .filter(TextContent.class::isInstance)
                .map(TextContent.class::cast)
                .map(TextContent::text)
                .reduce((left, right) -> left + "\n" + right)
                .orElse("");
    }

    private final class Decoder {
        private final Model model;
        private final Map<String, OpenAiGrammar.Grammar> grammars;
        private final long timestamp = System.currentTimeMillis();
        private final List<Part> parts = new ArrayList<>();
        private final Map<Integer, ToolPart> tools = new LinkedHashMap<>();
        private Usage usage = Usage.ZERO;
        private StopReason stopReason = StopReason.PENDING;
        private String errorMessage;
        private String responseId;
        private String rawStopReason;
        private TextPart text;
        private boolean started;
        private boolean contentEnded;
        private boolean terminated;

        private Decoder(Model model, Map<String, OpenAiGrammar.Grammar> grammars) {
            this.model = model;
            this.grammars = Map.copyOf(grammars);
        }

        private AssistantStreamEvent start() {
            started = true;
            return new AssistantStreamEvent.Start(snapshot());
        }

        private List<AssistantStreamEvent> accept(SseEvent event) {
            if (terminated) {
                return List.of();
            }
            if (event == END_OF_STREAM || event.data().equals("[DONE]")) {
                return complete();
            }

            JsonNode root;
            try {
                root = mapper.readTree(event.data());
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException("Invalid OpenAI SSE JSON: " + event.data(), failure);
            }
            if (root.path("id").isTextual()) {
                responseId = root.path("id").asText();
            }
            JsonNode providerError = root.path("error");
            if (!providerError.isMissingNode() && !providerError.isNull()) {
                terminated = true;
                String message = providerError.path("message").asText(providerError.toString());
                return List.of(new AssistantStreamEvent.Error(errorMessage(message)));
            }
            updateUsage(root.path("usage"));
            JsonNode choices = root.path("choices");
            if (!choices.isArray() || choices.isEmpty()) {
                return List.of();
            }
            JsonNode choice = choices.get(0);
            if (!root.path("usage").isObject()) {
                updateUsage(choice.path("usage"));
            }
            List<AssistantStreamEvent> output = new ArrayList<>();
            ensureStarted(output);
            JsonNode delta = choice.path("delta");
            appendText(delta.path("content"), output);
            appendReasoning(delta, output);
            appendReasoningDetails(delta.path("reasoning_details"), output);
            appendToolCalls(delta.path("tool_calls"), output);
            JsonNode finishReason = choice.path("finish_reason");
            if (finishReason.isTextual()) {
                mapStopReason(finishReason.textValue());
            }
            return output;
        }

        private void ensureStarted(List<AssistantStreamEvent> output) {
            if (!started) {
                started = true;
                output.add(new AssistantStreamEvent.Start(snapshot()));
            }
        }

        private void appendText(JsonNode value, List<AssistantStreamEvent> output) {
            if (!value.isTextual() || value.textValue().isEmpty()) {
                return;
            }
            if (text == null) {
                text = new TextPart(ContentKind.TEXT);
                parts.add(text);
                output.add(new AssistantStreamEvent.ContentStart(
                        ContentKind.TEXT, parts.size() - 1, snapshot()
                ));
            }
            text.value.append(value.textValue());
            output.add(new AssistantStreamEvent.ContentDelta(
                    ContentKind.TEXT, parts.indexOf(text), value.textValue(), snapshot()
            ));
        }

        private void appendReasoning(JsonNode delta, List<AssistantStreamEvent> output) {
            String field = null;
            JsonNode value = null;
            for (String candidate : List.of("reasoning_content", "reasoning", "reasoning_text")) {
                JsonNode candidateValue = delta.path(candidate);
                if (candidateValue.isTextual() && !candidateValue.textValue().isEmpty()) {
                    field = candidate;
                    value = candidateValue;
                    break;
                }
            }
            if (field == null) {
                return;
            }
            TextPart thinking = ensureThinking(output);
            if (thinking.signature == null) {
                thinking.signature = field;
            }
            thinking.value.append(value.textValue());
            output.add(new AssistantStreamEvent.ContentDelta(
                    ContentKind.THINKING, parts.indexOf(thinking), value.textValue(), snapshot()
            ));
        }

        private void appendReasoningDetails(JsonNode details, List<AssistantStreamEvent> output) {
            if (!details.isArray()) {
                return;
            }
            TextPart thinking = null;
            ArrayNode preserved = mapper.createArrayNode();
            for (JsonNode detail : details) {
                if (!isReasoningDetail(detail)) {
                    continue;
                }
                if (thinking == null) {
                    thinking = ensureThinking(output);
                    if (thinking.signature != null && thinking.signature.startsWith("[")) {
                        try {
                            JsonNode existing = mapper.readTree(thinking.signature);
                            if (existing.isArray()) {
                                existing.forEach(preserved::add);
                            }
                        } catch (JsonProcessingException failure) {
                            throw new IllegalArgumentException("Invalid reasoning details signature", failure);
                        }
                    }
                }
                preserved.add(detail);
            }
            if (thinking != null) {
                thinking.signature = preserved.toString();
            }
        }

        private TextPart ensureThinking(List<AssistantStreamEvent> output) {
            TextPart thinking = parts.stream()
                    .filter(part -> part instanceof TextPart item && item.kind == ContentKind.THINKING)
                    .map(TextPart.class::cast)
                    .findFirst()
                    .orElse(null);
            if (thinking == null) {
                thinking = new TextPart(ContentKind.THINKING);
                parts.add(thinking);
                output.add(new AssistantStreamEvent.ContentStart(
                        ContentKind.THINKING, parts.size() - 1, snapshot()
                ));
            }
            return thinking;
        }

        private boolean isReasoningDetail(JsonNode detail) {
            if (!detail.isObject()
                    || detail.has("id") && !detail.path("id").isTextual() && !detail.path("id").isNull()
                    || detail.has("format") && !detail.path("format").isTextual()
                    || detail.has("index") && !detail.path("index").canConvertToInt()) {
                return false;
            }
            return switch (detail.path("type").asText()) {
                case "reasoning.summary" -> detail.path("summary").isTextual();
                case "reasoning.encrypted" -> detail.path("data").isTextual();
                case "reasoning.text" -> detail.path("text").isTextual()
                        && (!detail.has("signature")
                        || detail.path("signature").isTextual()
                        || detail.path("signature").isNull());
                default -> false;
            };
        }

        private void appendToolCalls(JsonNode values, List<AssistantStreamEvent> output) {
            if (!values.isArray()) {
                return;
            }
            for (JsonNode value : values) {
                int providerIndex = value.path("index").asInt(tools.size());
                ToolPart tool = tools.get(providerIndex);
                if (tool == null) {
                    tool = new ToolPart(providerIndex);
                    tools.put(providerIndex, tool);
                    parts.add(tool);
                    updateToolIdentity(tool, value);
                    output.add(new AssistantStreamEvent.ContentStart(
                            ContentKind.TOOL_CALL, parts.size() - 1, snapshot()
                    ));
                } else {
                    updateToolIdentity(tool, value);
                }
                JsonNode arguments = value.path("function").path("arguments");
                JsonNode customInput = value.path("custom").path("input");
                String argumentsDelta;
                if (customInput.isTextual()) {
                    argumentsDelta = tool.appendCustom(customInput.asText(), false);
                } else {
                    argumentsDelta = arguments.isTextual() ? arguments.textValue() : "";
                    tool.arguments.append(argumentsDelta);
                }
                if (argumentsDelta != null) {
                    output.add(new AssistantStreamEvent.ContentDelta(
                            ContentKind.TOOL_CALL,
                            parts.indexOf(tool),
                            argumentsDelta,
                            snapshot()
                    ));
                }
            }
        }

        private void updateToolIdentity(ToolPart tool, JsonNode value) {
            if (tool.id.isEmpty() && value.path("id").isTextual()) {
                tool.id.append(value.path("id").textValue());
            }
            JsonNode name = value.path("function").path("name");
            if (!name.isTextual()) {
                name = value.path("custom").path("name");
            }
            if (tool.name.isEmpty() && name.isTextual()) {
                tool.name.append(name.textValue());
                OpenAiGrammar.Grammar grammar = grammars.get(name.textValue());
                if (value.path("custom").isObject()) {
                    tool.customProperty = grammar == null ? "input" : grammar.inputProperty();
                }
            }
        }

        private void endContent(List<AssistantStreamEvent> output) {
            if (contentEnded) {
                return;
            }
            for (ToolPart tool : tools.values()) {
                String delta = tool.closeCustom();
                if (delta != null) {
                    output.add(new AssistantStreamEvent.ContentDelta(
                            ContentKind.TOOL_CALL, parts.indexOf(tool), delta, snapshot()
                    ));
                }
            }
            validateToolArguments();
            for (int index = 0; index < parts.size(); index++) {
                output.add(new AssistantStreamEvent.ContentEnd(
                        parts.get(index).kind(), index, snapshot()
                ));
            }
            contentEnded = true;
        }

        private List<AssistantStreamEvent> complete() {
            List<AssistantStreamEvent> output = new ArrayList<>();
            if (!started) {
                ensureStarted(output);
            }
            if (stopReason == StopReason.PENDING) {
                stopReason = StopReason.ERROR;
                errorMessage = "Stream ended without finish_reason";
            }
            endContent(output);
            terminated = true;
            if (stopReason == StopReason.ERROR) {
                output.add(new AssistantStreamEvent.Error(errorMessage(
                        errorMessage == null ? "Provider returned an error stop reason" : errorMessage
                )));
            } else {
                output.add(new AssistantStreamEvent.Done(snapshot()));
            }
            return output;
        }

        private void validateToolArguments() {
            for (ToolPart tool : tools.values()) {
                parseArguments(tool.arguments.toString());
            }
        }

        private AssistantMessage snapshot() {
            List<ContentBlock> content = parts.stream().map(Part::content).toList();
            return new AssistantMessage(
                    content,
                    model.api(),
                    model.provider(),
                    model.id(),
                    usage,
                    stopReason,
                    errorMessage,
                    timestamp,
                    responseId,
                    rawStopReason
            );
        }

        private AssistantMessage errorMessage(String message) {
            return new AssistantMessage(
                    parts.stream().map(Part::content).toList(),
                    model.api(), model.provider(), model.id(), usage,
                    StopReason.ERROR, message, timestamp, responseId, rawStopReason
            );
        }

        private void updateUsage(JsonNode node) {
            if (!node.isObject()) {
                return;
            }
            long input = node.path("prompt_tokens").asLong(0);
            long output = node.path("completion_tokens").asLong(0);
            long total = node.path("total_tokens").asLong(input + output);
            long cacheRead = node.path("prompt_tokens_details").path("cached_tokens").asLong(0);
            long cacheWrite = node.path("prompt_tokens_details").path("cache_write_tokens").asLong(0);
            long uncachedInput = Math.max(0, input - cacheRead - cacheWrite);
            long reasoning = node.path("completion_tokens_details")
                    .path("reasoning_tokens").asLong(
                            node.path("reasoning_tokens").asLong(0)
                    );
            usage = new Usage(
                    uncachedInput, output, cacheRead, cacheWrite, reasoning, total, Cost.ZERO
            );
        }

        private Map<String, Object> parseArguments(String json) {
            if (json.isBlank()) {
                return Map.of();
            }
            try {
                JsonNode parsed = mapper.readTree(json);
                if (!parsed.isObject()) {
                    throw new IllegalArgumentException("Tool arguments must be a JSON object: " + json);
                }
                return mapper.convertValue(parsed, mapper.getTypeFactory().constructMapType(
                        LinkedHashMap.class, String.class, Object.class
                ));
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException("Invalid streamed tool arguments: " + json, failure);
            }
        }

        private void mapStopReason(String value) {
            rawStopReason = value;
            stopReason = switch (value) {
                case "stop" -> StopReason.STOP;
                case "length" -> StopReason.LENGTH;
                case "tool_calls", "function_call" -> StopReason.TOOL_USE;
                default -> StopReason.ERROR;
            };
            if (stopReason == StopReason.ERROR) {
                errorMessage = "Provider finish_reason: " + value;
            }
        }

        private sealed interface Part permits TextPart, ToolPart {
            ContentKind kind();

            ContentBlock content();
        }

        private final class TextPart implements Part {
            private final ContentKind kind;
            private final StringBuilder value = new StringBuilder();
            private String signature;

            private TextPart(ContentKind kind) {
                this.kind = kind;
            }

            @Override
            public ContentKind kind() {
                return kind;
            }

            @Override
            public ContentBlock content() {
                return kind == ContentKind.TEXT
                        ? new TextContent(value.toString())
                        : new ThinkingContent(value.toString(), signature);
            }
        }

        private final class ToolPart implements Part {
            private final int providerIndex;
            private final StringBuilder id = new StringBuilder();
            private final StringBuilder name = new StringBuilder();
            private final StringBuilder arguments = new StringBuilder();
            private String customProperty;
            private String customInput = "";
            private boolean customStarted;
            private boolean customClosed;

            private ToolPart(int providerIndex) {
                this.providerIndex = providerIndex;
            }

            @Override
            public ContentKind kind() {
                return ContentKind.TOOL_CALL;
            }

            private String appendCustom(String inputDelta, boolean close) {
                if (customProperty == null) {
                    return null;
                }
                if (customClosed) {
                    if (close && inputDelta.isEmpty()) {
                        return null;
                    }
                    throw new IllegalArgumentException(
                            "grammar tool input for property \"" + customProperty
                                    + "\" changed after it was closed"
                    );
                }
                if (!close && inputDelta.isEmpty()) {
                    return null;
                }
                StringBuilder delta = new StringBuilder();
                if (!customStarted) {
                    delta.append('{').append(writeJson(customProperty)).append(":\"");
                    customStarted = true;
                }
                String escaped = writeJson(inputDelta);
                delta.append(escaped, 1, escaped.length() - 1);
                customInput += inputDelta;
                if (close) {
                    delta.append("\"}");
                    customClosed = true;
                }
                arguments.append(delta);
                return delta.toString();
            }

            private String closeCustom() {
                return customProperty == null || customClosed
                        ? null
                        : appendCustom("", true);
            }

            @Override
            public ContentBlock content() {
                Map<String, Object> parsed;
                try {
                    parsed = parseArguments(arguments.toString());
                } catch (IllegalArgumentException incomplete) {
                    parsed = Map.of();
                }
                String callId = id.isEmpty() ? "call_" + providerIndex : id.toString();
                return new ToolCallContent(callId, name.toString(), parsed);
            }
        }
    }
}
