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
import io.github.idoly.pi.ai.UsageCosts;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.SseEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** OpenAI Responses API request and stream codec using Jackson's structured tree model. */
public final class OpenAiResponsesCodec {
    private static final SseEvent END_OF_STREAM = new SseEvent("pi-eof", "", null, null);

    private static final Set<String> NORMALIZED_TOOL_ID_PROVIDERS = Set.of(
            "openai", "openai-codex", "opencode"
    );

    private final ObjectMapper mapper;
    private final OpenAiResponsesCompatibility compatibility;

    public OpenAiResponsesCodec(ObjectMapper mapper) {
        this(mapper, OpenAiResponsesCompatibility.DEFAULT);
    }

    public OpenAiResponsesCodec(
            ObjectMapper mapper,
            OpenAiResponsesCompatibility compatibility
    ) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.compatibility = Objects.requireNonNull(compatibility, "compatibility");
    }

    public ObjectNode encodeRequest(Model model, ModelContext context, String thinkingLevel) {
        Objects.requireNonNull(model, "model");
        ObjectNode root = mapper.createObjectNode();
        root.put("model", model.id());
        root.put("stream", true);
        root.put("store", false);
        root.put("max_output_tokens", Math.max(16, model.maxTokens()));
        Map<String, OpenAiGrammar.Grammar> grammars = OpenAiGrammar.resolveAll(
                mapper, context.tools(), compatibility.supportsGrammarTools()
        );
        ArrayNode input = root.putArray("input");
        if (!context.systemPrompt().isBlank()) {
            boolean developer = model.reasoning() && compatibility.supportsDeveloperRole();
            input.add(message(developer ? "developer" : "system", context.systemPrompt()));
        }
        Map<String, String> normalizedToolIds = new LinkedHashMap<>();
        for (int index = 0; index < context.messages().size(); index++) {
            encodeMessage(
                    model, context.messages().get(index), input, index,
                    normalizedToolIds, grammars
            );
        }
        if (!context.tools().isEmpty()) {
            ArrayNode tools = root.putArray("tools");
            context.tools().forEach(tool -> tools.add(encodeTool(tool, grammars.get(tool.name()))));
        }
        if (model.reasoning()) {
            String requested = thinkingLevel == null || thinkingLevel.isBlank()
                    ? "off"
                    : thinkingLevel;
            String level = ThinkingLevels.clamp(model, requested);
            if (!level.equals("off")) {
                root.putObject("reasoning")
                        .put("effort", ThinkingLevels.providerValue(model, level))
                        .put("summary", "auto");
                root.putArray("include").add("reasoning.encrypted_content");
            } else {
                String offEffort = model.thinkingLevelMap() != null
                        && model.thinkingLevelMap().defines("off")
                        ? model.thinkingLevelMap().providerValue("off")
                        : compatibility.offReasoningEffort();
                if (offEffort != null) {
                    root.putObject("reasoning").put("effort", offEffort);
                }
            }
        }
        return root;
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

    private void encodeMessage(
            Model model,
            Message message,
            ArrayNode input,
            int messageIndex,
            Map<String, String> normalizedToolIds,
            Map<String, OpenAiGrammar.Grammar> grammars
    ) {
        if (message instanceof UserMessage user) {
            ObjectNode encoded = contentMessage("user", user.content());
            if (!encoded.path("content").isEmpty()) {
                input.add(encoded);
            }
        } else if (message instanceof AssistantMessage assistant) {
            int textBlockIndex = 0;
            for (ContentBlock block : assistant.content()) {
                if (block instanceof TextContent text && !text.text().isEmpty()) {
                    String id = textBlockIndex == 0
                            ? "msg_pi_" + messageIndex
                            : "msg_pi_" + messageIndex + "_" + textBlockIndex;
                    input.add(outputMessage(id, text.text()));
                    textBlockIndex++;
                } else if (block instanceof ThinkingContent thinking
                        && thinking.signature() != null) {
                    input.add(parseReasoningSignature(thinking.signature()));
                } else if (block instanceof ToolCallContent call) {
                    OpenAiGrammar.Grammar grammar = grammars.get(call.name());
                    String normalizedId = grammar == null
                            ? normalizeToolCallId(model, assistant, call.id())
                            : normalizeGrammarToolCallId(call.id());
                    normalizedToolIds.put(call.id(), normalizedId);
                    input.add(grammar == null
                            ? functionCall(model, assistant, call, normalizedId)
                            : customToolCall(call, normalizedId, grammar));
                }
            }
        } else if (message instanceof ToolResultMessage result) {
            String normalizedId = normalizedToolIds.getOrDefault(
                    result.toolCallId(), normalizeIdPart(result.toolCallId())
            );
            boolean custom = grammars.containsKey(result.toolName());
            ObjectNode encoded = input.addObject()
                    .put("type", custom ? "custom_tool_call_output" : "function_call_output")
                    .put("call_id", callId(normalizedId));
            encoded.set("output", toolResultOutput(model, result.content()));
        }
    }

    private ObjectNode message(String role, String text) {
        return mapper.createObjectNode().put("role", role).put("content", text);
    }

    private ObjectNode outputMessage(String id, String text) {
        ObjectNode message = mapper.createObjectNode()
                .put("type", "message")
                .put("id", id)
                .put("role", "assistant")
                .put("status", "completed");
        message.putArray("content").addObject()
                .put("type", "output_text")
                .put("text", text)
                .putArray("annotations");
        return message;
    }

    private ObjectNode contentMessage(String role, List<ContentBlock> content) {
        ObjectNode message = mapper.createObjectNode().put("role", role);
        ArrayNode encoded = message.putArray("content");
        for (ContentBlock block : content) {
            if (block instanceof TextContent text) {
                encoded.addObject().put("type", "input_text").put("text", text.text());
            } else if (block instanceof ImageContent image) {
                encoded.addObject()
                        .put("type", "input_image")
                        .put("detail", "auto")
                        .put("image_url", "data:" + image.mimeType() + ";base64," + image.data());
            }
        }
        return message;
    }

    private JsonNode parseReasoningSignature(String signature) {
        try {
            JsonNode item = mapper.readTree(signature);
            if (!item.isObject() || !item.path("type").asText().equals("reasoning")) {
                throw new IllegalArgumentException("Responses reasoning signature must be a reasoning item");
            }
            return item;
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Invalid Responses reasoning signature", failure);
        }
    }

    private ObjectNode functionCall(
            Model model,
            AssistantMessage source,
            ToolCallContent call,
            String normalizedId
    ) {
        String[] ids = splitToolCallId(normalizedId);
        ObjectNode encoded = mapper.createObjectNode()
                .put("type", "function_call")
                .put("call_id", ids[0])
                .put("name", call.name())
                .put("arguments", writeJson(call.arguments()));
        boolean differentModel = source.provider().equals(model.provider())
                && source.api().equals(model.api())
                && !source.model().equals(model.id());
        if (ids[1] != null && ids[1].startsWith("fc_") && !differentModel) {
            encoded.put("id", ids[1]);
        }
        return encoded;
    }

    private String normalizeToolCallId(Model model, AssistantMessage source, String id) {
        if (!compatibility.normalizeToolCallIds()
                || !NORMALIZED_TOOL_ID_PROVIDERS.contains(model.provider())
                || !id.contains("|")) {
            return normalizeIdPart(id);
        }
        String[] ids = splitToolCallId(id);
        String callId = normalizeIdPart(ids[0]);
        boolean foreign = !source.provider().equals(model.provider())
                || !source.api().equals(model.api());
        String itemId = foreign
                ? "fc_" + shortHash(ids[1])
                : normalizeIdPart(ids[1]);
        if (!itemId.startsWith("fc_")) {
            itemId = normalizeIdPart("fc_" + itemId);
        }
        return callId + "|" + itemId;
    }

    private static String normalizeIdPart(String value) {
        String normalized = value.replaceAll("[^a-zA-Z0-9_-]", "_");
        if (normalized.length() > 64) {
            normalized = normalized.substring(0, 64);
        }
        return normalized.replaceAll("_+$", "");
    }

    static String shortHash(String value) {
        int first = 0xdeadbeef;
        int second = 0x41c6ce57;
        for (int index = 0; index < value.length(); index++) {
            int character = value.charAt(index);
            first = (first ^ character) * 0x9e3779b1;
            second = (second ^ character) * 0x5f356495;
        }
        first = ((first ^ (first >>> 16)) * 0x85ebca6b)
                ^ ((second ^ (second >>> 13)) * 0xc2b2ae35);
        second = ((second ^ (second >>> 16)) * 0x85ebca6b)
                ^ ((first ^ (first >>> 13)) * 0xc2b2ae35);
        return Long.toString(Integer.toUnsignedLong(second), 36)
                + Long.toString(Integer.toUnsignedLong(first), 36);
    }

    private ObjectNode customToolCall(
            ToolCallContent call,
            String normalizedId,
            OpenAiGrammar.Grammar grammar
    ) {
        String[] ids = splitToolCallId(normalizedId);
        ObjectNode encoded = mapper.createObjectNode()
                .put("type", "custom_tool_call")
                .put("call_id", ids[0])
                .put("name", call.name())
                .put("input", OpenAiGrammar.input(
                        call.name(), call.arguments(), grammar.inputProperty()
                ));
        if (ids[1] != null) {
            encoded.put("id", ids[1]);
        }
        return encoded;
    }

    private static String normalizeGrammarToolCallId(String id) {
        String[] ids = splitToolCallId(id);
        String callId = normalizeIdPart(ids[0]);
        return ids[1] == null ? callId : callId + "|" + normalizeIdPart(ids[1]);
    }

    private JsonNode toolResultOutput(Model model, List<ContentBlock> content) {
        String resultText = text(content);
        List<ImageContent> images = content.stream()
                .filter(ImageContent.class::isInstance)
                .map(ImageContent.class::cast)
                .toList();
        if (images.isEmpty() || !model.input().contains("image")) {
            String output = !resultText.isEmpty()
                    ? resultText
                    : images.isEmpty() ? "(no tool output)" : "(see attached image)";
            return mapper.getNodeFactory().textNode(output);
        }
        ArrayNode output = mapper.createArrayNode();
        if (!resultText.isEmpty()) {
            output.addObject().put("type", "input_text").put("text", resultText);
        }
        for (ImageContent image : images) {
            output.addObject()
                    .put("type", "input_image")
                    .put("detail", "auto")
                    .put("image_url", "data:" + image.mimeType() + ";base64," + image.data());
        }
        return output;
    }

    private ObjectNode encodeTool(
            ToolDefinition definition,
            OpenAiGrammar.Grammar grammar
    ) {
        ObjectNode encoded = mapper.createObjectNode();
        encoded.put("type", grammar == null ? "function" : "custom");
        encoded.put("name", definition.name());
        encoded.put("description", definition.description());
        if (grammar != null) {
            encoded.putObject("format")
                    .put("type", "grammar")
                    .put("syntax", grammar.syntax())
                    .put("definition", grammar.definition());
            return encoded;
        }
        OpenAiStrictJsonSchema.Resolution schema = OpenAiStrictJsonSchema.resolve(
                mapper, definition, compatibility.supportsStrictMode()
        );
        encoded.set("parameters", schema.parameters());
        if (schema.includeStrict()) {
            encoded.put("strict", schema.strict());
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

    private static String callId(String id) {
        return splitToolCallId(id)[0];
    }

    private static String[] splitToolCallId(String id) {
        int separator = id.indexOf('|');
        return separator < 0
                ? new String[]{id, null}
                : new String[]{id.substring(0, separator), id.substring(separator + 1)};
    }

    private final class Decoder {
        private final Model model;
        private final Map<String, OpenAiGrammar.Grammar> grammars;
        private final long timestamp = System.currentTimeMillis();
        private final List<Part> parts = new ArrayList<>();
        private final Map<Long, Part> slots = new LinkedHashMap<>();
        private final Map<String, TextPart> reasoningById = new LinkedHashMap<>();
        private Usage usage = Usage.ZERO;
        private StopReason stopReason = StopReason.PENDING;
        private String errorMessage;
        private String responseId;
        private String rawStopReason;
        private boolean terminal;

        private Decoder(Model model, Map<String, OpenAiGrammar.Grammar> grammars) {
            this.model = model;
            this.grammars = Map.copyOf(grammars);
        }

        private AssistantStreamEvent start() {
            return new AssistantStreamEvent.Start(snapshot());
        }

        private List<AssistantStreamEvent> accept(SseEvent frame) {
            if (terminal) {
                return List.of();
            }
            if (frame == END_OF_STREAM || frame.data().equals("[DONE]")) {
                terminal = true;
                if (stopReason == StopReason.PENDING) {
                    return List.of(new AssistantStreamEvent.Error(error(
                            "OpenAI Responses stream ended before a terminal response event"
                    )));
                }
                return List.of();
            }

            JsonNode event;
            try {
                event = mapper.readTree(frame.data());
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException("Invalid OpenAI Responses event", failure);
            }
            String type = event.path("type").asText();
            List<AssistantStreamEvent> output = new ArrayList<>();
            switch (type) {
                case "response.created" -> responseId = event.path("response").path("id").asText(null);
                case "response.output_item.added" -> createSlot(
                        event.path("output_index").asLong(), event.path("item"), output
                );
                case "response.output_text.delta" -> appendDelta(
                        event.path("output_index").asLong(), ContentKind.TEXT,
                        event.path("delta").asText(), output
                );
                case "response.refusal.delta" -> appendDelta(
                        event.path("output_index").asLong(), ContentKind.TEXT,
                        event.path("delta").asText(), output
                );
                case "response.reasoning_summary_text.delta", "response.reasoning_text.delta" -> appendDelta(
                        event.path("output_index").asLong(), ContentKind.THINKING,
                        event.path("delta").asText(), output
                );
                case "response.reasoning_summary_part.done" -> appendDelta(
                        event.path("output_index").asLong(), ContentKind.THINKING,
                        "\n\n", output
                );
                case "response.function_call_arguments.delta" -> appendDelta(
                        event.path("output_index").asLong(), ContentKind.TOOL_CALL,
                        event.path("delta").asText(), output
                );
                case "response.function_call_arguments.done" -> finishArguments(
                        event.path("output_index").asLong(), event.path("arguments").asText(), output
                );
                case "response.custom_tool_call_input.delta" -> appendCustomInput(
                        event.path("output_index").asLong(), event.path("delta").asText(),
                        false, output
                );
                case "response.custom_tool_call_input.done" -> setCustomInput(
                        event.path("output_index").asLong(), event.path("input").asText(), output
                );
                case "response.output_item.done" -> finishSlot(
                        event.path("output_index").asLong(), event.path("item"), output
                );
                case "response.completed" -> complete(event.path("response"), output, false);
                case "response.incomplete" -> complete(event.path("response"), output, true);
                case "error" -> {
                    terminal = true;
                    String prefix = event.path("code").isTextual()
                            ? event.path("code").asText() + ": "
                            : "";
                    output.add(new AssistantStreamEvent.Error(error(
                            prefix + event.path("message").asText("OpenAI Responses error")
                    )));
                }
                case "response.failed" -> {
                    terminal = true;
                    JsonNode failedResponse = event.path("response");
                    rawStopReason = failedResponse.path("status").asText("failed");
                    JsonNode providerError = failedResponse.path("error");
                    output.add(new AssistantStreamEvent.Error(error(
                            providerError.path("code").asText("unknown") + ": "
                                    + providerError.path("message").asText("OpenAI response failed")
                    )));
                }
                default -> {
                    // Other Responses events carry metadata not represented in the Core content model.
                }
            }
            return output;
        }

        private void createSlot(long outputIndex, JsonNode item, List<AssistantStreamEvent> output) {
            if (slots.containsKey(outputIndex)) {
                return;
            }
            Part part = switch (item.path("type").asText()) {
                case "message" -> {
                    applyMessagePhase(item);
                    yield new TextPart(ContentKind.TEXT);
                }
                case "reasoning" -> new TextPart(ContentKind.THINKING);
                case "function_call" -> new ToolPart(
                        item.path("call_id").asText()
                                + (item.path("id").isTextual() ? "|" + item.path("id").asText() : ""),
                        item.path("name").asText(),
                        item.path("arguments").asText("")
                );
                case "custom_tool_call" -> {
                    String name = item.path("name").asText();
                    OpenAiGrammar.Grammar grammar = grammars.get(name);
                    yield new CustomToolPart(
                            item.path("call_id").asText()
                                    + (item.path("id").isTextual()
                                    ? "|" + item.path("id").asText() : ""),
                            name,
                            grammar == null ? "input" : grammar.inputProperty()
                    );
                }
                default -> null;
            };
            if (part == null) {
                return;
            }
            slots.put(outputIndex, part);
            parts.add(part);
            output.add(new AssistantStreamEvent.ContentStart(
                    part.kind(), parts.size() - 1, snapshot()
            ));
        }

        private void appendDelta(
                long outputIndex,
                ContentKind expected,
                String delta,
                List<AssistantStreamEvent> output
        ) {
            Part part = slots.get(outputIndex);
            if (part == null || part.kind() != expected) {
                return;
            }
            part.append(delta);
            output.add(new AssistantStreamEvent.ContentDelta(
                    expected, parts.indexOf(part), delta, snapshot()
            ));
        }

        private void finishArguments(
                long outputIndex,
                String arguments,
                List<AssistantStreamEvent> output
        ) {
            Part part = slots.get(outputIndex);
            if (!(part instanceof ToolPart tool)) {
                return;
            }
            String previous = tool.arguments.toString();
            tool.arguments.setLength(0);
            tool.arguments.append(arguments);
            if (arguments.startsWith(previous) && arguments.length() > previous.length()) {
                String delta = arguments.substring(previous.length());
                output.add(new AssistantStreamEvent.ContentDelta(
                        ContentKind.TOOL_CALL, parts.indexOf(tool), delta, snapshot()
                ));
            }
        }

        private void appendCustomInput(
                long outputIndex,
                String delta,
                boolean close,
                List<AssistantStreamEvent> output
        ) {
            Part part = slots.get(outputIndex);
            if (part instanceof CustomToolPart custom) {
                custom.update(custom.input + delta, close, output);
            }
        }

        private void setCustomInput(
                long outputIndex,
                String input,
                List<AssistantStreamEvent> output
        ) {
            Part part = slots.get(outputIndex);
            if (part instanceof CustomToolPart custom) {
                custom.update(input, true, output);
            }
        }

        private void finishSlot(long outputIndex, JsonNode item, List<AssistantStreamEvent> output) {
            Part part = slots.remove(outputIndex);
            if (part == null) {
                createSlot(outputIndex, item, output);
                part = slots.remove(outputIndex);
            }
            if (part == null) {
                return;
            }
            if (part instanceof ToolPart tool && item.path("type").asText().equals("function_call")) {
                tool.arguments.setLength(0);
                tool.arguments.append(item.path("arguments").asText("{}"));
                tool.validate();
            } else if (part instanceof TextPart thinking
                    && thinking.kind == ContentKind.THINKING
                    && item.path("type").asText().equals("reasoning")) {
                String finalThinking = joinedText(item.path("summary"));
                if (finalThinking.isEmpty()) {
                    finalThinking = joinedText(item.path("content"));
                }
                if (!finalThinking.isEmpty()) {
                    thinking.replace(finalThinking);
                }
                thinking.signature = item.toString();
                if (item.path("id").isTextual()) {
                    reasoningById.put(item.path("id").asText(), thinking);
                }
            } else if (part instanceof TextPart text
                    && text.kind == ContentKind.TEXT
                    && item.path("type").asText().equals("message")) {
                applyMessagePhase(item);
                text.replace(joinedMessageContent(item.path("content")));
            } else if (part instanceof CustomToolPart custom
                    && item.path("type").asText().equals("custom_tool_call")) {
                custom.update(item.path("input").asText(custom.input), true, output);
            }
            output.add(new AssistantStreamEvent.ContentEnd(
                    part.kind(), parts.indexOf(part), snapshot()
            ));
        }

        private void complete(JsonNode response, List<AssistantStreamEvent> output, boolean incomplete) {
            backfillReasoningSignatures(response.path("output"));
            updateUsage(response.path("usage"));
            responseId = response.path("id").asText(responseId);
            String status = response.path("status").asText(incomplete ? "incomplete" : "completed");
            if (incomplete) {
                String reason = response.path("incomplete_details").path("reason").asText(null);
                rawStopReason = reason == null ? status : status + "." + reason;
                if (reason == null) {
                    stopReason = StopReason.ERROR;
                    errorMessage = "Response incomplete without a provider reason";
                } else if (reason.equals("max_output_tokens")) {
                    stopReason = StopReason.LENGTH;
                } else {
                    stopReason = StopReason.ERROR;
                    errorMessage = "Response incomplete: " + reason;
                }
            } else {
                rawStopReason = status;
                stopReason = parts.stream().anyMatch(
                        part -> part.kind() == ContentKind.TOOL_CALL
                ) ? StopReason.TOOL_USE : StopReason.STOP;
            }
            terminal = true;
            if (stopReason == StopReason.ERROR) {
                output.add(new AssistantStreamEvent.Error(error(errorMessage)));
            } else {
                output.add(new AssistantStreamEvent.Done(snapshot()));
            }
        }

        private void applyMessagePhase(JsonNode item) {
            if (item.path("phase").asText().equals("final_answer")) {
                stopReason = StopReason.STOP;
            }
        }

        private String joinedText(JsonNode values) {
            if (!values.isArray()) {
                return "";
            }
            List<String> text = new ArrayList<>();
            for (JsonNode value : values) {
                if (value.path("text").isTextual()) {
                    text.add(value.path("text").asText());
                }
            }
            return String.join("\n\n", text);
        }

        private String joinedMessageContent(JsonNode values) {
            if (!values.isArray()) {
                return "";
            }
            StringBuilder text = new StringBuilder();
            for (JsonNode value : values) {
                JsonNode content = value.path("type").asText().equals("output_text")
                        ? value.path("text")
                        : value.path("refusal");
                if (content.isTextual()) {
                    text.append(content.asText());
                }
            }
            return text.toString();
        }

        private void backfillReasoningSignatures(JsonNode responseOutput) {
            if (!responseOutput.isArray()) {
                return;
            }
            for (JsonNode item : responseOutput) {
                if (!item.path("type").asText().equals("reasoning")
                        || !item.path("id").isTextual()
                        || !item.path("encrypted_content").isTextual()) {
                    continue;
                }
                TextPart thinking = reasoningById.get(item.path("id").asText());
                if (thinking == null || thinking.signature == null) {
                    continue;
                }
                try {
                    ObjectNode stored = (ObjectNode) mapper.readTree(thinking.signature);
                    if (!stored.path("encrypted_content").isTextual()) {
                        stored.set("encrypted_content", item.path("encrypted_content"));
                        thinking.signature = stored.toString();
                    }
                } catch (JsonProcessingException failure) {
                    throw new IllegalArgumentException("Invalid stored reasoning signature", failure);
                }
            }
        }

        private void updateUsage(JsonNode value) {
            if (!value.isObject()) {
                return;
            }
            long cacheRead = value.path("input_tokens_details").path("cached_tokens").asLong(0);
            long cacheWrite = value.path("input_tokens_details").path("cache_write_tokens").asLong(0);
            long totalInput = value.path("input_tokens").asLong(0);
            usage = new Usage(
                    Math.max(0, totalInput - cacheRead - cacheWrite),
                    value.path("output_tokens").asLong(0),
                    cacheRead,
                    cacheWrite,
                    value.path("output_tokens_details").path("reasoning_tokens").asLong(0),
                    value.path("total_tokens").asLong(0),
                    Cost.ZERO
            );
        }

        private AssistantMessage snapshot() {
            return new AssistantMessage(
                    parts.stream().map(Part::content).toList(),
                    model.api(), model.provider(), model.id(),
                    UsageCosts.calculate(model, usage), stopReason, errorMessage, timestamp, responseId, rawStopReason
            );
        }

        private AssistantMessage error(String message) {
            stopReason = StopReason.ERROR;
            errorMessage = message;
            return snapshot();
        }

        private sealed interface Part permits TextPart, ToolPart, CustomToolPart {
            ContentKind kind();

            void append(String delta);

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
            public void append(String delta) {
                value.append(delta);
            }

            private void replace(String content) {
                value.setLength(0);
                value.append(content);
            }

            @Override
            public ContentBlock content() {
                return kind == ContentKind.TEXT
                        ? new TextContent(value.toString())
                        : new ThinkingContent(value.toString(), signature);
            }
        }

        private final class CustomToolPart implements Part {
            private final String id;
            private final String name;
            private final String property;
            private String input = "";
            private boolean started;
            private boolean closed;

            private CustomToolPart(String id, String name, String property) {
                this.id = id;
                this.name = name;
                this.property = property;
            }

            @Override
            public ContentKind kind() {
                return ContentKind.TOOL_CALL;
            }

            @Override
            public void append(String delta) {
                input += delta;
            }

            private void update(
                    String nextInput,
                    boolean close,
                    List<AssistantStreamEvent> output
            ) {
                if (closed) {
                    if (close && nextInput.equals(input)) {
                        return;
                    }
                    throw new IllegalArgumentException(
                            "grammar tool input for property \"" + property
                                    + "\" changed after it was closed"
                    );
                }
                if (!nextInput.startsWith(input)) {
                    throw new IllegalArgumentException(
                            "grammar tool input for property \"" + property
                                    + "\" changed non-monotonically"
                    );
                }
                String inputDelta = nextInput.substring(input.length());
                if (!close && inputDelta.isEmpty()) {
                    return;
                }
                StringBuilder delta = new StringBuilder();
                if (!started) {
                    delta.append('{').append(writeJson(property)).append(":\"");
                    started = true;
                }
                String escaped = writeJson(inputDelta);
                delta.append(escaped, 1, escaped.length() - 1);
                input = nextInput;
                if (close) {
                    delta.append("\"}");
                    closed = true;
                }
                output.add(new AssistantStreamEvent.ContentDelta(
                        ContentKind.TOOL_CALL, parts.indexOf(this), delta.toString(), snapshot()
                ));
            }

            @Override
            public ContentBlock content() {
                return new ToolCallContent(id, name, Map.of(property, input));
            }
        }

        private final class ToolPart implements Part {
            private final String id;
            private final String name;
            private final StringBuilder arguments;

            private ToolPart(String id, String name, String arguments) {
                this.id = id;
                this.name = name;
                this.arguments = new StringBuilder(arguments == null ? "" : arguments);
            }

            @Override
            public ContentKind kind() {
                return ContentKind.TOOL_CALL;
            }

            @Override
            public void append(String delta) {
                arguments.append(delta);
            }

            @Override
            public ContentBlock content() {
                Map<String, Object> parsed;
                try {
                    parsed = parseArguments(arguments.toString());
                } catch (IllegalArgumentException incomplete) {
                    parsed = Map.of();
                }
                return new ToolCallContent(id, name, parsed);
            }

            private void validate() {
                parseArguments(arguments.toString());
            }
        }

        private Map<String, Object> parseArguments(String json) {
            if (json == null || json.isBlank()) {
                return Map.of();
            }
            try {
                JsonNode parsed = mapper.readTree(json);
                if (!parsed.isObject()) {
                    throw new IllegalArgumentException("Tool arguments must be a JSON object");
                }
                return mapper.convertValue(parsed, mapper.getTypeFactory().constructMapType(
                        LinkedHashMap.class, String.class, Object.class
                ));
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException("Invalid tool arguments: " + json, failure);
            }
        }
    }
}
