package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ConstrainedSampling;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.ContentKind;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.SseEvent;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class OpenAiChatUpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Model MODEL = new Model(
            "chat-fixture", "Chat Fixture", "openai-completions", "openai",
            "https://api.openai.com/v1", true, List.of("text"), 128_000, 1_024
    );
    private final OpenAiChatCodec codec = new OpenAiChatCodec(MAPPER);

    @Test
    void constrainedToolSchemaMatchesTypeScriptOracle() throws Exception {
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("path", Map.of("type", "string"));
        properties.put("offset", Map.of("type", "integer"));
        LinkedHashMap<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        schema.put("required", List.of("path"));
        ToolDefinition tool = new ToolDefinition(
                "edit", "Edit", schema,
                new ConstrainedSampling.JsonSchema(ConstrainedSampling.Strictness.PREFER)
        );

        JsonNode actual = codec.encodeRequest(
                MODEL,
                new ModelContext("", List.of(UserMessage.text("edit", 1L)), List.of(tool)),
                "off"
        ).path("tools").get(0);

        assertEquals(fixture().path("constrainedTool"), actual);

        OpenAiCompatibility grammarCompatibility = new OpenAiCompatibility(
                OpenAiCompatibility.MaxTokensField.MAX_COMPLETION_TOKENS,
                true, OpenAiCompatibility.ReasoningFormat.STANDARD,
                true, true, true, true
        );
        ToolDefinition grammarTool = new ToolDefinition(
                "sample_tool", "Sample tool",
                Map.of(
                        "type", "object",
                        "properties", Map.of("payload", Map.of("type", "string")),
                        "required", List.of("payload"),
                        "additionalProperties", false
                ),
                new ConstrainedSampling.Grammar("start: /[a-z]+/", null)
        );
        JsonNode grammarActual = new OpenAiChatCodec(
                MAPPER, grammarCompatibility
        ).encodeRequest(
                MODEL,
                new ModelContext(
                        "", List.of(UserMessage.text("sample", 1L)), List.of(grammarTool)
                ),
                "off"
        ).path("tools").get(0);
        assertEquals(fixture().path("grammarTool"), grammarActual);
    }

    @Test
    void terminalErrorsMatchTypeScriptOracle() throws Exception {
        JsonNode scenarios = fixture().path("terminalScenarios");
        for (String name : List.of("earlyEof", "contentFilter")) {
            JsonNode scenario = scenarios.path(name);
            List<SseEvent> frames = new ArrayList<>();
            for (JsonNode chunk : scenario.path("chunks")) {
                frames.add(data(MAPPER.writeValueAsString(chunk)));
            }
            if (scenario.path("includeDone").asBoolean()) {
                frames.add(data("[DONE]"));
            }
            List<AssistantStreamEvent> decoded = codec.decode(
                    Multi.createFrom().iterable(frames), MODEL
            ).collect().asList().await().atMost(Duration.ofSeconds(2));
            ArrayNode eventTypes = MAPPER.createArrayNode();
            decoded.forEach(event -> eventTypes.add(normalizeEvent(event).path("type").asText()));
            AssistantMessage message = assertInstanceOf(
                    AssistantStreamEvent.Error.class, decoded.getLast(), name
            ).message();
            ObjectNode normalized = MAPPER.createObjectNode()
                    .put("stopReason", "error")
                    .put("errorMessage", message.errorMessage());
            if (message.responseId() != null) {
                normalized.put("responseId", message.responseId());
            }
            if (message.rawStopReason() != null) {
                normalized.put("rawStopReason", message.rawStopReason());
            }

            assertEquals(scenario.path("events"), eventTypes, name);
            assertEquals(scenario.path("message"), normalized, name);
        }
    }

    @Test
    void requestStreamAndTranscriptMatchTypeScriptOracle() throws Exception {
        JsonNode fixture = fixture();
        assertEquals("@earendil-works/pi-ai", fixture.path("upstream").path("package").asText());
        assertEquals("0.84.2", fixture.path("upstream").path("version").asText());
        ModelContext context = new ModelContext(
                "Be precise.",
                List.of(UserMessage.text("calculate", 1L)),
                List.of(new ToolDefinition(
                        "lookup", "Lookup",
                        Map.of(
                                "type", "object",
                                "properties", Map.of("id", Map.of("type", "integer")),
                                "required", List.of("id")
                        )
                ))
        );
        JsonNode request = codec.encodeRequest(MODEL, context, "high");
        ObjectNode normalizedRequest = MAPPER.createObjectNode();
        for (String field : List.of(
                "model", "messages", "tools", "stream", "stream_options",
                "max_completion_tokens", "reasoning_effort"
        )) {
            normalizedRequest.set(field, request.path(field));
        }
        assertEquals(fixture.path("request"), normalizedRequest);

        List<SseEvent> frames = new ArrayList<>();
        for (JsonNode chunk : fixture.path("stream").path("chunks")) {
            frames.add(data(MAPPER.writeValueAsString(chunk)));
        }
        frames.add(data("[DONE]"));
        List<AssistantStreamEvent> decoded = codec.decode(
                Multi.createFrom().iterable(frames), MODEL
        ).collect().asList().await().atMost(Duration.ofSeconds(2));
        ArrayNode events = MAPPER.createArrayNode();
        decoded.forEach(event -> events.add(normalizeEvent(event)));
        assertEquals(fixture.path("stream").path("events"), events);

        AssistantMessage message = assertInstanceOf(
                AssistantStreamEvent.Done.class, decoded.getLast()
        ).message();
        assertEquals(
                fixture.path("stream").path("message"),
                MAPPER.readTree(MAPPER.writeValueAsBytes(normalizeMessage(message)))
        );
    }

    private static ObjectNode normalizeEvent(AssistantStreamEvent event) {
        ObjectNode node = MAPPER.createObjectNode();
        if (event instanceof AssistantStreamEvent.Start) {
            return node.put("type", "start");
        }
        if (event instanceof AssistantStreamEvent.Done) {
            return node.put("type", "done");
        }
        if (event instanceof AssistantStreamEvent.Error) {
            return node.put("type", "error");
        }
        if (event instanceof AssistantStreamEvent.ContentStart start) {
            return node.put("type", kind(start.kind()) + "_start")
                    .put("contentIndex", start.contentIndex());
        }
        if (event instanceof AssistantStreamEvent.ContentDelta delta) {
            return node.put("type", kind(delta.kind()) + "_delta")
                    .put("contentIndex", delta.contentIndex())
                    .put("delta", delta.delta());
        }
        AssistantStreamEvent.ContentEnd end = assertInstanceOf(
                AssistantStreamEvent.ContentEnd.class, event
        );
        return node.put("type", kind(end.kind()) + "_end")
                .put("contentIndex", end.contentIndex());
    }

    private static String kind(ContentKind kind) {
        return switch (kind) {
            case TEXT -> "text";
            case THINKING -> "thinking";
            case TOOL_CALL -> "toolcall";
        };
    }

    private static ObjectNode normalizeMessage(AssistantMessage message) {
        ObjectNode normalized = MAPPER.createObjectNode();
        ArrayNode content = normalized.putArray("content");
        for (ContentBlock block : message.content()) {
            if (block instanceof ThinkingContent thinking) {
                content.addObject().put("type", "thinking").put("thinking", thinking.thinking());
            } else if (block instanceof TextContent text) {
                content.addObject().put("type", "text").put("text", text.text());
            } else if (block instanceof ToolCallContent call) {
                ObjectNode tool = content.addObject()
                        .put("type", "toolCall")
                        .put("id", call.id())
                        .put("name", call.name());
                tool.set("arguments", MAPPER.valueToTree(call.arguments()));
            }
        }
        normalized.put("stopReason", message.stopReason() == io.github.idoly.pi.ai.StopReason.TOOL_USE
                ? "toolUse"
                : message.stopReason().name().toLowerCase());
        normalized.put("responseId", message.responseId());
        normalized.put("rawStopReason", message.rawStopReason());
        ObjectNode usage = normalized.putObject("usage");
        usage.put("input", message.usage().input());
        usage.put("output", message.usage().output());
        usage.put("cacheRead", message.usage().cacheRead());
        usage.put("cacheWrite", message.usage().cacheWrite());
        usage.put("reasoning", message.usage().reasoning());
        usage.put("totalTokens", message.usage().totalTokens());
        return normalized;
    }

    private static SseEvent data(String value) {
        return new SseEvent("message", value, null, null);
    }

    private static JsonNode fixture() throws Exception {
        return MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"), "openai-chat-0.84.2.json"
        ).toFile());
    }
}
