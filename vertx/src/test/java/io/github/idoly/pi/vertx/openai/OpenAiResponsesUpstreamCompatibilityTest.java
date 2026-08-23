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
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.SseEvent;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class OpenAiResponsesUpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Model MODEL = new Model(
            "gpt-fixture", "GPT Fixture", "openai-responses", "openai",
            "https://api.openai.com/v1", true, List.of("text", "image"),
            128_000, 1_024
    );
    private final OpenAiResponsesCodec codec = new OpenAiResponsesCodec(MAPPER);

    @Test
    void fixtureTargetsPinnedPiAiVersion() throws Exception {
        assertEquals("@earendil-works/pi-ai", fixture().path("upstream").path("package").asText());
        assertEquals("0.84.2", fixture().path("upstream").path("version").asText());
    }

    @Test
    void requestInputMatchesTypeScriptOracle() throws Exception {
        ObjectNode signature = MAPPER.createObjectNode()
                .put("type", "reasoning")
                .put("id", "rs_history");
        signature.putArray("summary").addObject()
                .put("type", "summary_text")
                .put("text", "history thought");
        signature.put("encrypted_content", "history-encrypted");
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new ThinkingContent("history thought", signature.toString()),
                        new TextContent("calling"),
                        new ToolCallContent("call_1|fc_1", "lookup", Map.of("id", 7))
                ),
                MODEL.api(), MODEL.provider(), MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 2L
        );
        ToolResultMessage result = new ToolResultMessage(
                "call_1|fc_1", "lookup", List.of(new TextContent("")),
                Map.of(), Usage.ZERO, false, 3L
        );
        ModelContext context = new ModelContext(
                "Be precise.",
                List.of(
                        new UserMessage(List.of(
                                new TextContent("inspect"),
                                new ImageContent("aGVsbG8=", "image/png")
                        ), 1L),
                        assistant,
                        result
                )
        );

        JsonNode actual = codec.encodeRequest(MODEL, context, "high").path("input");

        assertEquals(fixture().path("requestInput"), actual);
    }

    @Test
    void foreignToolIdNormalizationMatchesTypeScriptOracle() throws Exception {
        String foreignId = "call.bad|foreign/item+==";
        AssistantMessage assistant = new AssistantMessage(
                List.of(new ToolCallContent(foreignId, "lookup", Map.of("id", 7))),
                MODEL.api(), "github-copilot", MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1L
        );
        ToolResultMessage result = new ToolResultMessage(
                foreignId, "lookup", List.of(new TextContent("ok")),
                Map.of(), Usage.ZERO, false, 2L
        );

        JsonNode actual = codec.encodeRequest(
                MODEL, new ModelContext("", List.of(assistant, result)), "off"
        ).path("input");

        assertEquals(fixture().path("foreignToolInput"), actual);
    }

    @Test
    void grammarToolsMatchTypeScriptOracle() throws Exception {
        JsonNode expected = fixture().path("grammar");
        OpenAiResponsesCompatibility compatibility = new OpenAiResponsesCompatibility(
                true, "none",
                OpenAiResponsesCompatibility.SessionAffinityFormat.AUTO,
                true, true, true
        );
        OpenAiResponsesCodec grammarCodec = new OpenAiResponsesCodec(MAPPER, compatibility);
        ToolDefinition tool = new ToolDefinition(
                "sample_tool", "Sample tool",
                Map.of(
                        "type", "object",
                        "properties", Map.of("payload", Map.of("type", "string")),
                        "required", List.of("payload"),
                        "additionalProperties", false
                ),
                new ConstrainedSampling.Grammar("start: /[a-z]+/", null)
        );
        AssistantMessage history = new AssistantMessage(
                List.of(new ToolCallContent(
                        "call_1|ctc_1", "sample_tool", Map.of("payload", "abc")
                )),
                MODEL.api(), MODEL.provider(), MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1L
        );
        ToolResultMessage result = new ToolResultMessage(
                "call_1|ctc_1", "sample_tool", List.of(new TextContent("done")),
                Map.of(), Usage.ZERO, false, 2L
        );
        ModelContext context = new ModelContext(
                "", List.of(history, result), List.of(tool)
        );
        JsonNode request = grammarCodec.encodeRequest(MODEL, context, "off");
        assertEquals(expected.path("declaration"), request.path("tools").get(0));
        assertEquals(expected.path("replay"), request.path("input"));

        List<SseEvent> frames = new ArrayList<>();
        for (JsonNode frame : expected.path("frames")) {
            frames.add(new SseEvent(
                    "message", MAPPER.writeValueAsString(frame), null, null
            ));
        }
        Map<String, OpenAiGrammar.Grammar> grammars = OpenAiGrammar.resolveAll(
                MAPPER, List.of(tool), true
        );
        List<AssistantStreamEvent> decoded = grammarCodec.decode(
                Multi.createFrom().iterable(frames), MODEL, grammars
        ).collect().asList().await().atMost(Duration.ofSeconds(2));
        ArrayNode deltas = MAPPER.createArrayNode();
        decoded.stream()
                .filter(AssistantStreamEvent.ContentDelta.class::isInstance)
                .map(AssistantStreamEvent.ContentDelta.class::cast)
                .map(AssistantStreamEvent.ContentDelta::delta)
                .forEach(deltas::add);
        assertEquals(expected.path("deltas"), deltas);
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, decoded.getLast()
        ).message();
        ArrayNode content = MAPPER.createArrayNode();
        done.content().stream().map(ToolCallContent.class::cast).forEach(call -> {
            ObjectNode node = content.addObject()
                    .put("type", "toolCall")
                    .put("id", call.id())
                    .put("name", call.name());
            node.set("arguments", MAPPER.valueToTree(call.arguments()));
        });
        assertEquals(expected.path("content"), content);
        assertEquals("toolUse", expected.path("stopReason").asText());
        assertEquals(StopReason.TOOL_USE, done.stopReason());
    }

    @Test
    void terminalErrorsMatchTypeScriptOracle() throws Exception {
        JsonNode scenarios = fixture().path("terminalScenarios");
        for (String name : List.of("earlyEof", "contentFilter", "failed")) {
            JsonNode scenario = scenarios.path(name);
            List<SseEvent> frames = new ArrayList<>();
            for (JsonNode frame : scenario.path("frames")) {
                frames.add(new SseEvent(
                        "message", MAPPER.writeValueAsString(frame), null, null
                ));
            }

            List<AssistantStreamEvent> decoded = codec.decode(
                    Multi.createFrom().iterable(frames), MODEL
            ).collect().asList().await().atMost(Duration.ofSeconds(2));
            ArrayNode eventTypes = MAPPER.createArrayNode();
            decoded.forEach(event -> eventTypes.add(
                    event instanceof AssistantStreamEvent.Start ? "start" : "error"
            ));
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
    void streamEventsAndTranscriptMatchTypeScriptOracle() throws Exception {
        JsonNode streamFixture = fixture().path("stream");
        List<SseEvent> frames = new ArrayList<>();
        for (JsonNode frame : streamFixture.path("frames")) {
            frames.add(new SseEvent("message", MAPPER.writeValueAsString(frame), null, null));
        }

        List<AssistantStreamEvent> decoded = codec.decode(
                Multi.createFrom().iterable(frames), MODEL
        ).collect().asList().await().atMost(Duration.ofSeconds(2));
        ArrayNode events = MAPPER.createArrayNode();
        for (AssistantStreamEvent event : decoded) {
            JsonNode normalized = normalizeEvent(event);
            if (normalized != null) {
                events.add(normalized);
            }
        }
        AssistantMessage message = assertInstanceOf(
                AssistantStreamEvent.Done.class, decoded.getLast()
        ).message();

        assertEquals(streamFixture.path("events"), events);
        assertEquals(streamFixture.path("message"), jsonRoundTrip(normalizeMessage(message)));
    }

    private static JsonNode normalizeEvent(AssistantStreamEvent event) {
        if (event instanceof AssistantStreamEvent.ContentStart start) {
            return eventNode(kind(start.kind()) + "_start", start.contentIndex(), null);
        }
        if (event instanceof AssistantStreamEvent.ContentDelta delta) {
            return eventNode(kind(delta.kind()) + "_delta", delta.contentIndex(), delta.delta());
        }
        if (event instanceof AssistantStreamEvent.ContentEnd end) {
            return eventNode(kind(end.kind()) + "_end", end.contentIndex(), null);
        }
        return null;
    }

    private static ObjectNode eventNode(String type, int index, String delta) {
        ObjectNode node = MAPPER.createObjectNode().put("type", type).put("contentIndex", index);
        if (delta != null) {
            node.put("delta", delta);
        }
        return node;
    }

    private static String kind(ContentKind kind) {
        return switch (kind) {
            case TEXT -> "text";
            case THINKING -> "thinking";
            case TOOL_CALL -> "toolcall";
        };
    }

    private static ObjectNode normalizeMessage(AssistantMessage message) throws Exception {
        ObjectNode normalized = MAPPER.createObjectNode();
        ArrayNode content = normalized.putArray("content");
        for (ContentBlock block : message.content()) {
            if (block instanceof ThinkingContent thinking) {
                content.addObject()
                        .put("type", "thinking")
                        .put("thinking", thinking.thinking())
                        .set("signature", MAPPER.readTree(thinking.signature()));
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
        normalized.put("stopReason", switch (message.stopReason()) {
            case TOOL_USE -> "toolUse";
            default -> message.stopReason().name().toLowerCase();
        });
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

    private static JsonNode jsonRoundTrip(JsonNode value) throws Exception {
        return MAPPER.readTree(MAPPER.writeValueAsBytes(value));
    }

    private static JsonNode fixture() throws Exception {
        Path fixture = Path.of(
                System.getProperty("pi.compatFixtures"),
                "openai-responses-0.84.2.json"
        );
        return MAPPER.readTree(fixture.toFile());
    }
}
