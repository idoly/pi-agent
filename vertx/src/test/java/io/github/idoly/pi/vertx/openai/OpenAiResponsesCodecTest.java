package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ConstrainedSampling;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ThinkingLevelMap;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.SseEvent;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiResponsesCodecTest {
    private static final Model MODEL = new Model(
            "gpt-5", "GPT-5", "openai-responses", "openai",
            "https://api.openai.com/v1", true, List.of("text"), 128_000, 8
    );
    private final ObjectMapper mapper = new ObjectMapper();
    private final OpenAiResponsesCodec codec = new OpenAiResponsesCodec(mapper);

    @Test
    void encodesResponsesInputAndToolsWithStructuredJson() {
        AssistantMessage assistant = new AssistantMessage(
                List.of(new ToolCallContent(
                        "call_1|fc_1", "lookup", Map.of("id", 7)
                )),
                MODEL.api(), MODEL.provider(), MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1L
        );
        ToolResultMessage result = new ToolResultMessage(
                "call_1|fc_1", "lookup", List.of(new TextContent("found")),
                Map.of(), Usage.ZERO, false, 2L
        );
        ModelContext context = new ModelContext(
                "system",
                List.of(UserMessage.text("find", 0L), assistant, result),
                List.of(
                        new ToolDefinition(
                                "lookup", "Lookup",
                                Map.of("type", "object", "properties", Map.of())
                        ),
                        new ToolDefinition(
                                "strict_lookup", "Strict lookup",
                                Map.of(
                                        "type", "object",
                                        "properties", Map.of("id", Map.of("type", "integer"))
                                ),
                                new ConstrainedSampling.JsonSchema(
                                        ConstrainedSampling.Strictness.PREFER
                                )
                        )
                )
        );

        JsonNode request = codec.encodeRequest(MODEL, context, "high");

        assertEquals("gpt-5", request.path("model").asText());
        assertTrue(request.path("stream").asBoolean());
        assertTrue(!request.path("store").asBoolean());
        assertEquals(16, request.path("max_output_tokens").asInt());
        assertEquals("developer", request.path("input").get(0).path("role").asText());
        assertEquals("function_call", request.path("input").get(2).path("type").asText());
        assertEquals("call_1", request.path("input").get(2).path("call_id").asText());
        assertEquals("fc_1", request.path("input").get(2).path("id").asText());
        assertEquals("function_call_output", request.path("input").get(3).path("type").asText());
        assertEquals("high", request.path("reasoning").path("effort").asText());
        assertEquals("function", request.path("tools").get(0).path("type").asText());
        assertTrue(request.path("tools").get(1).path("strict").asBoolean());
        assertEquals("id", request.path("tools").get(1)
                .path("parameters").path("required").get(0).asText());
    }

    @Test
    void normalizesForeignToolIdsAndKeepsToolResultsPaired() {
        AssistantMessage foreign = new AssistantMessage(
                List.of(new ToolCallContent(
                        "call.bad|foreign/item+==", "lookup", Map.of("id", 7)
                )),
                MODEL.api(), "github-copilot", MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1L
        );
        ToolResultMessage result = new ToolResultMessage(
                "call.bad|foreign/item+==", "lookup", List.of(new TextContent("ok")),
                Map.of(), Usage.ZERO, false, 2L
        );

        JsonNode input = codec.encodeRequest(
                MODEL, new ModelContext("", List.of(foreign, result)), "off"
        ).path("input");

        assertEquals("call_bad", input.get(0).path("call_id").asText());
        assertEquals("fc_8ijpixmc6z0a", input.get(0).path("id").asText());
        assertEquals("call_bad", input.get(1).path("call_id").asText());
        assertEquals("8ijpixmc6z0a", OpenAiResponsesCodec.shortHash("foreign/item+=="));
    }

    @Test
    void appliesDeveloperAndReasoningOffCompatibility() {
        JsonNode defaults = codec.encodeRequest(
                MODEL, new ModelContext("system", List.of()), "off"
        );
        assertEquals("developer", defaults.path("input").get(0).path("role").asText());
        assertEquals("none", defaults.path("reasoning").path("effort").asText());

        OpenAiResponsesCompatibility compatibility = new OpenAiResponsesCompatibility(
                false, null,
                OpenAiResponsesCompatibility.SessionAffinityFormat.AUTO,
                true
        );
        JsonNode restricted = new OpenAiResponsesCodec(mapper, compatibility).encodeRequest(
                MODEL, new ModelContext("system", List.of()), "off"
        );
        assertEquals("system", restricted.path("input").get(0).path("role").asText());
        assertTrue(restricted.path("reasoning").isMissingNode());
    }

    @Test
    void clampsAndMapsModelThinkingLevelsInResponsesRequests() {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        values.put("off", null);
        values.put("medium", null);
        values.put("high", "max");
        Model mapped = new Model(
                MODEL.id(), MODEL.name(), MODEL.api(), MODEL.provider(), MODEL.baseUrl(),
                true, MODEL.input(), MODEL.contextWindow(), MODEL.maxTokens(),
                new ThinkingLevelMap(values)
        );

        JsonNode medium = codec.encodeRequest(mapped, new ModelContext("", List.of()), "medium");
        assertEquals("max", medium.path("reasoning").path("effort").asText());
        JsonNode off = codec.encodeRequest(mapped, new ModelContext("", List.of()), "off");
        assertEquals("minimal", off.path("reasoning").path("effort").asText());
        assertEquals("reasoning.encrypted_content", off.path("include").get(0).asText());
    }

    @Test
    void preservesSupportedToolResultImagesAndUsesFallbackOtherwise() {
        ToolResultMessage result = new ToolResultMessage(
                "call_image|fc_image", "inspect",
                List.of(new ImageContent("aGVsbG8=", "image/png")),
                Map.of(), Usage.ZERO, false, 1L
        );
        ModelContext context = new ModelContext("", List.of(result));
        Model imageCapable = new Model(
                MODEL.id(), MODEL.name(), MODEL.api(), MODEL.provider(), MODEL.baseUrl(),
                MODEL.reasoning(), List.of("text", "image"),
                MODEL.contextWindow(), MODEL.maxTokens()
        );

        JsonNode imageOutput = codec.encodeRequest(imageCapable, context, "off")
                .path("input").get(0).path("output");
        assertTrue(imageOutput.isArray());
        assertEquals("input_image", imageOutput.get(0).path("type").asText());
        assertEquals("data:image/png;base64,aGVsbG8=", imageOutput.get(0)
                .path("image_url").asText());

        Model textOnly = new Model(
                MODEL.id(), MODEL.name(), MODEL.api(), MODEL.provider(), MODEL.baseUrl(),
                MODEL.reasoning(), List.of("text"), MODEL.contextWindow(), MODEL.maxTokens()
        );
        JsonNode fallback = codec.encodeRequest(textOnly, context, "off")
                .path("input").get(0).path("output");
        assertEquals("(see attached image)", fallback.asText());
    }

    @Test
    void decodesOutputTextAndTerminalUsage() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"sequence_number\":1,\"item\":{\"id\":\"msg_1\",\"type\":\"message\","
                        + "\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}"),
                data("{\"type\":\"response.output_text.delta\",\"output_index\":0,"
                        + "\"content_index\":0,\"item_id\":\"msg_1\",\"sequence_number\":2,"
                        + "\"delta\":\"hello\",\"logprobs\":[]}"),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"sequence_number\":3,\"item\":{\"id\":\"msg_1\",\"type\":\"message\","
                        + "\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{"
                        + "\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[],\"logprobs\":[]}]}}"),
                completedResponse()
        );

        List<AssistantStreamEvent> events = codec.decode(frames, MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(List.of("Start", "ContentStart", "ContentDelta", "ContentEnd", "Done"),
                events.stream().map(event -> event.getClass().getSimpleName()).toList());
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        ).message();
        assertEquals(StopReason.STOP, done.stopReason());
        assertEquals("hello", assertInstanceOf(TextContent.class, done.content().getFirst()).text());
        assertEquals(4, done.usage().input());
        assertEquals(1, done.usage().cacheRead());
        assertEquals(2, done.usage().output());
        assertEquals(7, done.usage().totalTokens());
    }

    @Test
    void preservesEncryptedReasoningForStatelessReplay() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"sequence_number\":1,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\","
                        + "\"summary\":[],\"status\":\"in_progress\"}}"),
                data("{\"type\":\"response.reasoning_summary_text.delta\",\"output_index\":0,"
                        + "\"summary_index\":0,\"item_id\":\"rs_1\",\"sequence_number\":2,"
                        + "\"delta\":\"thought\"}"),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"sequence_number\":3,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\","
                        + "\"summary\":[{\"type\":\"summary_text\",\"text\":\"thought\"}],"
                        + "\"status\":\"completed\",\"encrypted_content\":\"encrypted\"}}"),
                completedResponse()
        );

        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class,
                codec.decode(frames, MODEL).collect().asList()
                        .await().atMost(Duration.ofSeconds(2)).getLast()
        ).message();
        ThinkingContent thinking = assertInstanceOf(
                ThinkingContent.class, done.content().getFirst()
        );
        assertEquals("thought", thinking.thinking());
        assertTrue(thinking.signature().contains("encrypted_content"));

        JsonNode replay = codec.encodeRequest(
                MODEL,
                new ModelContext("", List.of(done)),
                "high"
        );
        assertEquals("reasoning", replay.path("input").get(0).path("type").asText());
        assertEquals("encrypted", replay.path("input").get(0)
                .path("encrypted_content").asText());
    }

    @Test
    void backfillsAzureEncryptedReasoningFromTerminalOutput() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"item\":{\"id\":\"rs_azure\",\"type\":\"reasoning\",\"summary\":[]}}"),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"item\":{\"id\":\"rs_azure\",\"type\":\"reasoning\","
                        + "\"summary\":[{\"type\":\"summary_text\",\"text\":\"final thought\"}]}}"),
                data("{\"type\":\"response.completed\",\"response\":{\"id\":\"resp_azure\","
                        + "\"status\":\"completed\",\"output\":[{\"id\":\"rs_azure\","
                        + "\"type\":\"reasoning\",\"summary\":[],"
                        + "\"encrypted_content\":\"terminal-only\"}]}}")
        );

        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class,
                codec.decode(frames, MODEL).collect().asList()
                        .await().atMost(Duration.ofSeconds(2)).getLast()
        ).message();
        ThinkingContent thinking = assertInstanceOf(
                ThinkingContent.class, done.content().getFirst()
        );
        assertEquals("final thought", thinking.thinking());
        JsonNode signature;
        try {
            signature = mapper.readTree(thinking.signature());
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
        assertEquals("terminal-only", signature.path("encrypted_content").asText());
    }

    @Test
    void terminalIncompleteOverridesProvisionalFinalAnswerStop() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"item\":{\"id\":\"msg_1\",\"type\":\"message\","
                        + "\"phase\":\"final_answer\",\"content\":[]}}"),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"item\":{\"id\":\"msg_1\",\"type\":\"message\","
                        + "\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\","
                        + "\"text\":\"partial\"}]}}"),
                data("{\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\","
                        + "\"status\":\"incomplete\",\"incomplete_details\":{"
                        + "\"reason\":\"max_output_tokens\"}}}")
        );

        List<AssistantStreamEvent> events = codec.decode(frames, MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2));
        AssistantMessage contentEnd = events.stream()
                .filter(AssistantStreamEvent.ContentEnd.class::isInstance)
                .map(AssistantStreamEvent.ContentEnd.class::cast)
                .findFirst().orElseThrow().partial();
        assertEquals(StopReason.STOP, contentEnd.stopReason());
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        ).message();
        assertEquals(StopReason.LENGTH, done.stopReason());
        assertEquals("partial", assertInstanceOf(
                TextContent.class, done.content().getFirst()
        ).text());
    }

    @Test
    void encodesReplaysAndStreamsGrammarCustomTools() {
        OpenAiResponsesCompatibility grammarCompatibility = new OpenAiResponsesCompatibility(
                true, "none",
                OpenAiResponsesCompatibility.SessionAffinityFormat.AUTO,
                true, true, true
        );
        OpenAiResponsesCodec grammarCodec = new OpenAiResponsesCodec(
                mapper, grammarCompatibility
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
                "", List.of(history, result), List.of(grammarTool)
        );
        JsonNode request = grammarCodec.encodeRequest(MODEL, context, "off");
        assertEquals("custom", request.path("tools").get(0).path("type").asText());
        assertEquals("lark", request.path("tools").get(0)
                .path("format").path("syntax").asText());
        assertEquals("custom_tool_call", request.path("input").get(0).path("type").asText());
        assertEquals("abc", request.path("input").get(0).path("input").asText());
        assertEquals("custom_tool_call_output", request.path("input").get(1)
                .path("type").asText());

        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"item\":{\"type\":\"custom_tool_call\",\"call_id\":\"call_1\","
                        + "\"id\":\"ctc_1\",\"name\":\"sample_tool\",\"input\":\"\"}}"),
                data("{\"type\":\"response.custom_tool_call_input.delta\",\"output_index\":0,"
                        + "\"item_id\":\"ctc_1\",\"delta\":\"ab\"}"),
                data("{\"type\":\"response.custom_tool_call_input.done\",\"output_index\":0,"
                        + "\"item_id\":\"ctc_1\",\"input\":\"abc\"}"),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"item\":{\"type\":\"custom_tool_call\",\"call_id\":\"call_1\","
                        + "\"id\":\"ctc_1\",\"name\":\"sample_tool\",\"input\":\"abc\"}}"),
                data("{\"type\":\"response.completed\",\"response\":{\"status\":\"completed\","
                        + "\"usage\":{\"input_tokens\":1,\"output_tokens\":1,"
                        + "\"total_tokens\":2}}}")
        );
        Map<String, OpenAiGrammar.Grammar> grammars = OpenAiGrammar.resolveAll(
                mapper, List.of(grammarTool), true
        );
        List<AssistantStreamEvent> events = grammarCodec.decode(frames, MODEL, grammars)
                .collect().asList().await().atMost(Duration.ofSeconds(2));
        List<String> deltas = events.stream()
                .filter(AssistantStreamEvent.ContentDelta.class::isInstance)
                .map(AssistantStreamEvent.ContentDelta.class::cast)
                .map(AssistantStreamEvent.ContentDelta::delta)
                .toList();
        assertEquals(Map.of("payload", "abc"), mapper.convertValue(
                readTree(String.join("", deltas)),
                mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class)
        ));
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        ).message();
        ToolCallContent call = assertInstanceOf(
                ToolCallContent.class, done.content().getFirst()
        );
        assertEquals("call_1|ctc_1", call.id());
        assertEquals(Map.of("payload", "abc"), call.arguments());
        assertEquals(StopReason.TOOL_USE, done.stopReason());
    }

    @Test
    void rejectsMalformedFinalFunctionArguments() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"item\":{\"id\":\"fc_bad\",\"type\":\"function_call\","
                        + "\"call_id\":\"call_bad\",\"name\":\"lookup\",\"arguments\":\"\"}}"),
                data("{\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,"
                        + "\"delta\":\"{\"}" ),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"item\":{\"id\":\"fc_bad\",\"type\":\"function_call\","
                        + "\"call_id\":\"call_bad\",\"name\":\"lookup\","
                        + "\"arguments\":\"{\"}}")
        );

        assertThrows(IllegalArgumentException.class, () -> codec.decode(frames, MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2)));
    }

    @Test
    void decodesFunctionCallArgumentsAndCompositeId() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"type\":\"response.output_item.added\",\"output_index\":0,"
                        + "\"sequence_number\":1,\"item\":{\"id\":\"item_7\",\"type\":\"function_call\","
                        + "\"status\":\"in_progress\",\"call_id\":\"call_7\",\"name\":\"lookup\","
                        + "\"arguments\":\"\"}}"),
                data("{\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,"
                        + "\"item_id\":\"item_7\",\"sequence_number\":2,\"delta\":\"{\\\"id\\\":7}\"}"),
                data("{\"type\":\"response.output_item.done\",\"output_index\":0,"
                        + "\"sequence_number\":3,\"item\":{\"id\":\"item_7\",\"type\":\"function_call\","
                        + "\"status\":\"completed\",\"call_id\":\"call_7\",\"name\":\"lookup\","
                        + "\"arguments\":\"{\\\"id\\\":7}\"}}"),
                completedResponse()
        );

        List<AssistantStreamEvent> events = codec.decode(frames, MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2));
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        ).message();
        ToolCallContent call = assertInstanceOf(ToolCallContent.class, done.content().getFirst());
        assertEquals(StopReason.TOOL_USE, done.stopReason());
        assertEquals("call_7|item_7", call.id());
        assertEquals("lookup", call.name());
        assertEquals(Map.of("id", 7), call.arguments());
    }

    private JsonNode readTree(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static SseEvent completedResponse() {
        return data("{\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{"
                + "\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\","
                + "\"model\":\"gpt-5\",\"output\":[],\"parallel_tool_calls\":true,"
                + "\"tool_choice\":\"auto\",\"tools\":[],\"usage\":{\"input_tokens\":5,"
                + "\"input_tokens_details\":{\"cached_tokens\":1,\"cache_write_tokens\":0},"
                + "\"output_tokens\":2,\"output_tokens_details\":{\"reasoning_tokens\":0},"
                + "\"total_tokens\":7}}}" );
    }

    private static SseEvent data(String value) {
        return new SseEvent("message", value, null, null);
    }
}
