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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiChatCodecTest {
    private static final Model MODEL = new Model(
            "gpt-fixture", "GPT Fixture", "openai-completions", "fixture",
            "https://example.invalid/v1", true, List.of("text", "image"), 32_000, 2_048
    );
    private final ObjectMapper mapper = new ObjectMapper();
    private final OpenAiChatCodec codec = new OpenAiChatCodec(mapper);

    @Test
    void encodesMessagesToolsAndReasoningOptions() throws Exception {
        UserMessage user = new UserMessage(List.of(
                new TextContent("inspect"),
                new ImageContent("YWJj", "image/png")
        ), 1L);
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new TextContent("calling"),
                        new ToolCallContent("call-1", "lookup", Map.of("id", 7))
                ),
                MODEL.api(), MODEL.provider(), MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 2L
        );
        ToolResultMessage result = new ToolResultMessage(
                "call-1", "lookup", List.of(new TextContent("found")), Map.of(),
                Usage.ZERO, false, 3L
        );
        ModelContext context = new ModelContext(
                "system",
                List.of(user, assistant, result),
                List.of(new ToolDefinition(
                        "lookup", "Lookup an id",
                        Map.of(
                                "type", "object",
                                "properties", Map.of("id", Map.of("type", "integer")),
                                "required", List.of("id")
                        )
                ))
        );

        JsonNode request = codec.encodeRequest(MODEL, context, "high");

        assertEquals("gpt-fixture", request.path("model").asText());
        assertTrue(request.path("stream").asBoolean());
        assertTrue(request.path("stream_options").path("include_usage").asBoolean());
        assertEquals(2_048, request.path("max_completion_tokens").asInt());
        assertTrue(request.path("max_tokens").isMissingNode());
        assertEquals("high", request.path("reasoning_effort").asText());
        assertEquals("developer", request.path("messages").get(0).path("role").asText());
        assertEquals(
                "data:image/png;base64,YWJj",
                request.path("messages").get(1).path("content").get(1)
                        .path("image_url").path("url").asText()
        );
        assertEquals(
                "{\"id\":7}",
                request.path("messages").get(2).path("tool_calls").get(0)
                        .path("function").path("arguments").asText()
        );
        assertEquals("call-1", request.path("messages").get(3).path("tool_call_id").asText());
        assertEquals("integer", request.path("tools").get(0).path("function")
                .path("parameters").path("properties").path("id").path("type").asText());
        assertTrue(!request.path("tools").get(0).path("function").path("strict").asBoolean());
    }

    @Test
    void decodesTextParallelToolCallsUsageAndDone() {
        List<SseEvent> frames = List.of(
                data("{\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"role\":\"assistant\"},"
                        + "\"finish_reason\":null}]}"),
                data("{\"choices\":[{\"delta\":{\"content\":\"hel\"},\"finish_reason\":null}]}"),
                data("{\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}"),
                data("{\"choices\":[{\"delta\":{\"tool_calls\":["
                        + "{\"index\":0,\"id\":\"call-a\",\"function\":{\"name\":\"first\",\"arguments\":\"{\\\"x\\\":\"}},"
                        + "{\"index\":1,\"id\":\"call-b\",\"function\":{\"name\":\"second\",\"arguments\":\"{}\"}}"
                        + "]},\"finish_reason\":null}]}"),
                data("{\"choices\":[{\"delta\":{\"tool_calls\":["
                        + "{\"index\":0,\"function\":{\"arguments\":\"1}\"}}"
                        + "]},\"finish_reason\":null}]}"),
                data("{\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}"),
                data("{\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4,"
                        + "\"total_tokens\":14,\"prompt_tokens_details\":{\"cached_tokens\":3},"
                        + "\"completion_tokens_details\":{\"reasoning_tokens\":2}}}"),
                data("[DONE]")
        );

        List<AssistantStreamEvent> events = codec.decode(Multi.createFrom().iterable(frames), MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(List.of(
                "Start", "ContentStart", "ContentDelta", "ContentDelta",
                "ContentStart", "ContentDelta", "ContentStart", "ContentDelta",
                "ContentDelta", "ContentEnd", "ContentEnd", "ContentEnd", "Done"
        ), events.stream().map(event -> event.getClass().getSimpleName()).toList());
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        ).message();
        assertEquals(StopReason.TOOL_USE, done.stopReason());
        assertEquals("chatcmpl-1", done.responseId());
        assertEquals("tool_calls", done.rawStopReason());
        assertEquals("hello", assertInstanceOf(TextContent.class, done.content().get(0)).text());
        ToolCallContent first = assertInstanceOf(ToolCallContent.class, done.content().get(1));
        ToolCallContent second = assertInstanceOf(ToolCallContent.class, done.content().get(2));
        assertEquals("call-a", first.id());
        assertEquals(Map.of("x", 1), first.arguments());
        assertEquals("call-b", second.id());
        assertEquals(Map.of(), second.arguments());
        assertEquals(7, done.usage().input());
        assertEquals(4, done.usage().output());
        assertEquals(3, done.usage().cacheRead());
        assertEquals(2, done.usage().reasoning());
        events.stream()
                .filter(AssistantStreamEvent.ContentEnd.class::isInstance)
                .map(AssistantStreamEvent.ContentEnd.class::cast)
                .forEach(end -> assertEquals(14, end.partial().usage().totalTokens()));
    }

    @Test
    void reportsAnErrorWhenTheStreamEndsWithoutAFinishReason() {
        Multi<SseEvent> frames = Multi.createFrom().item(data(
                "{\"choices\":[{\"delta\":{\"content\":\"partial\"},"
                        + "\"finish_reason\":null}]}"
        ));

        List<AssistantStreamEvent> events = codec.decode(frames, MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2));

        AssistantStreamEvent.Error error = assertInstanceOf(
                AssistantStreamEvent.Error.class, events.getLast()
        );
        assertEquals(StopReason.ERROR, error.message().stopReason());
        assertEquals("Stream ended without finish_reason", error.message().errorMessage());
        assertEquals("partial", assertInstanceOf(
                TextContent.class, error.message().content().getFirst()
        ).text());
    }

    @Test
    void ignoresSemanticDeltasAfterFinishButAcceptsTailUsage() {
        List<AssistantStreamEvent> events = codec.decode(
                Multi.createFrom().items(
                        data("{\"choices\":[{\"delta\":{\"content\":\"final\"},\"finish_reason\":\"stop\"}]}"),
                        data("{\"choices\":[{\"delta\":{\"content\":\"late\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}"),
                        data("[DONE]")
                ), MODEL
        ).collect().asList().await().indefinitely();
        AssistantMessage done = ((AssistantStreamEvent.Done)
                events.getLast()).message();
        assertEquals("final", assertInstanceOf(
                TextContent.class, done.content().getFirst()
        ).text());
        assertEquals(4, done.usage().totalTokens());
        assertEquals(1, events.stream()
                .filter(AssistantStreamEvent.Done.class::isInstance).count());
    }

    @Test
    void preservesAndReplaysStructuredReasoningDetails() throws Exception {
        String signed = "{\"type\":\"reasoning.text\",\"text\":\"signed thought\","
                + "\"signature\":\"sha256:signed\",\"id\":\"reasoning-text-1\","
                + "\"format\":\"anthropic-claude-v1\",\"index\":0}";
        String encrypted = "{\"type\":\"reasoning.encrypted\",\"id\":\"call_1\","
                + "\"data\":\"encrypted-signature\"}";
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"id\":\"chatcmpl-details\",\"choices\":[{\"delta\":{"
                        + "\"reasoning\":\"signed thought\",\"reasoning_details\":["
                        + signed + "]},\"finish_reason\":null}]}"),
                data("{\"id\":\"chatcmpl-details\",\"choices\":[{\"delta\":{"
                        + "\"reasoning_details\":[" + encrypted + "]},\"finish_reason\":null}]}"),
                data("{\"id\":\"chatcmpl-details\",\"choices\":[{\"delta\":{"
                        + "\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{"
                        + "\"name\":\"lookup\",\"arguments\":\"{\\\"id\\\":7}\"}}]},"
                        + "\"finish_reason\":null}]}"),
                data("{\"id\":\"chatcmpl-details\",\"choices\":[{\"delta\":{},"
                        + "\"finish_reason\":\"tool_calls\"}]}"),
                data("[DONE]")
        );

        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class,
                codec.decode(frames, MODEL).collect().asList()
                        .await().atMost(Duration.ofSeconds(2)).getLast()
        ).message();
        ThinkingContent thinking = assertInstanceOf(
                ThinkingContent.class, done.content().getFirst()
        );
        assertEquals("signed thought", thinking.thinking());
        JsonNode signature = mapper.readTree(thinking.signature());
        assertEquals(2, signature.size());
        assertEquals("reasoning.text", signature.get(0).path("type").asText());
        assertEquals("reasoning.encrypted", signature.get(1).path("type").asText());

        JsonNode replay = codec.encodeRequest(
                MODEL, new ModelContext("", List.of(done)), "high"
        ).path("messages").get(0);
        assertEquals(signature, replay.path("reasoning_details"));
        assertTrue(replay.path("reasoning").isMissingNode());
        assertTrue(replay.path("reasoning_content").isMissingNode());
    }

    @Test
    void appliesLegacyAndQwenCompatibilitySettings() {
        OpenAiChatCodec legacy = new OpenAiChatCodec(mapper, OpenAiCompatibility.LEGACY);
        JsonNode legacyRequest = legacy.encodeRequest(MODEL, new ModelContext("", List.of()), "high");
        assertEquals(2_048, legacyRequest.path("max_tokens").asInt());
        assertTrue(legacyRequest.path("max_completion_tokens").isMissingNode());
        assertTrue(legacyRequest.path("stream_options").isMissingNode());
        assertTrue(legacyRequest.path("reasoning_effort").isMissingNode());

        OpenAiChatCodec qwen = new OpenAiChatCodec(mapper, new OpenAiCompatibility(
                OpenAiCompatibility.MaxTokensField.MAX_TOKENS,
                true,
                OpenAiCompatibility.ReasoningFormat.QWEN,
                true
        ));
        JsonNode enabled = qwen.encodeRequest(MODEL, new ModelContext("", List.of()), "medium");
        assertTrue(enabled.path("enable_thinking").asBoolean());
        assertEquals("medium", enabled.path("reasoning_effort").asText());
        JsonNode disabled = qwen.encodeRequest(MODEL, new ModelContext("", List.of()), "off");
        assertTrue(disabled.has("enable_thinking"));
        assertTrue(!disabled.path("enable_thinking").asBoolean());
    }

    @Test
    void clampsAndMapsModelThinkingLevelsInChatRequests() {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        values.put("medium", null);
        values.put("high", "max");
        Model mapped = new Model(
                MODEL.id(), MODEL.name(), MODEL.api(), MODEL.provider(), MODEL.baseUrl(),
                true, MODEL.input(), MODEL.contextWindow(), MODEL.maxTokens(),
                new ThinkingLevelMap(values)
        );

        JsonNode request = codec.encodeRequest(mapped, new ModelContext("", List.of()), "medium");

        assertEquals("max", request.path("reasoning_effort").asText());
    }

    @Test
    void appliesProviderSpecificReasoningFormats() {
        ModelContext empty = new ModelContext("", List.of());
        JsonNode openRouter = codec(OpenAiCompatibility.ReasoningFormat.OPENROUTER, true)
                .encodeRequest(MODEL, empty, "high");
        assertEquals("high", openRouter.path("reasoning").path("effort").asText());
        JsonNode openRouterOff = codec(OpenAiCompatibility.ReasoningFormat.OPENROUTER, true)
                .encodeRequest(MODEL, empty, "off");
        assertEquals("none", openRouterOff.path("reasoning").path("effort").asText());

        JsonNode deepSeek = codec(OpenAiCompatibility.ReasoningFormat.DEEPSEEK, true)
                .encodeRequest(MODEL, empty, "medium");
        assertEquals("enabled", deepSeek.path("thinking").path("type").asText());
        assertEquals("medium", deepSeek.path("reasoning_effort").asText());

        JsonNode qwenTemplate = codec(
                OpenAiCompatibility.ReasoningFormat.QWEN_CHAT_TEMPLATE, false
        ).encodeRequest(MODEL, empty, "off");
        assertTrue(!qwenTemplate.path("chat_template_kwargs")
                .path("enable_thinking").asBoolean());
        assertTrue(qwenTemplate.path("chat_template_kwargs")
                .path("preserve_thinking").asBoolean());

        JsonNode together = codec(OpenAiCompatibility.ReasoningFormat.TOGETHER, true)
                .encodeRequest(MODEL, empty, "high");
        assertTrue(together.path("reasoning").path("enabled").asBoolean());
        assertEquals("high", together.path("reasoning_effort").asText());

        JsonNode stringThinking = codec(
                OpenAiCompatibility.ReasoningFormat.STRING_THINKING, false
        ).encodeRequest(MODEL, empty, "low");
        assertEquals("low", stringThinking.path("thinking").asText());
    }

    private OpenAiChatCodec codec(
            OpenAiCompatibility.ReasoningFormat format,
            boolean supportsEffort
    ) {
        return new OpenAiChatCodec(mapper, new OpenAiCompatibility(
                OpenAiCompatibility.MaxTokensField.MAX_COMPLETION_TOKENS,
                true, format, supportsEffort, true
        ));
    }

    @Test
    void encodesReplaysAndStreamsGrammarCustomTools() throws Exception {
        OpenAiCompatibility grammarCompatibility = new OpenAiCompatibility(
                OpenAiCompatibility.MaxTokensField.MAX_COMPLETION_TOKENS,
                true, OpenAiCompatibility.ReasoningFormat.STANDARD,
                true, true, true, true
        );
        OpenAiChatCodec grammarCodec = new OpenAiChatCodec(mapper, grammarCompatibility);
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
                        "call_custom", "sample_tool", Map.of("payload", "abc")
                )),
                MODEL.api(), MODEL.provider(), MODEL.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1L
        );
        ModelContext context = new ModelContext(
                "", List.of(history), List.of(grammarTool)
        );
        JsonNode request = grammarCodec.encodeRequest(MODEL, context, "off");
        assertEquals("custom", request.path("tools").get(0).path("type").asText());
        assertEquals("lark", request.path("tools").get(0).path("custom")
                .path("format").path("grammar").path("syntax").asText());
        assertEquals("custom", request.path("messages").get(0)
                .path("tool_calls").get(0).path("type").asText());
        assertEquals("abc", request.path("messages").get(0)
                .path("tool_calls").get(0).path("custom").path("input").asText());

        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"id\":\"chatcmpl-custom\",\"choices\":[{\"delta\":{"
                        + "\"tool_calls\":[{\"index\":0,\"id\":\"call_custom\",\"type\":\"custom\","
                        + "\"custom\":{\"name\":\"sample_tool\",\"input\":\"ab\"}}]},"
                        + "\"finish_reason\":null}]}"),
                data("{\"id\":\"chatcmpl-custom\",\"choices\":[{\"delta\":{"
                        + "\"tool_calls\":[{\"index\":0,\"custom\":{\"input\":\"c\"}}]},"
                        + "\"finish_reason\":null}]}"),
                data("{\"id\":\"chatcmpl-custom\",\"choices\":[{\"delta\":{},"
                        + "\"finish_reason\":\"tool_calls\"}]}"),
                data("[DONE]")
        );
        Map<String, OpenAiGrammar.Grammar> grammars = OpenAiGrammar.resolveAll(
                mapper, List.of(grammarTool), true
        );
        List<AssistantStreamEvent> events = grammarCodec.decode(frames, MODEL, grammars)
                .collect().asList().await().atMost(Duration.ofSeconds(2));
        String argumentsJson = events.stream()
                .filter(AssistantStreamEvent.ContentDelta.class::isInstance)
                .map(AssistantStreamEvent.ContentDelta.class::cast)
                .map(AssistantStreamEvent.ContentDelta::delta)
                .reduce("", String::concat);
        assertEquals(Map.of("payload", "abc"), mapper.convertValue(
                mapper.readTree(argumentsJson),
                mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class)
        ));
        AssistantMessage done = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        ).message();
        ToolCallContent call = assertInstanceOf(
                ToolCallContent.class, done.content().getFirst()
        );
        assertEquals(Map.of("payload", "abc"), call.arguments());
        assertEquals(StopReason.TOOL_USE, done.stopReason());
    }

    @Test
    void resolvesPreferredAndRequiredStrictJsonSchemaTools() {
        ToolDefinition preferred = new ToolDefinition(
                "edit", "Edit",
                Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "path", Map.of("type", "string"),
                                "offset", Map.of("type", "integer")
                        ),
                        "required", List.of("path")
                ),
                new ConstrainedSampling.JsonSchema(ConstrainedSampling.Strictness.PREFER)
        );
        JsonNode function = codec.encodeRequest(
                MODEL, new ModelContext("", List.of(), List.of(preferred)), "off"
        ).path("tools").get(0).path("function");
        assertTrue(function.path("strict").asBoolean());
        JsonNode parameters = function.path("parameters");
        assertTrue(!parameters.path("additionalProperties").asBoolean());
        List<?> requiredNames = mapper.convertValue(
                parameters.path("required"), List.class
        );
        assertEquals(List.of("offset", "path"), requiredNames.stream()
                .map(Object::toString).sorted().toList());
        assertEquals("null", parameters.path("properties").path("offset")
                .path("anyOf").get(1).path("type").asText());

        OpenAiCompatibility noStrict = new OpenAiCompatibility(
                OpenAiCompatibility.MaxTokensField.MAX_TOKENS,
                false, OpenAiCompatibility.ReasoningFormat.NONE,
                false, false, false
        );
        JsonNode fallback = new OpenAiChatCodec(mapper, noStrict).encodeRequest(
                MODEL, new ModelContext("", List.of(), List.of(preferred)), "off"
        ).path("tools").get(0).path("function");
        assertTrue(fallback.path("strict").isMissingNode());
        assertTrue(fallback.path("parameters").path("additionalProperties").isMissingNode());

        ToolDefinition preferredRecursive = new ToolDefinition(
                "preferred_recursive", "Preferred recursive",
                Map.of("type", "object", "$ref", "#/$defs/node"),
                new ConstrainedSampling.JsonSchema(ConstrainedSampling.Strictness.PREFER)
        );
        JsonNode preferredFallback = codec.encodeRequest(
                MODEL,
                new ModelContext("", List.of(), List.of(preferredRecursive)),
                "off"
        ).path("tools").get(0).path("function");
        assertTrue(!preferredFallback.path("strict").asBoolean());
        assertEquals("#/$defs/node", preferredFallback.path("parameters").path("$ref").asText());

        ToolDefinition required = new ToolDefinition(
                "recursive", "Recursive",
                Map.of("type", "object", "$ref", "#/$defs/node"),
                new ConstrainedSampling.JsonSchema(ConstrainedSampling.Strictness.REQUIRE)
        );
        assertThrows(IllegalArgumentException.class, () -> codec.encodeRequest(
                MODEL, new ModelContext("", List.of(), List.of(required)), "off"
        ));
        assertThrows(IllegalArgumentException.class, () ->
                new OpenAiChatCodec(mapper, noStrict).encodeRequest(
                        MODEL, new ModelContext("", List.of(), List.of(required)), "off"
                )
        );
    }

    @Test
    void failsWhenFinishedToolArgumentsAreNotValidJson() {
        Multi<SseEvent> frames = Multi.createFrom().items(
                data("{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"bad\","
                        + "\"function\":{\"name\":\"broken\",\"arguments\":\"{\"}}]},"
                        + "\"finish_reason\":null}]}"),
                data("{\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}"),
                data("[DONE]")
        );

        assertThrows(IllegalArgumentException.class, () -> codec.decode(frames, MODEL)
                .collect().asList().await().atMost(Duration.ofSeconds(2)));
    }

    private static SseEvent data(String value) {
        return new SseEvent("message", value, null, null);
    }
}
