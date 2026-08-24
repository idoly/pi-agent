package io.github.idoly.pi.vertx.anthropic;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.SseEvent;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AnthropicMessagesCodecTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final AnthropicMessagesCodec codec = new AnthropicMessagesCodec(mapper);

    @Test
    void encodesMessagesToolsThinkingAndSignatures() {
        Model model = model();
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new ThinkingContent("thought", "signature"),
                        new TextContent("answer"),
                        new ToolCallContent("call", "lookup", Map.of("q", "x"))
                ), model.api(), model.provider(), model.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        ToolResultMessage result = new ToolResultMessage(
                "call", "lookup", List.of(new TextContent("result")),
                Map.of(), null, false, 2
        );
        ModelContext context = new ModelContext(
                "system",
                List.of(UserMessage.text("hello", 0), assistant, result),
                List.of(new ToolDefinition(
                        "lookup", "Lookup", Map.of("type", "object")
                ))
        );
        var request = codec.encodeRequest(model, context, "medium");
        assertEquals("model", request.path("model").asText());
        assertEquals("system", request.path("system").get(0)
                .path("text").asText());
        assertEquals("enabled", request.path("thinking").path("type").asText());
        assertEquals(8192, request.path("thinking").path("budget_tokens").asInt());
        assertEquals("signature", request.path("messages").get(1)
                .path("content").get(0).path("signature").asText());
        assertEquals("tool_result", request.path("messages").get(2)
                .path("content").get(0).path("type").asText());
        assertEquals("object", request.path("tools").get(0)
                .path("input_schema").path("type").asText());
    }

    @Test
    void cachesTheLastPlainUserMessageAsATextBlock() {
        var request = codec.encodeRequest(
                model(),
                new ModelContext(
                        "", List.of(UserMessage.text("hello", 0)), List.of()
                ),
                null
        );
        var content = request.path("messages").get(0).path("content");
        assertTrue(content.isArray());
        assertEquals("hello", content.get(0).path("text").asText());
        assertEquals("ephemeral", content.get(0)
                .path("cache_control").path("type").asText());
    }

    @Test
    void decodesTextThinkingToolUsageAndTerminalMessage() {
        Multi<SseEvent> events = Multi.createFrom().items(
                event("message_start", """
                        {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":10,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}}
                        """),
                event("content_block_start", """
                        {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}
                        """),
                event("content_block_delta", """
                        {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"why"}}
                        """),
                event("content_block_delta", """
                        {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}
                        """),
                event("content_block_stop", """
                        {"type":"content_block_stop","index":0}
                        """),
                event("content_block_start", """
                        {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call","name":"lookup","input":{}}}
                        """),
                event("content_block_delta", """
                        {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\\"q\\":\\"x\\"}"}}
                        """),
                event("content_block_stop", """
                        {"type":"content_block_stop","index":1}
                        """),
                event("message_delta", """
                        {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}
                        """),
                event("message_stop", "{" + "\"type\":\"message_stop\"}" )
        );
        List<AssistantStreamEvent> decoded = codec.decode(events, model())
                .collect().asList().await().indefinitely();
        AssistantMessage message = ((AssistantStreamEvent.Done)
                decoded.getLast()).message();
        assertEquals("msg_1", message.responseId());
        assertEquals(StopReason.TOOL_USE, message.stopReason());
        assertEquals(new ThinkingContent("why", "sig"), message.content().get(0));
        assertEquals(new ToolCallContent(
                "call", "lookup", Map.of("q", "x")
        ), message.content().get(1));
        assertEquals(10, message.usage().input());
        assertEquals(7, message.usage().output());
        assertEquals(3, message.usage().cacheRead());
        assertEquals(2, message.usage().cacheWrite());
    }

    @Test
    void rejectsTruncatedToolJsonAndMissingTerminalEvents() {
        Multi<SseEvent> truncated = Multi.createFrom().items(
                event("message_start", """
                        {"type":"message_start","message":{"usage":{}}}
                        """),
                event("content_block_start", """
                        {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"lookup","input":{}}}
                        """),
                event("content_block_delta", """
                        {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\\\"q\\\":"}}
                        """),
                event("content_block_stop", """
                        {"type":"content_block_stop","index":0}
                        """)
        );
        assertThrows(IllegalArgumentException.class, () -> codec.decode(
                truncated, model()
        ).collect().asList().await().indefinitely());

        IllegalStateException missingTerminal = assertThrows(
                IllegalStateException.class,
                () -> codec.decode(
                        Multi.createFrom().empty(), model()
                ).collect().asList().await().indefinitely()
        );
        assertEquals(
                "Anthropic stream ended before message_stop",
                missingTerminal.getMessage()
        );
    }

    @Test
    void ignoresFramesAfterEitherTerminalEvent() {
        List<AssistantStreamEvent> afterError = codec.decode(
                Multi.createFrom().items(
                        event("error", "{\"type\":\"error\",\"error\":{}}"),
                        event("message_stop", "{\"type\":\"message_stop\"}")
                ), model()
        ).collect().asList().await().indefinitely();
        assertEquals(2, afterError.size());
        assertInstanceOf(AssistantStreamEvent.Error.class, afterError.getLast());

        List<AssistantStreamEvent> afterDone = codec.decode(
                Multi.createFrom().items(
                        event("message_start", """
                                {"type":"message_start","message":{"id":"msg","usage":{}}}
                                """),
                        event("message_delta", """
                                {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{}}
                                """),
                        event("message_stop", "{\"type\":\"message_stop\"}"),
                        event("error", "not-json-after-terminal")
                ), model()
        ).collect().asList().await().indefinitely();
        assertEquals(2, afterDone.size());
        assertInstanceOf(AssistantStreamEvent.Done.class, afterDone.getLast());
    }

    @Test
    void emitsTerminalErrorForAnthropicErrorEvent() {
        String data = """
                {"type":"error","error":{"type":"overloaded_error","message":"busy"}}
                """.strip();
        List<AssistantStreamEvent> decoded = codec.decode(
                Multi.createFrom().item(event("error", data)), model()
        ).collect().asList().await().indefinitely();
        assertEquals(2, decoded.size());
        assertInstanceOf(AssistantStreamEvent.Start.class, decoded.getFirst());
        AssistantMessage error = ((AssistantStreamEvent.Error)
                decoded.getLast()).message();
        assertEquals(StopReason.ERROR, error.stopReason());
        assertEquals(data, error.errorMessage());
    }

    private static SseEvent event(String event, String data) {
        return new SseEvent(event, data.strip(), null, null);
    }

    private static Model model() {
        return new Model(
                "model", "Model", "anthropic-messages", "anthropic",
                "https://api.anthropic.com", true,
                List.of("text", "image"), 200_000, 16_384
        );
    }
}
