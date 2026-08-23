package io.github.idoly.pi.vertx.mistral;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.SseEvent;
import io.github.idoly.pi.vertx.openai.OpenAiChatCodec;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MistralConversationsModelStreamTest {
    @Test
    void normalizesToolIdsAndBuildsNativeUri() {
        assertEquals("1v4j3ru1j", MistralConversationsModelStream.toolId(
                "call.with-invalid/id"
        ));
        assertEquals("1j55k0s7o", MistralConversationsModelStream.toolId("call_1"));
        assertEquals("k4n83c7h0", MistralConversationsModelStream.toolId(""));
        assertEquals("Abc123xyz", MistralConversationsModelStream.toolId("Abc123xyz"));
        assertEquals(
                "https://api.mistral.ai/v1/chat/completions",
                MistralConversationsModelStream.uri(
                        "https://api.mistral.ai"
                ).toString()
        );
    }

    @Test
    void wireRequestMatchesTypeScriptOracle() throws Exception {
        AssistantMessage foreign = new AssistantMessage(
                List.of(new ToolCallContent(
                        "call.with-invalid/id", "lookup", Map.of("q", "x")
                )), "openai-responses", "openai", "foreign-model",
                Usage.ZERO, StopReason.TOOL_USE, null, 2
        );
        LinkedHashMap<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", Map.of("q", Map.of("type", "string")));
        schema.put("required", List.of("q"));
        ModelContext context = new ModelContext(
                "system",
                List.of(
                        UserMessage.text("hello", 1), foreign,
                        new ToolResultMessage(
                                "call.with-invalid/id", "lookup",
                                List.of(new TextContent("result")),
                                Map.of(), null, false, 3
                        )
                ),
                List.of(new ToolDefinition("lookup", "Lookup", schema))
        );
        try (MistralConversationsModelStream stream =
                     new MistralConversationsModelStream()) {
            var actual = stream.encodeRequest(
                    new Model(
                            "mistral-fixture", "Mistral Fixture",
                            "mistral-conversations", "mistral",
                            "https://api.mistral.ai", true,
                            List.of("text"), 128_000, 32_000
                    ),
                    context,
                    new StreamOptions(
                            "session", "fixture-key", "high",
                            CancellationSignal.NONE
                    )
            );
            var fixture = new ObjectMapper().readTree(Path.of(
                    System.getProperty("pi.compatFixtures"),
                    "provider-protocols-0.84.2.json"
            ).toFile());
            assertEquals(fixture.path("mistral").path("wireRequest"), actual);
        }
    }

    @Test
    void decodesMistralThinkingContentArray() {
        OpenAiChatCodec codec = new OpenAiChatCodec(new ObjectMapper());
        Multi<SseEvent> source = Multi.createFrom().items(
                new SseEvent(null, """
                        {"id":"response","choices":[{"delta":{"content":[{"type":"thinking","thinking":[{"type":"text","text":"reason"}]},{"type":"text","text":"answer"}]},"finish_reason":null}]}
                        """.strip(), null, null),
                new SseEvent(null, """
                        {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}
                        """.strip(), null, null),
                new SseEvent(null, "[DONE]", null, null)
        );
        List<AssistantStreamEvent> events = codec.decode(source, model())
                .collect().asList().await().indefinitely();
        AssistantMessage done = ((AssistantStreamEvent.Done)
                events.getLast()).message();
        assertEquals(new ThinkingContent("reason", null), done.content().get(0));
        assertEquals(new TextContent("answer"), done.content().get(1));
        assertEquals(StopReason.STOP, done.stopReason());
        assertEquals(5, done.usage().totalTokens());
    }

    private static Model model() {
        return new Model(
                "mistral-large", "Mistral Large", "mistral-conversations",
                "mistral", "https://api.mistral.ai", true,
                List.of("text"), 128_000, 32_000
        );
    }
}
