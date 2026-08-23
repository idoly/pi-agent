package io.github.idoly.pi.vertx.mistral;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.SseEvent;
import io.github.idoly.pi.vertx.openai.OpenAiChatCodec;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MistralConversationsModelStreamTest {
    @Test
    void normalizesToolIdsAndBuildsNativeUri() {
        assertEquals(9, MistralConversationsModelStream.toolId("call-long:id").length());
        assertEquals("Abc123xyz", MistralConversationsModelStream.toolId("Abc123xyz"));
        assertEquals(
                "https://api.mistral.ai/v1/chat/completions",
                MistralConversationsModelStream.uri(
                        "https://api.mistral.ai"
                ).toString()
        );
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
