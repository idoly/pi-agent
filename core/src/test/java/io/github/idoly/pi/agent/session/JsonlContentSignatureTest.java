package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonlContentSignatureTest {
    @Test
    void roundTripsProviderSignaturesWithoutChangingNullWireShape() {
        AssistantMessage signed = new AssistantMessage(
                List.of(
                        new TextContent("text", "dGV4dA=="),
                        new ThinkingContent("thinking", "dGhpbmtpbmc="),
                        new ToolCallContent(
                                "call", "tool", Map.of("x", 1), "dG9vbA=="
                        )
                ), "google-generative-ai", "google", "gemini",
                Usage.ZERO, StopReason.TOOL_USE, null, 1
        );
        SessionLogItem item = new SessionLogItem.Entry(
                1, new SessionEntry.Message(
                        "entry", null, 1, 1, signed, false
                ), "main"
        );
        String encoded = JsonlSessionCodec.encodeMutation(item);
        SessionLogItem decoded = JsonlSessionCodec.decodeMutation(encoded);
        assertEquals(item, decoded);
        assertEquals(3, occurrences(encoded, "signature"));

        AssistantMessage unsigned = new AssistantMessage(
                List.of(
                        new TextContent("text"),
                        new ToolCallContent("call", "tool", Map.of())
                ), "openai-responses", "openai", "model",
                Usage.ZERO, StopReason.TOOL_USE, null, 1
        );
        String unsignedJson = JsonlSessionCodec.encodeMutation(
                new SessionLogItem.Entry(
                        1, new SessionEntry.Message(
                                "entry", null, 1, 1, unsigned, false
                        ), "main"
                )
        );
        assertFalse(unsignedJson.contains("signature"));
    }

    private static int occurrences(String value, String target) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(target, offset)) >= 0) {
            count++;
            offset += target.length();
        }
        return count;
    }
}
