package io.github.idoly.pi.testkit;

import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.Usage;

import java.util.List;

public final class ScriptedMessages {
    private ScriptedMessages() {
    }

    public static Model model() {
        return new Model(
                "test-model", "Test Model", "test-api", "test-provider",
                "http://localhost", false, List.of("text"), 128_000, 8_192
        );
    }

    public static AssistantMessage assistant(String text, StopReason reason) {
        List<ContentBlock> content = text == null ? List.of() : List.of(new TextContent(text));
        return new AssistantMessage(
                content, "test-api", "test-provider", "test-model",
                Usage.ZERO, reason, null, 1_700_000_000_000L
        );
    }

    public static AssistantMessage error(String message) {
        return new AssistantMessage(
                List.of(new TextContent("")), "test-api", "test-provider", "test-model",
                Usage.ZERO, StopReason.ERROR, message, 1_700_000_000_000L
        );
    }
}
