package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Objects;

public record ModelContext(
        String systemPrompt,
        List<Message> messages,
        List<ToolDefinition> tools
) {
    public ModelContext {
        Objects.requireNonNull(systemPrompt, "systemPrompt");
        messages = List.copyOf(Objects.requireNonNull(messages, "messages"));
        tools = List.copyOf(Objects.requireNonNull(tools, "tools"));
    }

    public ModelContext(String systemPrompt, List<Message> messages) {
        this(systemPrompt, messages, List.of());
    }
}
