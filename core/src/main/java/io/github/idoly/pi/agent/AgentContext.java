package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.List;

public record AgentContext(
        String systemPrompt,
        List<AgentMessage> messages,
        List<AgentTool> tools
) {
    public AgentContext {
        messages = List.copyOf(messages);
        tools = List.copyOf(tools);
    }
}
