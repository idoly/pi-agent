package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Model;

import java.util.List;
import java.util.Set;

public record AgentState(
        String systemPrompt,
        Model model,
        String thinkingLevel,
        List<AgentTool> tools,
        List<AgentMessage> messages,
        boolean streaming,
        AgentMessage streamingMessage,
        Set<String> pendingToolCalls,
        String errorMessage
) {
    public AgentState {
        tools = List.copyOf(tools);
        messages = List.copyOf(messages);
        pendingToolCalls = Set.copyOf(pendingToolCalls);
    }
}
