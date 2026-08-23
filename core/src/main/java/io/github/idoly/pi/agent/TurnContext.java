package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ToolResultMessage;

import java.util.List;

public record TurnContext(
        AssistantMessage message,
        List<ToolResultMessage> toolResults,
        AgentContext context,
        List<AgentMessage> newMessages
) {
    public TurnContext {
        toolResults = List.copyOf(toolResults);
        newMessages = List.copyOf(newMessages);
    }
}
