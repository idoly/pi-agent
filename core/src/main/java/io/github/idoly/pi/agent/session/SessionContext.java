package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.List;

public record SessionContext(
        List<AgentMessage> messages,
        String thinkingLevel,
        ModelRef model,
        List<String> activeToolNames
) {
    public SessionContext {
        messages = List.copyOf(messages);
        thinkingLevel = thinkingLevel == null ? "off" : thinkingLevel;
        activeToolNames = activeToolNames == null ? null : List.copyOf(activeToolNames);
    }

    public record ModelRef(String provider, String modelId) {
    }
}
