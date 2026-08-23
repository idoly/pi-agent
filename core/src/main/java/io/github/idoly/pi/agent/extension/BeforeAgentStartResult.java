package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.List;

/** Chained prompt and system-prompt replacement before an Agent run. */
public record BeforeAgentStartResult(
        List<AgentMessage> prompts,
        String systemPrompt
) {
    public BeforeAgentStartResult {
        prompts = List.copyOf(prompts);
    }
}
