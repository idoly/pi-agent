package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.Objects;

public record BranchSummaryMessage(
        String summary,
        String fromId,
        long timestamp
) implements AgentMessage {
    public BranchSummaryMessage {
        Objects.requireNonNull(summary, "summary");
        Objects.requireNonNull(fromId, "fromId");
    }
}
