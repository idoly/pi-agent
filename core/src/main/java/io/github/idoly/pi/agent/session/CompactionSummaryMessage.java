package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.Objects;

public record CompactionSummaryMessage(
        String summary,
        long tokensBefore,
        long timestamp
) implements AgentMessage {
    public CompactionSummaryMessage {
        Objects.requireNonNull(summary, "summary");
        if (tokensBefore < 0) throw new IllegalArgumentException("tokensBefore must be non-negative");
    }
}
