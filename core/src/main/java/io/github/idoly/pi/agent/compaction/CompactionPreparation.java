package io.github.idoly.pi.agent.compaction;

import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.ai.AgentMessage;

import java.util.List;

public record CompactionPreparation(
        List<AgentMessage> messagesToSummarize,
        List<AgentMessage> turnPrefixMessages,
        List<AgentMessage> retainedTail,
        boolean splitTurn,
        long tokensBefore,
        String previousSummary,
        FileOperations fileOperations,
        CompactionSettings settings
) {
    public CompactionPreparation {
        messagesToSummarize = List.copyOf(messagesToSummarize);
        turnPrefixMessages = List.copyOf(turnPrefixMessages);
        retainedTail = List.copyOf(retainedTail);
        fileOperations = fileOperations.copy();
    }

    @Override
    public FileOperations fileOperations() {
        return fileOperations.copy();
    }
}
