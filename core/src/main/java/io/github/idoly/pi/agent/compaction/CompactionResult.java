package io.github.idoly.pi.agent.compaction;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;

public record CompactionResult(
        String summary,
        long tokensBefore,
        Usage usage,
        List<AgentMessage> retainedTail,
        JsonNode details
) {
    public CompactionResult {
        retainedTail = List.copyOf(retainedTail);
        details = details == null ? null : details.deepCopy();
    }

    @Override
    public JsonNode details() {
        return details == null ? null : details.deepCopy();
    }
}
