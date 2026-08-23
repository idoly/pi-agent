package io.github.idoly.pi.agent.compaction;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface CompactionSummarizer {
    CompletionStage<Summary> summarize(Request request);

    enum Kind { HISTORY, TURN_PREFIX, BRANCH }

    record Request(
            Kind kind,
            List<AgentMessage> messages,
            String previousSummary,
            String customInstructions,
            long maxTokens
    ) {
        public Request {
            messages = List.copyOf(messages);
        }
    }

    record Summary(String text, Usage usage) {
        public Summary {
            if (text == null) throw new NullPointerException("text");
            usage = usage == null ? Usage.ZERO : usage;
        }
    }
}
