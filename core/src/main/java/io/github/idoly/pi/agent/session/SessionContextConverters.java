package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.ContextConverter;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Message;
import io.github.idoly.pi.ai.UserMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class SessionContextConverters {
    public static final String COMPACTION_SUMMARY_PREFIX =
            "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";
    public static final String COMPACTION_SUMMARY_SUFFIX = "\n</summary>";
    public static final String BRANCH_SUMMARY_PREFIX =
            "The following is a summary of a branch that this conversation came back from:\n\n<summary>\n";
    public static final String BRANCH_SUMMARY_SUFFIX = "</summary>";

    private SessionContextConverters() {
    }

    public static ContextConverter standardMessages() {
        return messages -> {
            List<Message> converted = new ArrayList<>();
            for (AgentMessage message : messages) {
                if (message instanceof Message providerMessage) {
                    converted.add(providerMessage);
                } else if (message instanceof CompactionSummaryMessage summary) {
                    converted.add(UserMessage.text(
                            COMPACTION_SUMMARY_PREFIX + summary.summary()
                                    + COMPACTION_SUMMARY_SUFFIX,
                            summary.timestamp()
                    ));
                } else if (message instanceof BranchSummaryMessage summary) {
                    converted.add(UserMessage.text(
                            BRANCH_SUMMARY_PREFIX + summary.summary()
                                    + BRANCH_SUMMARY_SUFFIX,
                            summary.timestamp()
                    ));
                }
            }
            return CompletableFuture.completedFuture(List.copyOf(converted));
        };
    }
}
