package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class SessionContextBuilder {
    private SessionContextBuilder() {
    }

    public static SessionContext build(List<SessionEntry> pathEntries) {
        return build(pathEntries, Map.of());
    }

    public static SessionContext build(
            List<SessionEntry> pathEntries,
            Map<String, CustomEntryProjector> projectors
    ) {
        List<SessionEntry> path = List.copyOf(pathEntries);
        Map<String, CustomEntryProjector> safeProjectors = Map.copyOf(projectors);
        String thinkingLevel = "off";
        SessionContext.ModelRef model = null;
        List<String> activeTools = null;
        for (SessionEntry entry : path) {
            if (entry instanceof SessionEntry.ThinkingLevelChange change) {
                thinkingLevel = change.thinkingLevel();
            } else if (entry instanceof SessionEntry.ModelChange change) {
                model = new SessionContext.ModelRef(change.provider(), change.modelId());
            } else if (entry instanceof SessionEntry.Message message
                    && message.message() instanceof AssistantMessage assistant) {
                model = new SessionContext.ModelRef(assistant.provider(), assistant.model());
            } else if (entry instanceof SessionEntry.ActiveToolsChange change) {
                activeTools = change.activeToolNames();
            }
        }

        int boundary = 0;
        for (int index = path.size() - 1; index >= 0; index--) {
            if (path.get(index) instanceof SessionEntry.Compaction) {
                boundary = index;
                break;
            }
        }
        List<SessionEntry> contextEntries = path.subList(boundary, path.size());
        List<AgentMessage> messages = new ArrayList<>();
        for (int index = 0; index < contextEntries.size(); index++) {
            SessionEntry entry = contextEntries.get(index);
            if (entry instanceof SessionEntry.Message message) {
                messages.add(message.message());
            } else if (entry instanceof SessionEntry.Compaction compaction) {
                messages.add(new CompactionSummaryMessage(
                        compaction.summary(), compaction.tokensBefore(), compaction.timestamp()
                ));
                messages.addAll(compaction.retainedTail());
            } else if (entry instanceof SessionEntry.BranchSummary summary
                    && !summary.summary().isEmpty()) {
                messages.add(new BranchSummaryMessage(
                        summary.summary(), summary.fromId(), summary.timestamp()
                ));
            } else if (entry instanceof SessionEntry.Custom custom) {
                CustomEntryProjector projector = safeProjectors.get(custom.customType());
                if (projector != null) {
                    List<AgentMessage> projected = projector.project(
                            custom, index, List.copyOf(contextEntries)
                    );
                    if (projected != null) messages.addAll(List.copyOf(projected));
                }
            }
        }
        return new SessionContext(messages, thinkingLevel, model, activeTools);
    }

    @FunctionalInterface
    public interface CustomEntryProjector {
        List<AgentMessage> project(
                SessionEntry.Custom entry,
                int index,
                List<SessionEntry> contextEntries
        );
    }
}
