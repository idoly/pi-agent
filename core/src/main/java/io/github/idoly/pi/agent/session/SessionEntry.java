package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Objects;

public sealed interface SessionEntry permits
        SessionEntry.Message,
        SessionEntry.ModelChange,
        SessionEntry.ThinkingLevelChange,
        SessionEntry.ActiveToolsChange,
        SessionEntry.Compaction,
        SessionEntry.BranchSummary,
        SessionEntry.Custom {

    String id();

    String parentId();

    long sequence();

    long timestamp();

    Type type();

    enum Type {
        MESSAGE,
        MODEL_CHANGE,
        THINKING_LEVEL_CHANGE,
        ACTIVE_TOOLS_CHANGE,
        COMPACTION,
        BRANCH_SUMMARY,
        CUSTOM
    }

    record Message(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            AgentMessage message,
            boolean terminate
    ) implements SessionEntry {
        public Message {
            common(id, sequence, timestamp);
            Objects.requireNonNull(message, "message");
        }

        @Override
        public Type type() {
            return Type.MESSAGE;
        }
    }

    record ModelChange(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            String provider,
            String modelId
    ) implements SessionEntry {
        public ModelChange {
            common(id, sequence, timestamp);
            requireText(provider, "provider");
            requireText(modelId, "modelId");
        }

        @Override
        public Type type() {
            return Type.MODEL_CHANGE;
        }
    }

    record ThinkingLevelChange(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            String thinkingLevel
    ) implements SessionEntry {
        public ThinkingLevelChange {
            common(id, sequence, timestamp);
            requireText(thinkingLevel, "thinkingLevel");
        }

        @Override
        public Type type() {
            return Type.THINKING_LEVEL_CHANGE;
        }
    }

    record ActiveToolsChange(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            List<String> activeToolNames
    ) implements SessionEntry {
        public ActiveToolsChange {
            common(id, sequence, timestamp);
            activeToolNames = List.copyOf(Objects.requireNonNull(activeToolNames, "activeToolNames"));
            activeToolNames.forEach(name -> requireText(name, "active tool name"));
        }

        @Override
        public Type type() {
            return Type.ACTIVE_TOOLS_CHANGE;
        }
    }

    record Compaction(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            String summary,
            List<AgentMessage> retainedTail,
            long tokensBefore,
            JsonNode details,
            Usage usage
    ) implements SessionEntry {
        public Compaction {
            common(id, sequence, timestamp);
            Objects.requireNonNull(summary, "summary");
            retainedTail = List.copyOf(Objects.requireNonNull(retainedTail, "retainedTail"));
            if (tokensBefore < 0) throw new IllegalArgumentException("tokensBefore must be non-negative");
            details = copy(details);
        }

        public JsonNode details() {
            return copy(details);
        }

        @Override
        public Type type() {
            return Type.COMPACTION;
        }
    }

    record BranchSummary(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            String fromId,
            String summary,
            JsonNode details,
            Usage usage
    ) implements SessionEntry {
        public BranchSummary {
            common(id, sequence, timestamp);
            requireText(fromId, "fromId");
            Objects.requireNonNull(summary, "summary");
            details = copy(details);
        }

        public JsonNode details() {
            return copy(details);
        }

        @Override
        public Type type() {
            return Type.BRANCH_SUMMARY;
        }
    }

    record Custom(
            String id,
            String parentId,
            long sequence,
            long timestamp,
            String customType,
            JsonNode data
    ) implements SessionEntry {
        public Custom {
            common(id, sequence, timestamp);
            requireText(customType, "customType");
            data = copy(data);
        }

        public JsonNode data() {
            return copy(data);
        }

        @Override
        public Type type() {
            return Type.CUSTOM;
        }
    }

    private static void common(String id, long sequence, long timestamp) {
        requireText(id, "id");
        if (sequence <= 0) throw new IllegalArgumentException("sequence must be positive");
        if (timestamp < 0) throw new IllegalArgumentException("timestamp must be non-negative");
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }

    private static JsonNode copy(JsonNode value) {
        return SessionJson.copy(value, "session entry payload");
    }
}
