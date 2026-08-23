package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Objects;

public sealed interface SessionEntryDraft permits
        SessionEntryDraft.Message,
        SessionEntryDraft.ModelChange,
        SessionEntryDraft.ThinkingLevelChange,
        SessionEntryDraft.ActiveToolsChange,
        SessionEntryDraft.Compaction,
        SessionEntryDraft.BranchSummary,
        SessionEntryDraft.Custom {

    String id();

    record Message(String id, AgentMessage message, boolean terminate) implements SessionEntryDraft {
        public Message {
            requireId(id);
            Objects.requireNonNull(message, "message");
        }

        public Message(String id, AgentMessage message) {
            this(id, message, false);
        }
    }

    record ModelChange(String id, String provider, String modelId) implements SessionEntryDraft {
        public ModelChange {
            requireId(id);
            requireText(provider, "provider");
            requireText(modelId, "modelId");
        }
    }

    record ThinkingLevelChange(String id, String thinkingLevel) implements SessionEntryDraft {
        public ThinkingLevelChange {
            requireId(id);
            requireText(thinkingLevel, "thinkingLevel");
        }
    }

    record ActiveToolsChange(String id, List<String> activeToolNames) implements SessionEntryDraft {
        public ActiveToolsChange {
            requireId(id);
            activeToolNames = List.copyOf(activeToolNames);
        }
    }

    record Compaction(
            String id,
            String summary,
            List<AgentMessage> retainedTail,
            long tokensBefore,
            JsonNode details,
            Usage usage
    ) implements SessionEntryDraft {
        public Compaction {
            requireId(id);
            Objects.requireNonNull(summary, "summary");
            retainedTail = List.copyOf(retainedTail);
            if (tokensBefore < 0) throw new IllegalArgumentException("tokensBefore must be non-negative");
            details = copy(details);
        }

        public JsonNode details() {
            return copy(details);
        }
    }

    record BranchSummary(
            String id,
            String fromId,
            String summary,
            JsonNode details,
            Usage usage
    ) implements SessionEntryDraft {
        public BranchSummary {
            requireId(id);
            requireText(fromId, "fromId");
            Objects.requireNonNull(summary, "summary");
            details = copy(details);
        }

        public JsonNode details() {
            return copy(details);
        }
    }

    record Custom(String id, String customType, JsonNode data) implements SessionEntryDraft {
        public Custom {
            requireId(id);
            requireText(customType, "customType");
            data = copy(data);
        }

        public JsonNode data() {
            return copy(data);
        }
    }

    private static void requireId(String id) {
        requireText(id, "id");
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }

    private static JsonNode copy(JsonNode value) {
        return SessionJson.copy(value, "session entry payload");
    }
}
