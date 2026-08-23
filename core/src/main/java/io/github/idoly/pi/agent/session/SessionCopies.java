package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.UserMessage;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SessionCopies {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SessionCopies() {
    }

    static AgentMessage message(AgentMessage message) {
        if (message instanceof UserMessage user) {
            return new UserMessage(blocks(user.content()), user.timestamp());
        }
        if (message instanceof AssistantMessage assistant) {
            return new AssistantMessage(
                    blocks(assistant.content()), assistant.api(), assistant.provider(), assistant.model(),
                    assistant.usage(), assistant.stopReason(), assistant.errorMessage(), assistant.timestamp(),
                    assistant.responseId(), assistant.rawStopReason()
            );
        }
        if (message instanceof ToolResultMessage result) {
            return new ToolResultMessage(
                    result.toolCallId(), result.toolName(), blocks(result.content()),
                    map(result.details()), result.usage(), result.error(), result.timestamp()
            );
        }
        if (message instanceof CompactionSummaryMessage summary) {
            return new CompactionSummaryMessage(
                    summary.summary(), summary.tokensBefore(), summary.timestamp()
            );
        }
        if (message instanceof BranchSummaryMessage summary) {
            return new BranchSummaryMessage(summary.summary(), summary.fromId(), summary.timestamp());
        }
        throw new SessionError(
                SessionError.Code.INVALID_PAYLOAD,
                "Unregistered agent message type: " + message.getClass().getName()
        );
    }

    static List<AgentMessage> messages(List<AgentMessage> messages) {
        return messages.stream().map(SessionCopies::message).toList();
    }

    static SessionEntryDraft draft(SessionEntryDraft draft) {
        return switch (draft) {
            case SessionEntryDraft.Message value -> new SessionEntryDraft.Message(
                    value.id(), message(value.message()), value.terminate()
            );
            case SessionEntryDraft.ModelChange value -> value;
            case SessionEntryDraft.ThinkingLevelChange value -> value;
            case SessionEntryDraft.ActiveToolsChange value ->
                    new SessionEntryDraft.ActiveToolsChange(value.id(), value.activeToolNames());
            case SessionEntryDraft.Compaction value -> new SessionEntryDraft.Compaction(
                    value.id(), value.summary(), messages(value.retainedTail()),
                    value.tokensBefore(), value.details(), value.usage()
            );
            case SessionEntryDraft.BranchSummary value -> new SessionEntryDraft.BranchSummary(
                    value.id(), value.fromId(), value.summary(), value.details(), value.usage()
            );
            case SessionEntryDraft.Custom value -> new SessionEntryDraft.Custom(
                    value.id(), value.customType(), value.data()
            );
        };
    }

    static SessionEntry entry(SessionEntry entry) {
        return switch (entry) {
            case SessionEntry.Message value -> new SessionEntry.Message(
                    value.id(), value.parentId(), value.sequence(), value.timestamp(),
                    message(value.message()), value.terminate()
            );
            case SessionEntry.ModelChange value -> value;
            case SessionEntry.ThinkingLevelChange value -> value;
            case SessionEntry.ActiveToolsChange value -> new SessionEntry.ActiveToolsChange(
                    value.id(), value.parentId(), value.sequence(), value.timestamp(),
                    value.activeToolNames()
            );
            case SessionEntry.Compaction value -> new SessionEntry.Compaction(
                    value.id(), value.parentId(), value.sequence(), value.timestamp(),
                    value.summary(), messages(value.retainedTail()), value.tokensBefore(),
                    value.details(), value.usage()
            );
            case SessionEntry.BranchSummary value -> new SessionEntry.BranchSummary(
                    value.id(), value.parentId(), value.sequence(), value.timestamp(),
                    value.fromId(), value.summary(), value.details(), value.usage()
            );
            case SessionEntry.Custom value -> new SessionEntry.Custom(
                    value.id(), value.parentId(), value.sequence(), value.timestamp(),
                    value.customType(), value.data()
            );
        };
    }

    static SessionUsageRow usage(SessionUsageRow row) {
        return new SessionUsageRow(
                row.id(), row.sequence(), row.timestamp(), row.usage(), row.entryId(),
                row.adjustment(), row.details()
        );
    }

    private static List<ContentBlock> blocks(List<ContentBlock> blocks) {
        return blocks.stream().map(SessionCopies::block).toList();
    }

    private static ContentBlock block(ContentBlock block) {
        return switch (block) {
            case TextContent text -> new TextContent(
                    text.text(), text.signature()
            );
            case ImageContent image -> new ImageContent(image.data(), image.mimeType());
            case ThinkingContent thinking -> new ThinkingContent(
                    thinking.thinking(), thinking.signature()
            );
            case ToolCallContent call -> new ToolCallContent(
                    call.id(), call.name(), map(call.arguments()), call.signature()
            );
        };
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> map(Map<String, Object> value) {
        if (value == null || value.isEmpty()) return Map.of();
        JsonNode tree = MAPPER.valueToTree(value);
        Map<String, Object> copied = MAPPER.convertValue(tree, LinkedHashMap.class);
        return Map.copyOf(copied);
    }
}
