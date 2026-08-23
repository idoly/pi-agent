package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class JsonlSessionCodec {
    static final int FORMAT_VERSION = 4;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonlSessionCodec() {
    }

    static String encodeHeader(Header header) {
        ObjectNode node = MAPPER.createObjectNode()
                .put("kind", "header")
                .put("version", FORMAT_VERSION)
                .put("id", header.id())
                .put("createdAt", header.createdAt())
                .put("cwd", header.cwd());
        put(node, "parentSessionId", header.parentSessionId());
        if (header.metadata() != null) node.set("metadata", header.metadata());
        return json(node) + "\n";
    }

    static Header decodeHeader(String line) {
        JsonNode node = object(parse(line), "header");
        if (!"header".equals(text(node, "kind"))) invalid("is not a header");
        if (integer(node, "version") != FORMAT_VERSION) {
            invalid("has unsupported session version");
        }
        JsonNode metadata = node.get("metadata");
        if (metadata != null && !metadata.isObject()) invalid("has invalid metadata");
        return new Header(
                text(node, "id"), nonNegativeLong(node, "createdAt"),
                text(node, "cwd"), optionalText(node, "parentSessionId"),
                metadata == null ? null : SessionJson.copy(metadata, "header metadata")
        );
    }

    static String encodeMutation(SessionLogItem mutation) {
        ObjectNode node;
        switch (mutation) {
            case SessionLogItem.Entry item -> {
                node = encodeEntry(item.entry());
                node.put("kind", "entry");
                if (item.lane() != null) node.put("lane", item.lane());
            }
            case SessionLogItem.Record item -> {
                node = encodeRecord(item.record());
                node.put("kind", "record");
            }
            case SessionLogItem.Lane item -> node = MAPPER.createObjectNode()
                    .put("kind", "lane")
                    .put("seq", item.sequence())
                    .put("lane", item.lane());
            case SessionLogItem.Name item -> {
                node = MAPPER.createObjectNode()
                        .put("kind", "fact")
                        .put("seq", item.sequence())
                        .put("fact", "name");
                put(node, "name", item.name());
            }
            case SessionLogItem.Label item -> {
                node = MAPPER.createObjectNode()
                        .put("kind", "fact")
                        .put("seq", item.sequence())
                        .put("fact", "label")
                        .put("targetId", item.targetId());
                put(node, "label", item.label());
            }
            case SessionLogItem.Usage ignored -> throw new IllegalArgumentException(
                    "Legacy usage log items cannot be encoded"
            );
        }
        if (mutation instanceof SessionLogItem.Lane lane) {
            if (lane.leafId() == null) node.putNull("leafId");
            else node.put("leafId", lane.leafId());
        }
        return json(node) + "\n";
    }

    static SessionLogItem decodeMutation(String line) {
        JsonNode node = object(parse(line), "mutation");
        long sequence = positiveLong(node, "seq");
        return switch (text(node, "kind")) {
            case "entry" -> new SessionLogItem.Entry(
                    sequence, decodeEntry(node, sequence), optionalText(node, "lane")
            );
            case "record" -> new SessionLogItem.Record(
                    sequence, decodeRecord(node, sequence)
            );
            case "lane" -> new SessionLogItem.Lane(
                    sequence, text(node, "lane"), nullableText(node, "leafId")
            );
            case "fact" -> decodeFact(node, sequence);
            default -> throw invalid("has unknown mutation kind");
        };
    }

    private static SessionLogItem decodeFact(JsonNode node, long sequence) {
        return switch (text(node, "fact")) {
            case "name" -> new SessionLogItem.Name(sequence, optionalText(node, "name"));
            case "label" -> new SessionLogItem.Label(
                    sequence, text(node, "targetId"), optionalText(node, "label")
            );
            default -> throw invalid("has unknown fact type");
        };
    }

    private static ObjectNode encodeEntry(SessionEntry entry) {
        ObjectNode node = MAPPER.createObjectNode()
                .put("id", entry.id())
                .put("seq", entry.sequence())
                .put("timestamp", entry.timestamp())
                .put("type", entryType(entry.type()));
        if (entry.parentId() == null) node.putNull("parentId");
        else node.put("parentId", entry.parentId());
        switch (entry) {
            case SessionEntry.Message value -> {
                node.set("message", encodeMessage(value.message()));
                if (value.terminate()) node.put("terminate", true);
            }
            case SessionEntry.ModelChange value -> {
                node.put("provider", value.provider());
                node.put("modelId", value.modelId());
            }
            case SessionEntry.ThinkingLevelChange value ->
                    node.put("thinkingLevel", value.thinkingLevel());
            case SessionEntry.ActiveToolsChange value ->
                    node.set("activeToolNames", MAPPER.valueToTree(value.activeToolNames()));
            case SessionEntry.Compaction value -> {
                node.put("summary", value.summary());
                node.set("retainedTail", encodeMessages(value.retainedTail()));
                node.put("tokensBefore", value.tokensBefore());
                if (value.details() != null) node.set("details", value.details());
                if (value.usage() != null) node.set("usage", encodeUsage(value.usage()));
            }
            case SessionEntry.BranchSummary value -> {
                node.put("fromId", value.fromId());
                node.put("summary", value.summary());
                if (value.details() != null) node.set("details", value.details());
                if (value.usage() != null) node.set("usage", encodeUsage(value.usage()));
            }
            case SessionEntry.Custom value -> {
                node.put("customType", value.customType());
                if (value.data() != null) node.set("data", value.data());
            }
        }
        return node;
    }

    private static SessionEntry decodeEntry(JsonNode node, long sequence) {
        String id = text(node, "id");
        String parentId = nullableText(node, "parentId");
        long timestamp = nonNegativeLong(node, "timestamp");
        return switch (text(node, "type")) {
            case "message" -> new SessionEntry.Message(
                    id, parentId, sequence, timestamp, decodeMessage(required(node, "message")),
                    optionalBoolean(node, "terminate", false)
            );
            case "model_change" -> new SessionEntry.ModelChange(
                    id, parentId, sequence, timestamp,
                    text(node, "provider"), text(node, "modelId")
            );
            case "thinking_level_change" -> new SessionEntry.ThinkingLevelChange(
                    id, parentId, sequence, timestamp, text(node, "thinkingLevel")
            );
            case "active_tools_change" -> new SessionEntry.ActiveToolsChange(
                    id, parentId, sequence, timestamp, strings(node, "activeToolNames")
            );
            case "compaction" -> new SessionEntry.Compaction(
                    id, parentId, sequence, timestamp, text(node, "summary"),
                    decodeMessages(required(node, "retainedTail")),
                    nonNegativeLong(node, "tokensBefore"), optionalNode(node, "details"),
                    node.has("usage") ? decodeUsage(node.get("usage")) : null
            );
            case "branch_summary" -> new SessionEntry.BranchSummary(
                    id, parentId, sequence, timestamp, text(node, "fromId"),
                    text(node, "summary"), optionalNode(node, "details"),
                    node.has("usage") ? decodeUsage(node.get("usage")) : null
            );
            case "custom" -> new SessionEntry.Custom(
                    id, parentId, sequence, timestamp, text(node, "customType"),
                    optionalNode(node, "data")
            );
            default -> throw invalid("has unknown entry type " + text(node, "type"));
        };
    }

    private static ObjectNode encodeRecord(SessionRecord record) {
        SessionRecordDraft value = record.storedValue();
        ObjectNode node = MAPPER.createObjectNode()
                .put("id", record.id())
                .put("seq", record.sequence())
                .put("lane", record.lane())
                .put("timestamp", record.timestamp())
                .put("type", recordType(record.type()));
        switch (value) {
            case SessionRecordDraft.OperationStarted item -> {
                if (item.sourceLeafId() == null) node.putNull("sourceLeafId");
                else node.put("sourceLeafId", item.sourceLeafId());
                node.set("intent", encodeIntent(item.intent()));
            }
            case SessionRecordDraft.AbortRequested item -> node.put("runId", item.runId());
            case SessionRecordDraft.OperationFinished item -> {
                node.put("runId", item.runId());
                node.put("outcome", lower(item.outcome().name()));
                if (item.error() != null) node.set("error", MAPPER.valueToTree(item.error()));
            }
            case SessionRecordDraft.StepAttempt item -> {
                node.put("runId", item.runId());
                node.put("step", lower(item.step().name()));
                node.put("attempt", item.attempt());
                node.put("resultEntryId", item.resultEntryId());
                if (item.compactionReason() != null) {
                    node.put("compactionReason", lower(item.compactionReason().name()));
                }
            }
            case SessionRecordDraft.ToolStarted item -> {
                node.put("runId", item.runId());
                node.put("assistantEntryId", item.assistantEntryId());
                node.put("toolIndex", item.toolIndex());
                node.put("toolCallId", item.toolCallId());
                node.put("toolName", item.toolName());
                node.set("effectiveArgs", item.effectiveArgs());
                node.put("resultEntryId", item.resultEntryId());
                node.put("replay", lower(item.replay().name()));
            }
            case SessionRecordDraft.QueueEnqueued item -> {
                node.put("queue", queue(item.queue()));
                put(node, "runId", item.runId());
                node.set("target", encodeDraft(item.target()));
            }
            case SessionRecordDraft.QueueCancelled item -> {
                put(node, "runId", item.runId());
                node.put("entryId", item.entryId());
            }
            case SessionRecordDraft.WriteDeferred item -> {
                node.put("runId", item.runId());
                node.set("target", encodeDraft(item.target()));
            }
            case SessionRecordDraft.UsageRecord item -> {
                node.put("cause", item.cause());
                node.set("usage", encodeUsage(item.usage()));
                put(node, "runId", item.runId());
                put(node, "entryId", item.entryId());
                if (item.attempt() != null) node.put("attempt", item.attempt());
                put(node, "stopReason", item.stopReason());
                put(node, "toolCallId", item.toolCallId());
                if (item.details() != null) node.set("details", item.details());
            }
        }
        return node;
    }

    private static SessionRecord decodeRecord(JsonNode node, long sequence) {
        String id = text(node, "id");
        String lane = text(node, "lane");
        long timestamp = nonNegativeLong(node, "timestamp");
        SessionRecordDraft value = switch (text(node, "type")) {
            case "operation_started" -> new SessionRecordDraft.OperationStarted(
                    id, lane, nullableText(node, "sourceLeafId"),
                    decodeIntent(required(node, "intent"))
            );
            case "abort_requested" -> new SessionRecordDraft.AbortRequested(
                    id, lane, text(node, "runId")
            );
            case "operation_finished" -> new SessionRecordDraft.OperationFinished(
                    id, lane, text(node, "runId"),
                    SessionRecordDraft.OperationOutcome.valueOf(upper(text(node, "outcome"))),
                    node.has("error") ? new SessionRecordDraft.OperationError(
                            text(node.get("error"), "code"), text(node.get("error"), "message")
                    ) : null
            );
            case "step_attempt" -> {
                SessionRecordDraft.Step step = SessionRecordDraft.Step.valueOf(
                        upper(text(node, "step"))
                );
                yield new SessionRecordDraft.StepAttempt(
                        id, lane, text(node, "runId"), step,
                        nonNegativeInt(node, "attempt"), text(node, "resultEntryId"),
                        node.has("compactionReason")
                                ? SessionRecordDraft.CompactionReason.valueOf(
                                        upper(text(node, "compactionReason"))
                                ) : null
                );
            }
            case "tool_started" -> new SessionRecordDraft.ToolStarted(
                    id, lane, text(node, "runId"), text(node, "assistantEntryId"),
                    nonNegativeInt(node, "toolIndex"), text(node, "toolCallId"),
                    text(node, "toolName"), required(node, "effectiveArgs"),
                    text(node, "resultEntryId"),
                    SessionRecordDraft.Replay.valueOf(upper(text(node, "replay")))
            );
            case "queue_enqueued" -> new SessionRecordDraft.QueueEnqueued(
                    id, lane, decodeQueue(text(node, "queue")), optionalText(node, "runId"),
                    decodeDraft(required(node, "target"))
            );
            case "queue_cancelled" -> new SessionRecordDraft.QueueCancelled(
                    id, lane, optionalText(node, "runId"), text(node, "entryId")
            );
            case "write_deferred" -> new SessionRecordDraft.WriteDeferred(
                    id, lane, text(node, "runId"), decodeDraft(required(node, "target"))
            );
            case "usage" -> new SessionRecordDraft.UsageRecord(
                    id, lane, text(node, "cause"), decodeUsage(required(node, "usage")),
                    optionalText(node, "runId"), optionalText(node, "entryId"),
                    node.has("attempt") ? nonNegativeInt(node, "attempt") : null,
                    optionalText(node, "stopReason"), optionalText(node, "toolCallId"),
                    optionalNode(node, "details")
            );
            default -> throw invalid("has unknown record type " + text(node, "type"));
        };
        return new SessionRecord(sequence, timestamp, value);
    }

    private static ObjectNode encodeIntent(SessionRecordDraft.OperationIntent intent) {
        ObjectNode node = MAPPER.createObjectNode();
        switch (intent) {
            case SessionRecordDraft.OperationIntent.Run run -> {
                node.put("kind", "run");
                node.set("originalPrompt", encodeMessages(run.originalPrompt()));
                ArrayNode initial = node.putArray("initialMessages");
                run.initialMessages().forEach(draft -> initial.add(encodeDraft(draft)));
                put(node, "systemPromptOverride", run.systemPromptOverride());
                if (run.resumeData() != null) node.set("resumeData", run.resumeData());
            }
            case SessionRecordDraft.OperationIntent.Compaction compaction -> {
                node.put("kind", "compaction");
                put(node, "customInstructions", compaction.customInstructions());
                node.put("resultEntryId", compaction.resultEntryId());
            }
            case SessionRecordDraft.OperationIntent.Navigation navigation -> {
                node.put("kind", "navigation");
                if (navigation.targetId() == null) node.putNull("targetId");
                else node.put("targetId", navigation.targetId());
                node.put("summarize", navigation.summarize());
                put(node, "customInstructions", navigation.customInstructions());
                put(node, "label", navigation.label());
                put(node, "summaryEntryId", navigation.summaryEntryId());
            }
        }
        return node;
    }

    private static SessionRecordDraft.OperationIntent decodeIntent(JsonNode node) {
        object(node, "intent");
        return switch (text(node, "kind")) {
            case "run" -> {
                ArrayNode initial = array(required(node, "initialMessages"), "initialMessages");
                ArrayList<SessionEntryDraft> drafts = new ArrayList<>();
                initial.forEach(value -> drafts.add(decodeDraft(value)));
                yield new SessionRecordDraft.OperationIntent.Run(
                        decodeMessages(required(node, "originalPrompt")), drafts,
                        optionalText(node, "systemPromptOverride"), optionalNode(node, "resumeData")
                );
            }
            case "compaction" -> new SessionRecordDraft.OperationIntent.Compaction(
                    optionalText(node, "customInstructions"), text(node, "resultEntryId")
            );
            case "navigation" -> new SessionRecordDraft.OperationIntent.Navigation(
                    nullableText(node, "targetId"), optionalBoolean(node, "summarize", false),
                    optionalText(node, "customInstructions"), optionalText(node, "label"),
                    optionalText(node, "summaryEntryId")
            );
            default -> throw invalid("has unknown operation kind " + text(node, "kind"));
        };
    }

    private static ObjectNode encodeDraft(SessionEntryDraft draft) {
        ObjectNode node = MAPPER.createObjectNode().put("id", draft.id());
        switch (draft) {
            case SessionEntryDraft.Message value -> {
                node.put("type", "message");
                node.set("message", encodeMessage(value.message()));
                if (value.terminate()) node.put("terminate", true);
            }
            case SessionEntryDraft.ModelChange value -> {
                node.put("type", "model_change");
                node.put("provider", value.provider());
                node.put("modelId", value.modelId());
            }
            case SessionEntryDraft.ThinkingLevelChange value -> {
                node.put("type", "thinking_level_change");
                node.put("thinkingLevel", value.thinkingLevel());
            }
            case SessionEntryDraft.ActiveToolsChange value -> {
                node.put("type", "active_tools_change");
                node.set("activeToolNames", MAPPER.valueToTree(value.activeToolNames()));
            }
            case SessionEntryDraft.Compaction value -> {
                node.put("type", "compaction");
                node.put("summary", value.summary());
                node.set("retainedTail", encodeMessages(value.retainedTail()));
                node.put("tokensBefore", value.tokensBefore());
                if (value.details() != null) node.set("details", value.details());
                if (value.usage() != null) node.set("usage", encodeUsage(value.usage()));
            }
            case SessionEntryDraft.BranchSummary value -> {
                node.put("type", "branch_summary");
                node.put("fromId", value.fromId());
                node.put("summary", value.summary());
                if (value.details() != null) node.set("details", value.details());
                if (value.usage() != null) node.set("usage", encodeUsage(value.usage()));
            }
            case SessionEntryDraft.Custom value -> {
                node.put("type", "custom");
                node.put("customType", value.customType());
                if (value.data() != null) node.set("data", value.data());
            }
        }
        return node;
    }

    private static SessionEntryDraft decodeDraft(JsonNode node) {
        object(node, "provisioned entry");
        String id = text(node, "id");
        return switch (text(node, "type")) {
            case "message" -> new SessionEntryDraft.Message(
                    id, decodeMessage(required(node, "message")),
                    optionalBoolean(node, "terminate", false)
            );
            case "model_change" -> new SessionEntryDraft.ModelChange(
                    id, text(node, "provider"), text(node, "modelId")
            );
            case "thinking_level_change" -> new SessionEntryDraft.ThinkingLevelChange(
                    id, text(node, "thinkingLevel")
            );
            case "active_tools_change" -> new SessionEntryDraft.ActiveToolsChange(
                    id, strings(node, "activeToolNames")
            );
            case "compaction" -> new SessionEntryDraft.Compaction(
                    id, text(node, "summary"), decodeMessages(required(node, "retainedTail")),
                    nonNegativeLong(node, "tokensBefore"), optionalNode(node, "details"),
                    node.has("usage") ? decodeUsage(node.get("usage")) : null
            );
            case "branch_summary" -> new SessionEntryDraft.BranchSummary(
                    id, text(node, "fromId"), text(node, "summary"),
                    optionalNode(node, "details"),
                    node.has("usage") ? decodeUsage(node.get("usage")) : null
            );
            case "custom" -> new SessionEntryDraft.Custom(
                    id, text(node, "customType"), optionalNode(node, "data")
            );
            default -> throw invalid("has unknown provisioned entry type");
        };
    }

    private static ObjectNode encodeMessage(AgentMessage message) {
        ObjectNode node = MAPPER.createObjectNode().put("timestamp", message.timestamp());
        switch (message) {
            case UserMessage value -> {
                node.put("role", "user");
                node.set("content", encodeBlocks(value.content()));
            }
            case AssistantMessage value -> {
                node.put("role", "assistant");
                node.set("content", encodeBlocks(value.content()));
                node.put("api", value.api());
                node.put("provider", value.provider());
                node.put("model", value.model());
                node.set("usage", encodeUsage(value.usage()));
                node.put("stopReason", encodeStopReason(value.stopReason()));
                put(node, "errorMessage", value.errorMessage());
                put(node, "responseId", value.responseId());
                put(node, "rawStopReason", value.rawStopReason());
            }
            case ToolResultMessage value -> {
                node.put("role", "toolResult");
                node.put("toolCallId", value.toolCallId());
                node.put("toolName", value.toolName());
                node.set("content", encodeBlocks(value.content()));
                node.set("details", MAPPER.valueToTree(value.details()));
                if (value.usage() != null) node.set("usage", encodeUsage(value.usage()));
                node.put("isError", value.error());
            }
            case CompactionSummaryMessage value -> {
                node.put("role", "compactionSummary");
                node.put("summary", value.summary());
                node.put("tokensBefore", value.tokensBefore());
            }
            case BranchSummaryMessage value -> {
                node.put("role", "branchSummary");
                node.put("summary", value.summary());
                node.put("fromId", value.fromId());
            }
            default -> throw invalid("contains an unregistered message type");
        }
        return node;
    }

    private static AgentMessage decodeMessage(JsonNode node) {
        object(node, "message");
        long timestamp = nonNegativeLong(node, "timestamp");
        return switch (text(node, "role")) {
            case "user" -> new UserMessage(decodeBlocks(required(node, "content")), timestamp);
            case "assistant" -> new AssistantMessage(
                    decodeBlocks(required(node, "content")), text(node, "api"),
                    text(node, "provider"), text(node, "model"),
                    decodeUsage(required(node, "usage")),
                    decodeStopReason(text(node, "stopReason")),
                    optionalText(node, "errorMessage"), timestamp,
                    optionalText(node, "responseId"), optionalText(node, "rawStopReason")
            );
            case "toolResult" -> new ToolResultMessage(
                    text(node, "toolCallId"), text(node, "toolName"),
                    decodeBlocks(required(node, "content")), objectMap(node.get("details")),
                    node.has("usage") ? decodeUsage(node.get("usage")) : null,
                    optionalBoolean(node, "isError", false), timestamp
            );
            case "compactionSummary" -> new CompactionSummaryMessage(
                    text(node, "summary"), nonNegativeLong(node, "tokensBefore"), timestamp
            );
            case "branchSummary" -> new BranchSummaryMessage(
                    text(node, "summary"), text(node, "fromId"), timestamp
            );
            default -> throw invalid("has unknown message role " + text(node, "role"));
        };
    }

    private static ArrayNode encodeMessages(List<AgentMessage> messages) {
        ArrayNode values = MAPPER.createArrayNode();
        messages.forEach(message -> values.add(encodeMessage(message)));
        return values;
    }

    private static List<AgentMessage> decodeMessages(JsonNode node) {
        ArrayList<AgentMessage> values = new ArrayList<>();
        array(node, "messages").forEach(value -> values.add(decodeMessage(value)));
        return List.copyOf(values);
    }

    private static ArrayNode encodeBlocks(List<ContentBlock> blocks) {
        ArrayNode values = MAPPER.createArrayNode();
        for (ContentBlock block : blocks) {
            ObjectNode node = MAPPER.createObjectNode();
            switch (block) {
                case TextContent text -> {
                    node.put("type", "text");
                    node.put("text", text.text());
                    put(node, "signature", text.signature());
                }
                case ImageContent image -> {
                    node.put("type", "image");
                    node.put("data", image.data());
                    node.put("mimeType", image.mimeType());
                }
                case ThinkingContent thinking -> {
                    node.put("type", "thinking");
                    node.put("thinking", thinking.thinking());
                    put(node, "signature", thinking.signature());
                }
                case ToolCallContent call -> {
                    node.put("type", "toolCall");
                    node.put("id", call.id());
                    node.put("name", call.name());
                    node.set("arguments", MAPPER.valueToTree(call.arguments()));
                    put(node, "signature", call.signature());
                }
            }
            values.add(node);
        }
        return values;
    }

    private static List<ContentBlock> decodeBlocks(JsonNode value) {
        ArrayList<ContentBlock> blocks = new ArrayList<>();
        for (JsonNode node : array(value, "content")) {
            blocks.add(switch (text(node, "type")) {
                case "text" -> new TextContent(
                        text(node, "text"), optionalText(node, "signature")
                );
                case "image" -> new ImageContent(text(node, "data"), text(node, "mimeType"));
                case "thinking" -> new ThinkingContent(
                        text(node, "thinking"), optionalText(node, "signature")
                );
                case "toolCall" -> new ToolCallContent(
                        text(node, "id"), text(node, "name"),
                        objectMap(required(node, "arguments")),
                        optionalText(node, "signature")
                );
                default -> throw invalid("has unknown content type " + text(node, "type"));
            });
        }
        return List.copyOf(blocks);
    }

    private static ObjectNode encodeUsage(Usage usage) {
        ObjectNode node = MAPPER.createObjectNode()
                .put("input", usage.input())
                .put("output", usage.output())
                .put("cacheRead", usage.cacheRead())
                .put("cacheWrite", usage.cacheWrite())
                .put("totalTokens", usage.totalTokens());
        if (usage.reasoning() != 0) node.put("reasoning", usage.reasoning());
        ObjectNode cost = node.putObject("cost");
        putNumber(cost, "input", usage.cost().input());
        putNumber(cost, "output", usage.cost().output());
        putNumber(cost, "cacheRead", usage.cost().cacheRead());
        putNumber(cost, "cacheWrite", usage.cost().cacheWrite());
        putNumber(cost, "total", usage.cost().total());
        return node;
    }

    private static Usage decodeUsage(JsonNode node) {
        object(node, "usage");
        JsonNode cost = object(required(node, "cost"), "cost");
        return new Usage(
                nonNegativeLong(node, "input"), nonNegativeLong(node, "output"),
                nonNegativeLong(node, "cacheRead"), nonNegativeLong(node, "cacheWrite"),
                node.has("reasoning") ? nonNegativeLong(node, "reasoning") : 0,
                nonNegativeLong(node, "totalTokens"),
                new Cost(
                        number(cost, "input"), number(cost, "output"),
                        number(cost, "cacheRead"), number(cost, "cacheWrite"),
                        number(cost, "total")
                )
        );
    }

    private static String encodeStopReason(StopReason reason) {
        return reason == StopReason.TOOL_USE ? "toolUse" : lower(reason.name());
    }

    private static StopReason decodeStopReason(String reason) {
        return "toolUse".equals(reason) ? StopReason.TOOL_USE
                : StopReason.valueOf(upper(reason));
    }

    private static String entryType(SessionEntry.Type type) {
        return switch (type) {
            case MESSAGE -> "message";
            case MODEL_CHANGE -> "model_change";
            case THINKING_LEVEL_CHANGE -> "thinking_level_change";
            case ACTIVE_TOOLS_CHANGE -> "active_tools_change";
            case COMPACTION -> "compaction";
            case BRANCH_SUMMARY -> "branch_summary";
            case CUSTOM -> "custom";
        };
    }

    private static String recordType(SessionRecordDraft.Type type) {
        return lower(type.name());
    }

    private static String queue(SessionRecordDraft.Queue queue) {
        return switch (queue) {
            case STEER -> "steer";
            case FOLLOW_UP -> "followUp";
            case NEXT_RUN -> "nextRun";
        };
    }

    private static SessionRecordDraft.Queue decodeQueue(String value) {
        return switch (value) {
            case "steer" -> SessionRecordDraft.Queue.STEER;
            case "followUp" -> SessionRecordDraft.Queue.FOLLOW_UP;
            case "nextRun" -> SessionRecordDraft.Queue.NEXT_RUN;
            default -> throw invalid("has unknown queue " + value);
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectMap(JsonNode node) {
        if (node == null || node.isNull()) return Map.of();
        object(node, "object payload");
        return MAPPER.convertValue(node, LinkedHashMap.class);
    }

    private static List<String> strings(JsonNode node, String field) {
        ArrayList<String> values = new ArrayList<>();
        array(required(node, field), field).forEach(value -> {
            if (!value.isTextual()) invalid("has invalid " + field);
            values.add(value.textValue());
        });
        return List.copyOf(values);
    }

    private static JsonNode parse(String line) {
        try {
            return MAPPER.readTree(line);
        } catch (JsonProcessingException error) {
            throw new JsonlSyntaxException("is not valid JSON", error);
        }
    }

    private static String json(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) throw invalid("has invalid " + field);
        return value;
    }

    private static JsonNode optionalNode(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null ? null : SessionJson.copy(value, field);
    }

    private static JsonNode object(JsonNode node, String label) {
        if (node == null || !node.isObject()) throw invalid("has invalid " + label);
        return node;
    }

    private static ArrayNode array(JsonNode node, String label) {
        if (node == null || !node.isArray()) throw invalid("has invalid " + label);
        return (ArrayNode) node;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) throw invalid("has invalid " + field);
        return value.textValue();
    }

    private static String optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null) return null;
        if (!value.isTextual()) throw invalid("has invalid " + field);
        return value.textValue();
    }

    private static String nullableText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null) throw invalid("has invalid " + field);
        if (value.isNull()) return null;
        if (!value.isTextual()) throw invalid("has invalid " + field);
        return value.textValue();
    }

    private static boolean optionalBoolean(JsonNode node, String field, boolean fallback) {
        JsonNode value = node.get(field);
        if (value == null) return fallback;
        if (!value.isBoolean()) throw invalid("has invalid " + field);
        return value.booleanValue();
    }

    private static int integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.canConvertToInt()) throw invalid("has invalid " + field);
        return value.intValue();
    }

    private static int nonNegativeInt(JsonNode node, String field) {
        int value = integer(node, field);
        if (value < 0) throw invalid("has invalid " + field);
        return value;
    }

    private static long positiveLong(JsonNode node, String field) {
        long value = nonNegativeLong(node, field);
        if (value == 0) throw invalid("has invalid " + field);
        return value;
    }

    private static long nonNegativeLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()
                || value.longValue() < 0) {
            throw invalid("has invalid " + field);
        }
        return value.longValue();
    }

    private static double number(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isNumber() || !Double.isFinite(value.doubleValue())) {
            throw invalid("has invalid " + field);
        }
        return value.doubleValue();
    }

    private static void putNumber(ObjectNode node, String field, double value) {
        if (value == Math.rint(value) && value >= Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            node.put(field, (long) value);
        } else {
            node.put(field, value);
        }
    }

    private static void put(ObjectNode node, String field, String value) {
        if (value != null) node.put(field, value);
    }

    private static String lower(String value) {
        return value.toLowerCase(java.util.Locale.ROOT);
    }

    private static String upper(String value) {
        return value.replace('-', '_').toUpperCase(java.util.Locale.ROOT);
    }

    private static SessionError invalid(String message) {
        return new SessionError(SessionError.Code.INVALID_ENTRY, message);
    }

    record Header(
            String id,
            long createdAt,
            String cwd,
            String parentSessionId,
            JsonNode metadata
    ) {
        Header {
            SessionJson.validate(metadata == null
                    ? com.fasterxml.jackson.databind.node.NullNode.getInstance()
                    : metadata, "header metadata");
        }

        @Override
        public JsonNode metadata() {
            return SessionJson.copy(metadata, "header metadata");
        }
    }

    static final class JsonlSyntaxException extends RuntimeException {
        JsonlSyntaxException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
