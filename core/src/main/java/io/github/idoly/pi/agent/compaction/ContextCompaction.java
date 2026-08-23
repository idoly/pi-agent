package io.github.idoly.pi.agent.compaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.agent.session.BranchSummaryMessage;
import io.github.idoly.pi.agent.session.CompactionSummaryMessage;
import io.github.idoly.pi.agent.session.SessionContextBuilder;
import io.github.idoly.pi.agent.session.SessionEntry;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.Message;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class ContextCompaction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int ESTIMATED_IMAGE_CHARS = 4_800;
    private static final int TOOL_RESULT_MAX_CHARS = 2_000;

    private ContextCompaction() {
    }

    public static long calculateContextTokens(Usage usage) {
        return usage.totalTokens() != 0
                ? usage.totalTokens()
                : usage.input() + usage.output() + usage.cacheRead() + usage.cacheWrite();
    }

    public static boolean shouldCompact(
            long contextTokens,
            long contextWindow,
            CompactionSettings settings
    ) {
        return settings.enabled()
                && contextTokens > contextWindow - settings.reserveTokens();
    }

    public static long estimateTokens(AgentMessage message) {
        long chars = 0;
        if (message instanceof UserMessage user) {
            chars = contentChars(user.content());
        } else if (message instanceof AssistantMessage assistant) {
            for (ContentBlock block : assistant.content()) {
                if (block instanceof TextContent text) chars += text.text().length();
                else if (block instanceof ThinkingContent thinking) {
                    chars += thinking.thinking().length();
                } else if (block instanceof ToolCallContent call) {
                    chars += call.name().length() + safeJson(call.arguments()).length();
                }
            }
        } else if (message instanceof ToolResultMessage result) {
            chars = contentChars(result.content());
        } else if (message instanceof BranchSummaryMessage summary) {
            chars = summary.summary().length();
        } else if (message instanceof CompactionSummaryMessage summary) {
            chars = summary.summary().length();
        }
        return (chars + 3) / 4;
    }

    public static ContextUsageEstimate estimateContextTokens(List<AgentMessage> messages) {
        int usageIndex = -1;
        Usage usage = null;
        for (int index = messages.size() - 1; index >= 0; index--) {
            Usage candidate = assistantUsage(messages.get(index));
            if (candidate != null) {
                usageIndex = index;
                usage = candidate;
                break;
            }
        }
        if (usage == null) {
            long estimated = messages.stream().mapToLong(ContextCompaction::estimateTokens).sum();
            return new ContextUsageEstimate(estimated, 0, estimated, null);
        }
        long usageTokens = calculateContextTokens(usage);
        long trailing = 0;
        for (int index = usageIndex + 1; index < messages.size(); index++) {
            trailing += estimateTokens(messages.get(index));
        }
        return new ContextUsageEstimate(
                usageTokens + trailing, usageTokens, trailing, usageIndex
        );
    }

    public static Usage lastAssistantUsage(List<SessionEntry> entries) {
        for (int index = entries.size() - 1; index >= 0; index--) {
            if (entries.get(index) instanceof SessionEntry.Message message) {
                Usage usage = assistantUsage(message.message());
                if (usage != null) return usage;
            }
        }
        return null;
    }

    public static CutPoint findCutPoint(
            List<SessionEntry> entries,
            int startIndex,
            int endIndex,
            long keepRecentTokens
    ) {
        List<Integer> cutPoints = validCutPoints(entries, startIndex, endIndex);
        if (cutPoints.isEmpty()) return new CutPoint(startIndex, -1, false);
        long accumulated = 0;
        int cutIndex = cutPoints.getFirst();
        for (int index = endIndex - 1; index >= startIndex; index--) {
            if (!(entries.get(index) instanceof SessionEntry.Message message)) continue;
            accumulated += estimateTokens(message.message());
            if (accumulated >= keepRecentTokens) {
                for (int candidate : cutPoints) {
                    if (candidate >= index) {
                        cutIndex = candidate;
                        break;
                    }
                }
                break;
            }
        }
        while (cutIndex > startIndex) {
            SessionEntry previous = entries.get(cutIndex - 1);
            if (previous instanceof SessionEntry.Compaction
                    || previous instanceof SessionEntry.Message) break;
            cutIndex--;
        }
        SessionEntry cutEntry = entries.get(cutIndex);
        boolean user = cutEntry instanceof SessionEntry.Message message
                && message.message() instanceof UserMessage;
        int turnStart = user ? -1 : findTurnStartIndex(entries, cutIndex, startIndex);
        return new CutPoint(cutIndex, turnStart, !user && turnStart != -1);
    }

    public static int findTurnStartIndex(
            List<SessionEntry> entries,
            int entryIndex,
            int startIndex
    ) {
        for (int index = entryIndex; index >= startIndex; index--) {
            SessionEntry entry = entries.get(index);
            if (entry instanceof SessionEntry.BranchSummary) return index;
            if (entry instanceof SessionEntry.Message message
                    && message.message() instanceof UserMessage) return index;
        }
        return -1;
    }

    public static CompactionPreparation prepare(
            List<SessionEntry> pathEntries,
            CompactionSettings settings
    ) {
        if (pathEntries.isEmpty()
                || pathEntries.getLast() instanceof SessionEntry.Compaction) return null;
        int previousIndex = -1;
        for (int index = pathEntries.size() - 1; index >= 0; index--) {
            if (pathEntries.get(index) instanceof SessionEntry.Compaction) {
                previousIndex = index;
                break;
            }
        }
        String previousSummary = null;
        List<SessionEntry> compactable = pathEntries;
        if (previousIndex >= 0) {
            SessionEntry.Compaction previous =
                    (SessionEntry.Compaction) pathEntries.get(previousIndex);
            previousSummary = previous.summary();
            ArrayList<SessionEntry> virtual = new ArrayList<>();
            String parent = previous.id();
            for (int index = 0; index < previous.retainedTail().size(); index++) {
                String id = previous.id() + ":retained:" + index;
                AgentMessage message = previous.retainedTail().get(index);
                virtual.add(new SessionEntry.Message(
                        id, parent, previous.sequence(), message.timestamp(), message, false
                ));
                parent = id;
            }
            virtual.addAll(pathEntries.subList(previousIndex + 1, pathEntries.size()));
            compactable = List.copyOf(virtual);
        }
        long tokensBefore = estimateContextTokens(
                SessionContextBuilder.build(pathEntries).messages()
        ).tokens();
        CutPoint cut = findCutPoint(
                compactable, 0, compactable.size(), settings.keepRecentTokens()
        );
        int historyEnd = cut.splitTurn()
                ? cut.turnStartIndex() : cut.firstKeptEntryIndex();
        List<AgentMessage> history = messages(
                compactable, 0, historyEnd, true
        );
        List<AgentMessage> turnPrefix = cut.splitTurn()
                ? messages(compactable, cut.turnStartIndex(), cut.firstKeptEntryIndex(), true)
                : List.of();
        List<AgentMessage> retained = messages(
                compactable, cut.firstKeptEntryIndex(), compactable.size(), true
        );
        FileOperations operations = new FileOperations();
        if (previousIndex >= 0) {
            inheritFileOperations(
                    ((SessionEntry.Compaction) pathEntries.get(previousIndex)).details(),
                    operations
            );
        }
        history.forEach(message -> extractFileOperations(message, operations));
        turnPrefix.forEach(message -> extractFileOperations(message, operations));
        return new CompactionPreparation(
                history, turnPrefix, retained, cut.splitTurn(), tokensBefore,
                previousSummary, operations, settings
        );
    }

    public static CompletionStage<CompactionResult> compact(
            CompactionPreparation preparation,
            CompactionSummarizer summarizer,
            String customInstructions
    ) {
        if (preparation.splitTurn() && !preparation.turnPrefixMessages().isEmpty()) {
            CompletionStage<CompactionSummarizer.Summary> history;
            if (preparation.messagesToSummarize().isEmpty()) {
                history = CompletableFuture.completedFuture(null);
            } else {
                history = summarizer.summarize(new CompactionSummarizer.Request(
                        CompactionSummarizer.Kind.HISTORY,
                        preparation.messagesToSummarize(), preparation.previousSummary(),
                        customInstructions, floorFraction(
                                preparation.settings().reserveTokens(), 0.8
                        )
                ));
            }
            return history.thenCompose(historySummary -> summarizer.summarize(
                    new CompactionSummarizer.Request(
                            CompactionSummarizer.Kind.TURN_PREFIX,
                            preparation.turnPrefixMessages(), null, null,
                            floorFraction(preparation.settings().reserveTokens(), 0.5)
                    )
            ).thenApply(prefix -> {
                String historyText = historySummary == null
                        ? "No prior history." : historySummary.text();
                Usage usage = historySummary == null
                        ? prefix.usage() : combineUsage(historySummary.usage(), prefix.usage());
                String summary = historyText
                        + "\n\n---\n\n**Turn Context (split turn):**\n\n"
                        + prefix.text();
                return result(preparation, summary, usage);
            }));
        }
        return summarizer.summarize(new CompactionSummarizer.Request(
                CompactionSummarizer.Kind.HISTORY,
                preparation.messagesToSummarize(), preparation.previousSummary(),
                customInstructions,
                floorFraction(preparation.settings().reserveTokens(), 0.8)
        )).thenApply(summary -> result(preparation, summary.text(), summary.usage()));
    }

    public static FileLists computeFileLists(FileOperations operations) {
        TreeSet<String> modified = new TreeSet<>();
        modified.addAll(operations.edited());
        modified.addAll(operations.written());
        TreeSet<String> readOnly = new TreeSet<>(operations.read());
        readOnly.removeAll(modified);
        return new FileLists(List.copyOf(readOnly), List.copyOf(modified));
    }

    public static String formatFileOperations(List<String> readFiles, List<String> modifiedFiles) {
        ArrayList<String> sections = new ArrayList<>();
        if (!readFiles.isEmpty()) {
            sections.add("<read-files>\n" + String.join("\n", readFiles)
                    + "\n</read-files>");
        }
        if (!modifiedFiles.isEmpty()) {
            sections.add("<modified-files>\n" + String.join("\n", modifiedFiles)
                    + "\n</modified-files>");
        }
        return sections.isEmpty() ? "" : "\n\n" + String.join("\n\n", sections);
    }

    public static String serializeConversation(List<? extends Message> messages) {
        ArrayList<String> parts = new ArrayList<>();
        for (Message message : messages) {
            if (message instanceof UserMessage user) {
                String content = contentText(user.content());
                if (!content.isEmpty()) parts.add("[User]: " + content);
            } else if (message instanceof AssistantMessage assistant) {
                ArrayList<String> thinking = new ArrayList<>();
                ArrayList<String> calls = new ArrayList<>();
                for (ContentBlock block : assistant.content()) {
                    if (block instanceof ThinkingContent value) {
                        thinking.add(value.thinking());
                    } else if (block instanceof ToolCallContent call) {
                        ArrayList<String> arguments = new ArrayList<>();
                        for (Map.Entry<String, Object> entry : call.arguments().entrySet()) {
                            arguments.add(entry.getKey() + "=" + safeJson(entry.getValue()));
                        }
                        calls.add(call.name() + "(" + String.join(", ", arguments) + ")");
                    }
                }
                if (!thinking.isEmpty()) {
                    parts.add("[Assistant thinking]: " + String.join("\n", thinking));
                }
                boolean hasText = assistant.content().stream()
                        .anyMatch(TextContent.class::isInstance);
                if (hasText) parts.add("[Assistant]: " + contentText(assistant.content()));
                if (!calls.isEmpty()) {
                    parts.add("[Assistant tool calls]: " + String.join("; ", calls));
                }
            } else if (message instanceof ToolResultMessage result) {
                String content = contentText(result.content());
                if (!content.isEmpty()) {
                    parts.add("[Tool result]: " + truncate(content, TOOL_RESULT_MAX_CHARS));
                }
            }
        }
        return String.join("\n\n", parts);
    }

    public static void extractFileOperations(
            AgentMessage message,
            FileOperations operations
    ) {
        if (!(message instanceof AssistantMessage assistant)) return;
        for (ContentBlock block : assistant.content()) {
            if (!(block instanceof ToolCallContent call)) continue;
            Object pathValue = call.arguments().get("path");
            if (!(pathValue instanceof String path) || path.isEmpty()) continue;
            switch (call.name()) {
                case "read" -> operations.addRead(path);
                case "write" -> operations.addWritten(path);
                case "edit" -> operations.addEdited(path);
                default -> { }
            }
        }
    }

    private static CompactionResult result(
            CompactionPreparation preparation,
            String summary,
            Usage usage
    ) {
        FileLists files = computeFileLists(preparation.fileOperations());
        ObjectNode details = MAPPER.createObjectNode();
        details.set("readFiles", MAPPER.valueToTree(files.readFiles()));
        details.set("modifiedFiles", MAPPER.valueToTree(files.modifiedFiles()));
        return new CompactionResult(
                summary + formatFileOperations(files.readFiles(), files.modifiedFiles()),
                preparation.tokensBefore(), usage, preparation.retainedTail(), details
        );
    }

    private static long floorFraction(long value, double fraction) {
        return (long) Math.floor(value * fraction);
    }

    private static Usage combineUsage(Usage first, Usage second) {
        return new Usage(
                first.input() + second.input(), first.output() + second.output(),
                first.cacheRead() + second.cacheRead(),
                first.cacheWrite() + second.cacheWrite(),
                first.reasoning() + second.reasoning(),
                first.totalTokens() + second.totalTokens(),
                new Cost(
                        first.cost().input() + second.cost().input(),
                        first.cost().output() + second.cost().output(),
                        first.cost().cacheRead() + second.cost().cacheRead(),
                        first.cost().cacheWrite() + second.cost().cacheWrite(),
                        first.cost().total() + second.cost().total()
                )
        );
    }

    private static List<Integer> validCutPoints(
            List<SessionEntry> entries,
            int start,
            int end
    ) {
        ArrayList<Integer> points = new ArrayList<>();
        for (int index = start; index < end; index++) {
            SessionEntry entry = entries.get(index);
            if (entry instanceof SessionEntry.Message message
                    && (message.message() instanceof UserMessage
                    || message.message() instanceof AssistantMessage)) {
                points.add(index);
            }
            if (entry instanceof SessionEntry.BranchSummary) points.add(index);
        }
        return points;
    }

    private static List<AgentMessage> messages(
            List<SessionEntry> entries,
            int start,
            int end,
            boolean excludeCompaction
    ) {
        ArrayList<AgentMessage> messages = new ArrayList<>();
        for (int index = start; index < end; index++) {
            SessionEntry entry = entries.get(index);
            if (entry instanceof SessionEntry.Message message) {
                messages.add(message.message());
            } else if (entry instanceof SessionEntry.BranchSummary summary) {
                messages.add(new BranchSummaryMessage(
                        summary.summary(), summary.fromId(), summary.timestamp()
                ));
            } else if (!excludeCompaction && entry instanceof SessionEntry.Compaction summary) {
                messages.add(new CompactionSummaryMessage(
                        summary.summary(), summary.tokensBefore(), summary.timestamp()
                ));
            }
        }
        return List.copyOf(messages);
    }

    private static void inheritFileOperations(JsonNode details, FileOperations operations) {
        if (details == null || !details.isObject()) return;
        JsonNode read = details.get("readFiles");
        if (read != null && read.isArray()) {
            read.forEach(value -> {
                if (value.isTextual()) operations.addRead(value.textValue());
            });
        }
        JsonNode modified = details.get("modifiedFiles");
        if (modified != null && modified.isArray()) {
            modified.forEach(value -> {
                if (value.isTextual()) operations.addEdited(value.textValue());
            });
        }
    }

    private static Usage assistantUsage(AgentMessage message) {
        if (!(message instanceof AssistantMessage assistant)
                || assistant.stopReason() == StopReason.ABORTED
                || assistant.stopReason() == StopReason.ERROR
                || calculateContextTokens(assistant.usage()) <= 0) return null;
        return assistant.usage();
    }

    private static long contentChars(List<ContentBlock> content) {
        long chars = 0;
        for (ContentBlock block : content) {
            if (block instanceof TextContent text && !text.text().isEmpty()) {
                chars += text.text().length();
            } else if (block instanceof ImageContent) {
                chars += ESTIMATED_IMAGE_CHARS;
            }
        }
        return chars;
    }

    private static String contentText(List<ContentBlock> content) {
        ArrayList<String> text = new ArrayList<>();
        for (ContentBlock block : content) {
            if (block instanceof TextContent value) text.add(value.text());
        }
        return String.join("", text);
    }

    private static String truncate(String text, int max) {
        if (text.length() <= max) return text;
        return text.substring(0, max) + "\n\n[... "
                + (text.length() - max) + " more characters truncated]";
    }

    private static String safeJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException | IllegalArgumentException ignored) {
            return "[unserializable]";
        }
    }

    public record ContextUsageEstimate(
            long tokens,
            long usageTokens,
            long trailingTokens,
            Integer lastUsageIndex
    ) {
    }

    public record CutPoint(
            int firstKeptEntryIndex,
            int turnStartIndex,
            boolean splitTurn
    ) {
    }

    public record FileLists(List<String> readFiles, List<String> modifiedFiles) {
        public FileLists {
            readFiles = List.copyOf(readFiles);
            modifiedFiles = List.copyOf(modifiedFiles);
        }
    }
}
