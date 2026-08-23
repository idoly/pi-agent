package io.github.idoly.pi.agent.compaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.BranchSummaryMessage;
import io.github.idoly.pi.agent.session.CompactionSummaryMessage;
import io.github.idoly.pi.agent.session.SessionBranchQuery;
import io.github.idoly.pi.agent.session.SessionEntry;
import io.github.idoly.pi.agent.session.SessionEntryQuery;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.ToolResultMessage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class BranchSummarization {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String PREAMBLE =
            "The user explored a different conversation branch before returning here.\n"
                    + "Summary of that exploration:\n\n";

    private BranchSummarization() {
    }

    public static CompletionStage<CollectedEntries> collect(
            AgentSession session,
            String oldLeafId,
            String targetId
    ) {
        if (oldLeafId == null) {
            return CompletableFuture.completedFuture(
                    new CollectedEntries(List.of(), null)
            );
        }
        CompletionStage<List<SessionEntry>> oldPath = session.findEntriesOnBranch(
                branch(oldLeafId)
        );
        CompletionStage<List<SessionEntry>> targetPath = session.findEntriesOnBranch(
                branch(targetId)
        );
        return oldPath.thenCombine(targetPath, (oldEntries, targetEntries) -> {
            Set<String> oldIds = new HashSet<>();
            oldEntries.forEach(entry -> oldIds.add(entry.id()));
            String common = null;
            for (SessionEntry entry : targetEntries) {
                if (oldIds.contains(entry.id())) {
                    common = entry.id();
                    break;
                }
            }
            ArrayList<SessionEntry> abandoned = new ArrayList<>();
            for (SessionEntry entry : oldEntries) {
                if (entry.id().equals(common)) break;
                abandoned.add(entry);
            }
            java.util.Collections.reverse(abandoned);
            return new CollectedEntries(abandoned, common);
        });
    }

    public static BranchPreparation prepare(
            List<SessionEntry> entries,
            long tokenBudget
    ) {
        FileOperations operations = new FileOperations();
        for (SessionEntry entry : entries) {
            if (entry instanceof SessionEntry.BranchSummary summary) {
                inherit(summary.details(), operations);
            }
        }
        ArrayList<AgentMessage> messages = new ArrayList<>();
        long total = 0;
        for (int index = entries.size() - 1; index >= 0; index--) {
            SessionEntry entry = entries.get(index);
            AgentMessage message = message(entry);
            if (message == null) continue;
            ContextCompaction.extractFileOperations(message, operations);
            long tokens = ContextCompaction.estimateTokens(message);
            if (tokenBudget > 0 && total + tokens > tokenBudget) {
                if ((entry instanceof SessionEntry.Compaction
                        || entry instanceof SessionEntry.BranchSummary)
                        && total < tokenBudget * 0.9) {
                    messages.addFirst(message);
                    total += tokens;
                }
                break;
            }
            messages.addFirst(message);
            total += tokens;
        }
        return new BranchPreparation(messages, operations, total);
    }

    public static CompletionStage<BranchSummaryResult> summarize(
            List<SessionEntry> entries,
            long tokenBudget,
            long maxTokens,
            String customInstructions,
            CompactionSummarizer summarizer
    ) {
        BranchPreparation preparation = prepare(entries, tokenBudget);
        if (preparation.messages().isEmpty()) {
            return CompletableFuture.completedFuture(new BranchSummaryResult(
                    "No content to summarize", io.github.idoly.pi.ai.Usage.ZERO,
                    List.of(), List.of()
            ));
        }
        return summarizer.summarize(new CompactionSummarizer.Request(
                CompactionSummarizer.Kind.BRANCH, preparation.messages(), null,
                customInstructions, maxTokens
        )).thenApply(summary -> {
            ContextCompaction.FileLists files = ContextCompaction.computeFileLists(
                    preparation.fileOperations()
            );
            String text = PREAMBLE + summary.text()
                    + ContextCompaction.formatFileOperations(
                            files.readFiles(), files.modifiedFiles()
                    );
            return new BranchSummaryResult(
                    text.isEmpty() ? "No summary generated" : text,
                    summary.usage(), files.readFiles(), files.modifiedFiles()
            );
        });
    }

    public static CompletionStage<SessionEntry.BranchSummary> summarizeAndAppend(
            AgentSession session,
            String oldLeafId,
            String targetId,
            long tokenBudget,
            long maxTokens,
            String customInstructions,
            CompactionSummarizer summarizer
    ) {
        return collect(session, oldLeafId, targetId).thenCompose(collected ->
                summarize(
                        collected.entries(), tokenBudget, maxTokens,
                        customInstructions, summarizer
                ).thenCompose(result -> {
                    ObjectNode details = MAPPER.createObjectNode();
                    details.set("readFiles", MAPPER.valueToTree(result.readFiles()));
                    details.set("modifiedFiles", MAPPER.valueToTree(result.modifiedFiles()));
                    return session.appendBranchSummary(
                            oldLeafId, result.summary(), details, result.usage()
                    );
                })
        );
    }

    private static SessionBranchQuery branch(String start) {
        return new SessionBranchQuery(
                start, null, null,
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.NEWEST_FIRST, null, null
                )
        );
    }

    private static AgentMessage message(SessionEntry entry) {
        if (entry instanceof SessionEntry.Message value) {
            return value.message() instanceof ToolResultMessage ? null : value.message();
        }
        if (entry instanceof SessionEntry.BranchSummary value) {
            return new BranchSummaryMessage(
                    value.summary(), value.fromId(), value.timestamp()
            );
        }
        if (entry instanceof SessionEntry.Compaction value) {
            return new CompactionSummaryMessage(
                    value.summary(), value.tokensBefore(), value.timestamp()
            );
        }
        return null;
    }

    private static void inherit(
            com.fasterxml.jackson.databind.JsonNode details,
            FileOperations operations
    ) {
        if (details == null || !details.isObject()) return;
        var read = details.get("readFiles");
        if (read != null && read.isArray()) read.forEach(value -> {
            if (value.isTextual()) operations.addRead(value.textValue());
        });
        var modified = details.get("modifiedFiles");
        if (modified != null && modified.isArray()) modified.forEach(value -> {
            if (value.isTextual()) operations.addEdited(value.textValue());
        });
    }

    public record CollectedEntries(List<SessionEntry> entries, String commonAncestorId) {
        public CollectedEntries { entries = List.copyOf(entries); }
    }

    public record BranchPreparation(
            List<AgentMessage> messages,
            FileOperations fileOperations,
            long totalTokens
    ) {
        public BranchPreparation {
            messages = List.copyOf(messages);
            fileOperations = fileOperations.copy();
        }

        @Override public FileOperations fileOperations() { return fileOperations.copy(); }
    }

    public record BranchSummaryResult(
            String summary,
            io.github.idoly.pi.ai.Usage usage,
            List<String> readFiles,
            List<String> modifiedFiles
    ) {
        public BranchSummaryResult {
            readFiles = List.copyOf(readFiles);
            modifiedFiles = List.copyOf(modifiedFiles);
        }
    }
}
