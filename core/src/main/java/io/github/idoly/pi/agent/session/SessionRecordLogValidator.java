package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SessionRecordLogValidator {
    private SessionRecordLogValidator() {
    }

    public static void validate(
            String lane,
            List<SessionRecord> openOperations,
            List<SessionRecord> records,
            List<SessionEntry> entries
    ) {
        if (openOperations.size() > 1) {
            corrupt(
                    RecordLogCorruption.Reason.MULTIPLE_OPEN_OPERATIONS,
                    "Lane " + lane + " has at least two open operations"
            );
        }
        Map<String, SessionEntry> entriesById = new HashMap<>();
        entries.forEach(entry -> entriesById.put(entry.id(), entry));
        Map<String, SessionRecordDraft.OperationStarted> starts = new HashMap<>();
        Map<String, Long> finishedAt = new HashMap<>();
        Map<String, Long> abortedAt = new HashMap<>();
        Map<String, SessionRecord> enqueuedByTarget = new HashMap<>();
        Map<String, SessionRecord> latestAttempt = new HashMap<>();
        Set<String> toolInvocations = new HashSet<>();
        ArrayList<SessionRecord> ordered = new ArrayList<>(records);
        ordered.sort(Comparator.comparingLong(SessionRecord::sequence));

        for (SessionRecord record : ordered) {
            SessionRecordDraft value = record.value();
            if (value instanceof SessionRecordDraft.OperationStarted started) {
                starts.put(started.id(), started);
                validateOperationResult(started, entriesById);
                continue;
            }
            String operationId = operationId(value);
            if (operationId != null) {
                if (!starts.containsKey(operationId)) {
                    corrupt(
                            RecordLogCorruption.Reason.UNKNOWN_OPERATION,
                            "Record " + record.id()
                                    + " references unknown operation " + operationId
                    );
                }
                Long finish = finishedAt.get(operationId);
                if (finish != null && record.sequence() > finish) {
                    corrupt(
                            RecordLogCorruption.Reason.RECORD_AFTER_FINISH,
                            "Record " + record.id()
                                    + " follows the finish of operation " + operationId
                    );
                }
            }
            switch (value) {
                case SessionRecordDraft.OperationFinished finished ->
                        finishedAt.put(finished.runId(), record.sequence());
                case SessionRecordDraft.AbortRequested aborted ->
                        abortedAt.put(aborted.runId(), record.sequence());
                case SessionRecordDraft.StepAttempt attempt -> {
                    validateAttemptReason(attempt);
                    validateAttemptSequence(
                            record, attempt, latestAttempt.get(attempt.runId()), entriesById
                    );
                    validateAttemptResult(attempt, entriesById);
                    latestAttempt.put(attempt.runId(), record);
                }
                case SessionRecordDraft.ToolStarted started ->
                        validateToolStart(started, entriesById, toolInvocations);
                case SessionRecordDraft.QueueEnqueued queued -> {
                    Long aborted = queued.runId() == null ? null : abortedAt.get(queued.runId());
                    if (queued.queue() != SessionRecordDraft.Queue.NEXT_RUN
                            && aborted != null && record.sequence() > aborted) {
                        corrupt(
                                RecordLogCorruption.Reason.QUEUE_AFTER_ABORT,
                                queueName(queued.queue()) + " item " + queued.target().id()
                                        + " was enqueued after abort"
                        );
                    }
                    enqueuedByTarget.put(queued.target().id(), record);
                    validateProvisioned(queued.target(), entriesById);
                }
                case SessionRecordDraft.QueueCancelled cancelled -> {
                    SessionRecord enqueue = enqueuedByTarget.get(cancelled.entryId());
                    SessionRecordDraft.QueueEnqueued queued = enqueue == null ? null
                            : (SessionRecordDraft.QueueEnqueued) enqueue.value();
                    if (enqueue == null || enqueue.sequence() >= record.sequence()
                            || !java.util.Objects.equals(queued.runId(), cancelled.runId())
                            || entriesById.containsKey(cancelled.entryId())) {
                        corrupt(
                                RecordLogCorruption.Reason.INVALID_QUEUE_CANCELLATION,
                                "Queue cancellation " + cancelled.id()
                                        + " has no pending matching enqueue"
                        );
                    }
                }
                case SessionRecordDraft.WriteDeferred deferred ->
                        validateProvisioned(deferred.target(), entriesById);
                default -> { }
            }
        }
    }

    private static void validateOperationResult(
            SessionRecordDraft.OperationStarted record,
            Map<String, SessionEntry> entries
    ) {
        switch (record.intent()) {
            case SessionRecordDraft.OperationIntent.Run run ->
                    run.initialMessages().forEach(target -> validateProvisioned(target, entries));
            case SessionRecordDraft.OperationIntent.Compaction compaction ->
                    validateResult(
                            compaction.resultEntryId(), entries,
                            SessionEntry.Compaction.class, "manual compaction"
                    );
            case SessionRecordDraft.OperationIntent.Navigation navigation -> {
                if (navigation.summaryEntryId() != null) {
                    validateResult(
                            navigation.summaryEntryId(), entries,
                            SessionEntry.BranchSummary.class, "navigation summary"
                    );
                }
            }
        }
    }

    private static void validateAttemptReason(SessionRecordDraft.StepAttempt attempt) {
        if ((attempt.step() == SessionRecordDraft.Step.COMPACTION)
                != (attempt.compactionReason() != null)) {
            corrupt(
                    RecordLogCorruption.Reason.INVALID_COMPACTION_REASON,
                    attempt.step().name().toLowerCase() + " attempt " + attempt.id()
                            + " has no valid compaction reason"
            );
        }
    }

    private static void validateAttemptSequence(
            SessionRecord current,
            SessionRecordDraft.StepAttempt attempt,
            SessionRecord previousRecord,
            Map<String, SessionEntry> entries
    ) {
        SessionRecordDraft.StepAttempt previous = previousRecord == null ? null
                : (SessionRecordDraft.StepAttempt) previousRecord.value();
        SessionEntry previousResult = previous == null
                ? null : entries.get(previous.resultEntryId());
        boolean continues = previous != null && previous.step() == attempt.step()
                && (previousResult == null || previousResult.sequence() >= current.sequence());
        int expected = continues ? previous.attempt() + 1 : 1;
        if (attempt.attempt() != expected) {
            corrupt(
                    RecordLogCorruption.Reason.NON_CONSECUTIVE_ATTEMPT,
                    attempt.step().name().toLowerCase() + " attempt " + attempt.id()
                            + " is " + attempt.attempt() + "; expected " + expected
            );
        }
        if (!continues || previous == null
                || attempt.step() == SessionRecordDraft.Step.ASSISTANT) return;
        if (!attempt.resultEntryId().equals(previous.resultEntryId())) {
            corrupt(
                    RecordLogCorruption.Reason.INCONSISTENT_STEP,
                    attempt.step().name().toLowerCase()
                            + " attempts disagree on their result entry id"
            );
        }
        if (attempt.compactionReason() != previous.compactionReason()) {
            corrupt(
                    RecordLogCorruption.Reason.INCONSISTENT_STEP,
                    attempt.step().name().toLowerCase()
                            + " attempts disagree on their compaction reason"
            );
        }
    }

    private static void validateAttemptResult(
            SessionRecordDraft.StepAttempt attempt,
            Map<String, SessionEntry> entries
    ) {
        Class<? extends SessionEntry> expected = switch (attempt.step()) {
            case ASSISTANT -> SessionEntry.Message.class;
            case COMPACTION -> SessionEntry.Compaction.class;
            case BRANCH_SUMMARY -> SessionEntry.BranchSummary.class;
        };
        SessionEntry entry = entries.get(attempt.resultEntryId());
        boolean matches = entry == null || expected.isInstance(entry);
        if (matches && attempt.step() == SessionRecordDraft.Step.ASSISTANT
                && entry instanceof SessionEntry.Message message) {
            matches = message.message() instanceof AssistantMessage;
        }
        if (!matches) {
            corrupt(
                    RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                    "Provisioned " + attempt.step().name().toLowerCase()
                            + " result entry " + attempt.resultEntryId()
                            + " exists with different content"
            );
        }
    }

    private static void validateToolStart(
            SessionRecordDraft.ToolStarted started,
            Map<String, SessionEntry> entries,
            Set<String> invocations
    ) {
        String invocation = started.assistantEntryId() + '\0' + started.toolIndex();
        if (!invocations.add(invocation)) {
            corrupt(
                    RecordLogCorruption.Reason.DUPLICATE_TOOL_INVOCATION,
                    "Tool invocation " + started.assistantEntryId() + ':'
                            + started.toolIndex() + " is duplicated"
            );
        }
        SessionEntry assistantEntry = entries.get(started.assistantEntryId());
        if (!(assistantEntry instanceof SessionEntry.Message message)
                || !(message.message() instanceof AssistantMessage assistant)) {
            corrupt(
                    RecordLogCorruption.Reason.TOOL_CALL_MISMATCH,
                    "Tool start " + started.id()
                            + " does not reference an assistant entry"
            );
        }
        AssistantMessage assistant = (AssistantMessage)
                ((SessionEntry.Message) assistantEntry).message();
        List<ToolCallContent> calls = assistant.content().stream()
                .filter(ToolCallContent.class::isInstance)
                .map(ToolCallContent.class::cast).toList();
        ToolCallContent call = started.toolIndex() < calls.size()
                ? calls.get(started.toolIndex()) : null;
        if (call == null || !call.id().equals(started.toolCallId())
                || !call.name().equals(started.toolName())) {
            corrupt(
                    RecordLogCorruption.Reason.TOOL_CALL_MISMATCH,
                    "Tool start " + started.id()
                            + " does not match its assistant tool-call ordinal"
            );
        }
        SessionEntry result = entries.get(started.resultEntryId());
        if (result != null && (!(result instanceof SessionEntry.Message resultMessage)
                || !(resultMessage.message() instanceof ToolResultMessage toolResult)
                || !toolResult.toolCallId().equals(started.toolCallId())
                || !toolResult.toolName().equals(started.toolName()))) {
            corrupt(
                    RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                    "Provisioned tool result entry " + started.resultEntryId()
                            + " exists with different content"
            );
        }
    }

    private static void validateProvisioned(
            SessionEntryDraft target,
            Map<String, SessionEntry> entries
    ) {
        SessionEntry entry = entries.get(target.id());
        if (entry != null && !matches(entry, target)) {
            corrupt(
                    RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                    "Provisioned entry " + target.id()
                            + " exists with content different from its intent"
            );
        }
    }

    private static boolean matches(SessionEntry entry, SessionEntryDraft draft) {
        return switch (draft) {
            case SessionEntryDraft.Message value -> entry instanceof SessionEntry.Message stored
                    && stored.message().equals(value.message())
                    && stored.terminate() == value.terminate();
            case SessionEntryDraft.ModelChange value -> entry instanceof SessionEntry.ModelChange stored
                    && stored.provider().equals(value.provider())
                    && stored.modelId().equals(value.modelId());
            case SessionEntryDraft.ThinkingLevelChange value ->
                    entry instanceof SessionEntry.ThinkingLevelChange stored
                            && stored.thinkingLevel().equals(value.thinkingLevel());
            case SessionEntryDraft.ActiveToolsChange value ->
                    entry instanceof SessionEntry.ActiveToolsChange stored
                            && stored.activeToolNames().equals(value.activeToolNames());
            case SessionEntryDraft.Compaction value -> entry instanceof SessionEntry.Compaction stored
                    && stored.summary().equals(value.summary())
                    && stored.retainedTail().equals(value.retainedTail())
                    && stored.tokensBefore() == value.tokensBefore()
                    && java.util.Objects.equals(stored.details(), value.details())
                    && java.util.Objects.equals(stored.usage(), value.usage());
            case SessionEntryDraft.BranchSummary value ->
                    entry instanceof SessionEntry.BranchSummary stored
                            && stored.fromId().equals(value.fromId())
                            && stored.summary().equals(value.summary())
                            && java.util.Objects.equals(stored.details(), value.details())
                            && java.util.Objects.equals(stored.usage(), value.usage());
            case SessionEntryDraft.Custom value -> entry instanceof SessionEntry.Custom stored
                    && stored.customType().equals(value.customType())
                    && java.util.Objects.equals(stored.data(), value.data());
        };
    }

    private static void validateResult(
            String id,
            Map<String, SessionEntry> entries,
            Class<? extends SessionEntry> type,
            String description
    ) {
        SessionEntry entry = entries.get(id);
        if (entry != null && !type.isInstance(entry)) {
            corrupt(
                    RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                    "Provisioned " + description + " entry " + id
                            + " exists with different content"
            );
        }
    }

    private static String operationId(SessionRecordDraft value) {
        return switch (value) {
            case SessionRecordDraft.OperationStarted ignored -> null;
            case SessionRecordDraft.AbortRequested record -> record.runId();
            case SessionRecordDraft.OperationFinished record -> record.runId();
            case SessionRecordDraft.StepAttempt record -> record.runId();
            case SessionRecordDraft.ToolStarted record -> record.runId();
            case SessionRecordDraft.QueueEnqueued record -> record.runId();
            case SessionRecordDraft.QueueCancelled record -> record.runId();
            case SessionRecordDraft.WriteDeferred record -> record.runId();
            case SessionRecordDraft.UsageRecord record -> record.runId();
        };
    }

    private static String queueName(SessionRecordDraft.Queue queue) {
        return switch (queue) {
            case STEER -> "steer";
            case FOLLOW_UP -> "followUp";
            case NEXT_RUN -> "nextRun";
        };
    }

    private static void corrupt(RecordLogCorruption.Reason reason, String message) {
        throw new RecordLogCorruption(reason, message);
    }
}
