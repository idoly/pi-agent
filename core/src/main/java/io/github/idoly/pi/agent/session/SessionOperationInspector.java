package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.harness.SuspendedOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Read-only reconstruction of open session operations from durable records. */
@ExperimentalSessionApi
public final class SessionOperationInspector {
    private SessionOperationInspector() {
    }

    public static CompletionStage<List<OpenOperation>> inspect(AgentSession session) {
        return session.lanes().thenCompose(lanes -> {
            ArrayList<CompletableFuture<OpenOperation>> pending = new ArrayList<>();
            for (AgentSession.LanePointer lane : lanes) {
                pending.add(inspectLane(session.view(lane.lane()))
                        .toCompletableFuture());
            }
            return CompletableFuture.allOf(
                    pending.toArray(CompletableFuture[]::new)
            ).thenApply(ignored -> pending.stream()
                    .map(CompletableFuture::join)
                    .filter(java.util.Objects::nonNull)
                    .toList());
        });
    }

    public static CompletionStage<OpenOperation> inspectLane(AgentSession session) {
        String lane = session.lane();
        return session.validateRecordLog(lane).thenCompose(ignored ->
                session.findOpenOperations(lane, 2)
        ).thenCompose(open -> {
            if (open.isEmpty()) return CompletableFuture.completedFuture(null);
            SessionRecord startRecord = open.getFirst();
            SessionRecordDraft.OperationStarted started =
                    (SessionRecordDraft.OperationStarted) startRecord.value();
            return session.findRecords(new SessionRecordQuery(
                    lane, null, started.id(), null, null,
                    SessionEntryQuery.Order.OLDEST_FIRST, null
            )).thenApply(records -> reconstruct(
                    session, startRecord, started, records
            ));
        });
    }

    public static CompletionStage<List<SuspendedOperation>> suspended(
            AgentSession session
    ) {
        return inspect(session).thenApply(operations -> operations.stream()
                .map(OpenOperation::asSuspendedOperation)
                .toList());
    }

    public static CompletionStage<List<LastResult>> lastResults(
            AgentSession session
    ) {
        return session.lanes().thenCompose(lanes -> {
            ArrayList<CompletableFuture<LastResult>> pending = new ArrayList<>();
            for (AgentSession.LanePointer lane : lanes) {
                pending.add(lastResult(session.view(lane.lane())).toCompletableFuture());
            }
            return CompletableFuture.allOf(
                    pending.toArray(CompletableFuture[]::new)
            ).thenApply(ignored -> pending.stream()
                    .map(CompletableFuture::join)
                    .filter(java.util.Objects::nonNull)
                    .toList());
        });
    }

    public static CompletionStage<LastResult> lastResult(AgentSession session) {
        String lane = session.lane();
        InMemorySessionState.TerminalOperation terminal;
        try {
            terminal = session.state().latestTerminal(lane);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        if (terminal == null) return CompletableFuture.completedFuture(null);
        SessionRecord finishRecord = terminal.finished();
        SessionRecordDraft.OperationFinished finished =
                (SessionRecordDraft.OperationFinished) finishRecord.value();
        SessionRecord startRecord = terminal.started();
        if (startRecord == null) {
            return CompletableFuture.failedFuture(new RecordLogCorruption(
                    RecordLogCorruption.Reason.UNKNOWN_OPERATION,
                    "Finished operation " + finished.runId()
                            + " has no acceptance record"
            ));
        }
        SessionRecordDraft.OperationStarted started =
                (SessionRecordDraft.OperationStarted) startRecord.value();
        if (!started.id().equals(finished.runId())
                || !started.lane().equals(finishRecord.lane())) {
            return CompletableFuture.failedFuture(new RecordLogCorruption(
                    RecordLogCorruption.Reason.UNKNOWN_OPERATION,
                    "Finished operation " + finished.runId()
                            + " does not match its acceptance record"
            ));
        }
        LastResult result = reconstructLastResult(
                lane, startRecord, started, finishRecord,
                finished, terminal.leafId()
        );
        if (result.resultEntryId() == null) {
            return CompletableFuture.completedFuture(result);
        }
        return session.entry(result.resultEntryId()).thenApply(entry -> {
            validatePublishedResult(started, result.resultEntryId(), entry);
            return result;
        });
    }

    private static OpenOperation reconstruct(
            AgentSession session,
            SessionRecord startRecord,
            SessionRecordDraft.OperationStarted started,
            List<SessionRecord> records
    ) {
        boolean aborting = false;
        SessionRecordDraft.StepAttempt latest = null;
        long latestSequence = Long.MIN_VALUE;
        ArrayList<UnresolvedToolEffect> unresolved = new ArrayList<>();
        for (SessionRecord record : records) {
            SessionRecordDraft value = record.value();
            if (value instanceof SessionRecordDraft.AbortRequested) aborting = true;
            if (value instanceof SessionRecordDraft.StepAttempt attempt
                    && record.sequence() > latestSequence) {
                latest = attempt;
                latestSequence = record.sequence();
            } else if (value instanceof SessionRecordDraft.ToolStarted tool
                    && session.rawState().getEntry(tool.resultEntryId()) == null) {
                unresolved.add(new UnresolvedToolEffect(
                        tool.assistantEntryId(), tool.toolIndex(),
                        tool.toolCallId(), tool.toolName(), tool.resultEntryId(),
                        tool.replay(), tool.replay() == SessionRecordDraft.Replay.SAFE
                                ? ToolRecovery.REPLAY_ALLOWED
                                : ToolRecovery.ADMINISTRATIVE_RESULT_REQUIRED
                ));
            }
        }
        Attempt structuralAttempt = latest == null ? null : new Attempt(
                latest.step(), latest.attempt(), latest.resultEntryId(),
                latest.compactionReason()
        );
        return new OpenOperation(
                started.lane(), started.id(), kind(started.intent()),
                startRecord.timestamp(), started.sourceLeafId(),
                aborting ? Status.ABORTING : Status.SUSPENDED,
                structuralAttempt, List.copyOf(unresolved)
        );
    }

    private static void validatePublishedResult(
            SessionRecordDraft.OperationStarted started,
            String resultEntryId,
            SessionEntry entry
    ) {
        boolean valid = entry != null;
        if (valid && started.intent()
                instanceof SessionRecordDraft.OperationIntent.Compaction) {
            valid = entry instanceof SessionEntry.Compaction;
        } else if (valid && started.intent()
                instanceof SessionRecordDraft.OperationIntent.Navigation navigation
                && navigation.summaryEntryId() != null) {
            valid = entry instanceof SessionEntry.BranchSummary;
        }
        if (!valid) {
            throw new RecordLogCorruption(
                    RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                    "Completed operation " + started.id()
                            + " has no matching published entry " + resultEntryId
            );
        }
    }

    private static LastResult reconstructLastResult(
            String lane,
            SessionRecord startRecord,
            SessionRecordDraft.OperationStarted started,
            SessionRecord finishRecord,
            SessionRecordDraft.OperationFinished finished,
            String leafId
    ) {
        String resultEntryId = null;
        if (finished.outcome() == SessionRecordDraft.OperationOutcome.COMPLETED) {
            resultEntryId = switch (started.intent()) {
                case SessionRecordDraft.OperationIntent.Compaction compaction ->
                        compaction.resultEntryId();
                case SessionRecordDraft.OperationIntent.Navigation navigation ->
                        navigation.summaryEntryId() == null
                                ? navigation.targetId() : navigation.summaryEntryId();
                case SessionRecordDraft.OperationIntent.Run ignored -> null;
            };
        }
        return new LastResult(
                lane, started.id(), kind(started.intent()), finished.outcome(),
                startRecord.timestamp(), finishRecord.timestamp(),
                finishRecord.sequence(), leafId, resultEntryId, finished.error()
        );
    }

    private static SuspendedOperation.Kind kind(
            SessionRecordDraft.OperationIntent intent
    ) {
        return switch (intent.kind()) {
            case RUN -> SuspendedOperation.Kind.RUN;
            case COMPACTION -> SuspendedOperation.Kind.COMPACTION;
            case NAVIGATION -> SuspendedOperation.Kind.NAVIGATION;
        };
    }

    public enum Status { SUSPENDED, ABORTING }

    public enum ToolRecovery {
        REPLAY_ALLOWED,
        ADMINISTRATIVE_RESULT_REQUIRED
    }

    public record LastResult(
            String lane,
            String runId,
            SuspendedOperation.Kind kind,
            SessionRecordDraft.OperationOutcome outcome,
            long startedAt,
            long finishedAt,
            long finishSequence,
            String leafId,
            String resultEntryId,
            SessionRecordDraft.OperationError error
    ) {
    }

    public record Attempt(
            SessionRecordDraft.Step step,
            int attempt,
            String resultEntryId,
            SessionRecordDraft.CompactionReason compactionReason
    ) {
    }

    public record UnresolvedToolEffect(
            String assistantEntryId,
            int toolIndex,
            String toolCallId,
            String toolName,
            String resultEntryId,
            SessionRecordDraft.Replay replay,
            ToolRecovery recovery
    ) {
    }

    public record OpenOperation(
            String lane,
            String id,
            SuspendedOperation.Kind kind,
            long startedAt,
            String sourceLeafId,
            Status status,
            Attempt latestAttempt,
            List<UnresolvedToolEffect> unresolvedTools
    ) {
        public OpenOperation {
            unresolvedTools = List.copyOf(unresolvedTools);
        }

        public SuspendedOperation asSuspendedOperation() {
            return new SuspendedOperation(
                    lane, kind, id, startedAt,
                    SuspendedOperation.Reason.CRASH, List.of(), List.of()
            );
        }
    }
}
