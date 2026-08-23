package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.agent.compaction.BranchSummarization;
import io.github.idoly.pi.agent.compaction.CompactionSummarizer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Durable session-level branch navigation lifecycle. */
public final class SessionNavigation {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SessionNavigation() {
    }

    public static CompletionStage<Outcome> navigate(
            AgentSession session,
            String targetId,
            Options options,
            CompactionSummarizer summarizer
    ) {
        return navigate(session, targetId, options, summarizer, null);
    }

    public static CompletionStage<Outcome> navigate(
            AgentSession session,
            String targetId,
            Options options,
            CompactionSummarizer summarizer,
            SessionOperationEventBus events
    ) {
        Objects.requireNonNull(session, "session");
        Options effective = options == null ? Options.DEFAULT : options;
        if (effective.summarize()) Objects.requireNonNull(summarizer, "summarizer");
        return accept(session, targetId, effective).thenCompose(accepted -> {
            emit(events, new SessionOperationEvent.Started(
                    session.lane(), accepted.runId(),
                    io.github.idoly.pi.agent.harness.SuspendedOperation.Kind.NAVIGATION,
                    accepted.sourceLeafId()
            ));
            return observeTerminal(session, events, executeClaimed(
                    session, accepted.runId(), () -> driveAccepted(
                            session, accepted, targetId,
                            effective, summarizer, 1, events
                    )
            ));
        });
    }

    public static CompletionStage<Outcome> resume(
            AgentSession session,
            RecoveryOptions recovery,
            CompactionSummarizer summarizer
    ) {
        return resume(session, recovery, summarizer, null);
    }

    public static CompletionStage<Outcome> resume(
            AgentSession session,
            RecoveryOptions recovery,
            CompactionSummarizer summarizer,
            SessionOperationEventBus events
    ) {
        Objects.requireNonNull(session, "session");
        RecoveryOptions effective = Objects.requireNonNull(recovery, "recovery");
        return session.validateRecordLog(session.lane()).thenCompose(ignored ->
                session.findOpenOperations(session.lane(), 2)
        ).thenCompose(open -> {
            if (open.isEmpty()) {
                return failed(
                        SessionError.Code.NOT_FOUND,
                        "No open operation on lane " + session.lane()
                );
            }
            SessionRecordDraft.OperationStarted started =
                    (SessionRecordDraft.OperationStarted) open.getFirst().value();
            if (!(started.intent() instanceof SessionRecordDraft.OperationIntent.Navigation intent)) {
                return failed(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Open operation is not navigation: " + started.id()
                );
            }
            if (intent.summarize() != (intent.summaryEntryId() != null)) {
                return CompletableFuture.failedFuture(new RecordLogCorruption(
                        RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                        "Navigation operation " + started.id()
                                + " has inconsistent summary reservation"
                ));
            }
            if (intent.summarize() && summarizer == null) {
                return failed(
                        SessionError.Code.INVALID_PAYLOAD,
                        "A summarizer is required to resume summarized navigation"
                );
            }
            Accepted accepted = new Accepted(
                    started.id(), started.sourceLeafId(), intent.summaryEntryId()
            );
            Options options = new Options(
                    intent.summarize(), intent.customInstructions(), intent.label(),
                    effective.tokenBudget(), effective.maxTokens()
            );
            return observeTerminal(session, events, executeClaimed(
                    session, started.id(), () -> resumeClaimed(
                            session, accepted, intent, options,
                            effective, summarizer, events
                    )
            ));
        });
    }

    public static CompletionStage<Boolean> requestAbort(
            AgentSession session,
            String runId
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(runId, "runId");
        return session.transaction(transaction -> {
            if (!transaction.operationOpen(runId)) {
                throw new SessionError(
                        SessionError.Code.NOT_FOUND,
                        "Open operation not found: " + runId
                );
            }
            if (transaction.abortRequested(runId)) return false;
            transaction.appendRecord(new SessionRecordDraft.AbortRequested(
                    session.idGenerator().next(), session.lane(), runId
            ));
            return true;
        }).thenApply(created -> {
            session.rawState().cancelOperationExecution(session.lane(), runId);
            return created;
        });
    }

    private static CompletionStage<Outcome> resumeClaimed(
            AgentSession session,
            Accepted accepted,
            SessionRecordDraft.OperationIntent.Navigation intent,
            Options options,
            RecoveryOptions recovery,
            CompactionSummarizer summarizer,
            SessionOperationEventBus events
    ) {
        if (session.state().hasAbortRequest(session.lane(), accepted.runId())) {
            return commit(
                    session, accepted.runId(), accepted.sourceLeafId(),
                    intent.targetId(), options, null, null
            );
        }
        if (!intent.summarize()) {
            return driveAccepted(
                    session, accepted, intent.targetId(), options, null, 1, events
            );
        }
        return session.entry(accepted.summaryEntryId()).thenCompose(existing -> {
            if (existing != null) {
                return CompletableFuture.failedFuture(new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Open navigation " + accepted.runId()
                                + " already has its terminal summary entry"
                ));
            }
            return session.findRecords(new SessionRecordQuery(
                    session.lane(), SessionRecordDraft.Type.STEP_ATTEMPT,
                    accepted.runId(), null, null,
                    SessionEntryQuery.Order.OLDEST_FIRST, null
            )).thenCompose(records -> {
                int attempts = records.stream()
                        .map(SessionRecord::value)
                        .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                        .map(SessionRecordDraft.StepAttempt.class::cast)
                        .filter(attempt -> attempt.step()
                                == SessionRecordDraft.Step.BRANCH_SUMMARY)
                        .mapToInt(SessionRecordDraft.StepAttempt::attempt)
                        .max().orElse(0);
                if (attempts >= recovery.maxAttempts()) {
                    return finishFailure(
                            session, accepted.runId(),
                            new IllegalStateException(
                                    "Branch summary retry attempts exhausted at " + attempts
                            )
                    );
                }
                return driveAccepted(
                        session, accepted, intent.targetId(), options,
                        summarizer, attempts + 1, events
                );
            });
        });
    }

    private static CompletionStage<Outcome> driveAccepted(
            AgentSession session,
            Accepted accepted,
            String targetId,
            Options options,
            CompactionSummarizer summarizer,
            int attempt,
            SessionOperationEventBus events
    ) {
        CompletionStage<BranchSummarization.CollectedEntries> collected =
                options.summarize()
                        ? BranchSummarization.collect(
                                session, accepted.sourceLeafId(), targetId
                        )
                        : CompletableFuture.completedFuture(
                                new BranchSummarization.CollectedEntries(List.of(), null)
                        );
        CompletionStage<Outcome> execution = collected.thenCompose(entries ->
                runAccepted(
                        session, accepted, targetId,
                        options, summarizer, entries, attempt, events
                )
        );
        return execution.exceptionallyCompose(failure -> finishFailure(
                session, accepted.runId(), unwrap(failure)
        ));
    }

    private static CompletionStage<Accepted> accept(
            AgentSession session,
            String targetId,
            Options options
    ) {
        String runId = session.idGenerator().next();
        String summaryEntryId = options.summarize()
                ? session.idGenerator().next() : null;
        return session.transaction(transaction -> {
            String sourceLeafId = transaction.leafId();
            if (Objects.equals(sourceLeafId, targetId)) {
                throw new SessionError(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Navigation target is the current lane leaf"
                );
            }
            if (options.summarize() && sourceLeafId == null) {
                throw new SessionError(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Cannot summarize navigation from an empty branch"
                );
            }
            if (options.summarize() && targetId == null) {
                throw new SessionError(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Cannot summarize navigation to the session root"
                );
            }
            if (options.label() != null && targetId == null) {
                throw new SessionError(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Cannot label the session root"
                );
            }
            if (targetId != null && transaction.entry(targetId) == null) {
                throw new SessionError(
                        SessionError.Code.NOT_FOUND, "Entry not found: " + targetId
                );
            }
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    runId, session.lane(), sourceLeafId,
                    new SessionRecordDraft.OperationIntent.Navigation(
                            targetId, options.summarize(),
                            options.customInstructions(), options.label(), summaryEntryId
                    )
            ));
            return new Accepted(runId, sourceLeafId, summaryEntryId);
        });
    }

    private static CompletionStage<Outcome> runAccepted(
            AgentSession session,
            Accepted accepted,
            String targetId,
            Options options,
            CompactionSummarizer summarizer,
            BranchSummarization.CollectedEntries collected,
            int attemptNumber,
            SessionOperationEventBus events
    ) {
        if (!options.summarize()) {
            return commit(
                    session, accepted.runId(), accepted.sourceLeafId(),
                    targetId, options, null, null
            );
        }
        SessionRecordDraft.StepAttempt attempt = new SessionRecordDraft.StepAttempt(
                session.idGenerator().next(), session.lane(), accepted.runId(),
                SessionRecordDraft.Step.BRANCH_SUMMARY, attemptNumber,
                accepted.summaryEntryId(), null
        );
        return session.appendRecord(attempt).thenCompose(attemptRecord -> {
            emit(events, new SessionOperationEvent.AttemptStarted(
                    session.lane(), accepted.runId(),
                    SessionRecordDraft.Step.BRANCH_SUMMARY,
                    attemptNumber, null
            ));
            return BranchSummarization.summarize(
                    collected.entries(), options.tokenBudget(),
                    options.maxTokens(), options.customInstructions(), summarizer
            ).thenCompose(summary -> {
                    SessionRecordDraft.UsageRecord usage =
                            new SessionRecordDraft.UsageRecord(
                                    session.idGenerator().next(), session.lane(),
                                    "branch_summary", summary.usage(), accepted.runId(),
                                    null, attemptNumber, null, null, null
                            );
                    return session.appendRecord(usage).thenCompose(usageRecord ->
                            commit(
                                    session, accepted.runId(), accepted.sourceLeafId(),
                                    targetId, options, accepted.summaryEntryId(), summary
                            )
                    );
                });
        });
    }

    private static CompletionStage<Outcome> commit(
            AgentSession session,
            String runId,
            String sourceLeafId,
            String targetId,
            Options options,
            String summaryEntryId,
            BranchSummarization.BranchSummaryResult summary
    ) {
        session.rawState().reconcileOperationAbort(session.lane(), runId);
        return session.transaction(transaction -> {
            if (transaction.abortRequested(runId)) {
                transaction.appendRecord(finished(
                        session, runId, SessionRecordDraft.OperationOutcome.ABORTED, null
                ));
                return new Outcome.Aborted(runId, transaction.leafId());
            }
            if (!Objects.equals(transaction.leafId(), sourceLeafId)) {
                throw new SessionError(
                        SessionError.Code.STORAGE,
                        "Navigation source leaf changed while operation was active"
                );
            }
            transaction.moveLane(targetId);
            SessionEntry.BranchSummary entry = null;
            if (summary != null) {
                ObjectNode details = MAPPER.createObjectNode();
                details.set("readFiles", MAPPER.valueToTree(summary.readFiles()));
                details.set("modifiedFiles", MAPPER.valueToTree(summary.modifiedFiles()));
                entry = (SessionEntry.BranchSummary) transaction.append(
                        new SessionEntryDraft.BranchSummary(
                                summaryEntryId, sourceLeafId, summary.summary(),
                                details, summary.usage()
                        )
                );
            }
            String leafId = transaction.leafId();
            if (options.label() != null) transaction.label(leafId, options.label());
            transaction.appendRecord(finished(
                    session, runId, SessionRecordDraft.OperationOutcome.COMPLETED, null
            ));
            return new Outcome.Completed(runId, leafId, entry);
        });
    }

    private static CompletionStage<Outcome> finishFailure(
            AgentSession session,
            String runId,
            Throwable failure
    ) {
        session.rawState().reconcileOperationAbort(session.lane(), runId);
        return session.transaction(transaction -> {
            if (transaction.abortRequested(runId)) {
                transaction.appendRecord(finished(
                        session, runId, SessionRecordDraft.OperationOutcome.ABORTED, null
                ));
                return new Outcome.Aborted(runId, transaction.leafId());
            }
            SessionRecordDraft.OperationError error =
                    new SessionRecordDraft.OperationError(
                            failure instanceof SessionError sessionError
                                    ? sessionError.code().name().toLowerCase() : "navigation_failed",
                            failure.getMessage() == null
                                    ? failure.getClass().getSimpleName() : failure.getMessage()
                    );
            transaction.appendRecord(finished(
                    session, runId, SessionRecordDraft.OperationOutcome.FAILED, error
            ));
            return new Outcome.Failed(runId, transaction.leafId(), error);
        });
    }

    private static SessionRecordDraft.OperationFinished finished(
            AgentSession session,
            String runId,
            SessionRecordDraft.OperationOutcome outcome,
            SessionRecordDraft.OperationError error
    ) {
        return new SessionRecordDraft.OperationFinished(
                session.idGenerator().next(), session.lane(), runId, outcome, error
        );
    }

    private static CompletionStage<Outcome> observeTerminal(
            AgentSession session,
            SessionOperationEventBus events,
            CompletionStage<Outcome> stage
    ) {
        return stage.thenApply(outcome -> {
            SessionOperationEvent.Outcome eventOutcome;
            String resultEntryId = null;
            SessionRecordDraft.OperationError error = null;
            if (outcome instanceof Outcome.Completed completed) {
                eventOutcome = SessionOperationEvent.Outcome.COMPLETED;
                resultEntryId = completed.summaryEntry() == null
                        ? null : completed.summaryEntry().id();
            } else if (outcome instanceof Outcome.Aborted) {
                eventOutcome = SessionOperationEvent.Outcome.ABORTED;
            } else {
                eventOutcome = SessionOperationEvent.Outcome.FAILED;
                error = ((Outcome.Failed) outcome).error();
            }
            emit(events, new SessionOperationEvent.Finished(
                    session.lane(), outcome.runId(),
                    io.github.idoly.pi.agent.harness.SuspendedOperation.Kind.NAVIGATION,
                    eventOutcome, outcome.leafId(), resultEntryId, error
            ));
            return outcome;
        });
    }

    private static void emit(
            SessionOperationEventBus events,
            SessionOperationEvent event
    ) {
        if (events != null) events.emit(event);
    }

    private static CompletionStage<Outcome> executeClaimed(
            AgentSession session,
            String runId,
            Supplier<CompletionStage<Outcome>> operation
    ) {
        if (!session.state().claimOperationExecution(session.lane(), runId)) {
            return failed(
                    SessionError.Code.STORAGE,
                    "Operation is already executing: " + runId
            );
        }
        CompletionStage<Outcome> stage;
        try {
            stage = operation.get();
        } catch (Throwable failure) {
            session.rawState().releaseOperationExecution(session.lane(), runId);
            throw failure;
        }
        return stage.whenComplete((ignored, failure) ->
                session.rawState().releaseOperationExecution(session.lane(), runId));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletionStage<T> failed(
            SessionError.Code code,
            String message
    ) {
        return CompletableFuture.failedFuture(new SessionError(code, message));
    }

    private record Accepted(
            String runId,
            String sourceLeafId,
            String summaryEntryId
    ) {
    }

    public record RecoveryOptions(
            long tokenBudget,
            long maxTokens,
            int maxAttempts
    ) {
        public RecoveryOptions {
            if (tokenBudget < 0) {
                throw new IllegalArgumentException("tokenBudget must not be negative");
            }
            if (maxTokens < 0) {
                throw new IllegalArgumentException("maxTokens must not be negative");
            }
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be positive");
            }
        }
    }

    public record Options(
            boolean summarize,
            String customInstructions,
            String label,
            long tokenBudget,
            long maxTokens
    ) {
        public static final Options DEFAULT = new Options(
                false, null, null, 0, 0
        );

        public Options {
            if (tokenBudget < 0) throw new IllegalArgumentException("tokenBudget must not be negative");
            if (maxTokens < 0) throw new IllegalArgumentException("maxTokens must not be negative");
        }
    }

    public sealed interface Outcome {
        String runId();

        String leafId();

        record Completed(
                String runId,
                String leafId,
                SessionEntry.BranchSummary summaryEntry
        ) implements Outcome {
        }

        record Aborted(String runId, String leafId) implements Outcome {
        }

        record Failed(
                String runId,
                String leafId,
                SessionRecordDraft.OperationError error
        ) implements Outcome {
        }
    }
}
