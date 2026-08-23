package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.CompactionPreparation;
import io.github.idoly.pi.agent.compaction.CompactionResult;
import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.agent.compaction.ContextCompaction;
import io.github.idoly.pi.agent.harness.CompactionSettings;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Durable session-level compaction lifecycle over the record-based v4 store. */
public final class SessionCompactionOperation {
    private SessionCompactionOperation() {
    }

    public static CompletionStage<Outcome> compact(
            AgentSession session,
            Options options,
            CompactionSummarizer summarizer
    ) {
        return compact(session, options, summarizer, null);
    }

    public static CompletionStage<Outcome> compact(
            AgentSession session,
            Options options,
            CompactionSummarizer summarizer,
            SessionOperationEventBus events
    ) {
        Objects.requireNonNull(session, "session");
        Options effective = Objects.requireNonNull(options, "options");
        Objects.requireNonNull(summarizer, "summarizer");
        return accept(session, effective).thenCompose(accepted -> {
            emit(events, new SessionOperationEvent.Started(
                    session.lane(), accepted.runId(),
                    io.github.idoly.pi.agent.harness.SuspendedOperation.Kind.COMPACTION,
                    accepted.sourceLeafId()
            ));
            return observeTerminal(session, events, executeClaimed(
                    session, accepted.runId(), () -> drive(
                            session, accepted, effective.reason(),
                            effective.customInstructions(), summarizer, 1, events
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
        Objects.requireNonNull(summarizer, "summarizer");
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
            if (!(started.intent() instanceof SessionRecordDraft.OperationIntent.Compaction intent)) {
                return failed(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Open operation is not compaction: " + started.id()
                );
            }
            Accepted accepted = new Accepted(
                    started.id(), started.sourceLeafId(), intent.resultEntryId(), null
            );
            return observeTerminal(session, events, executeClaimed(
                    session, started.id(), () -> resumeClaimed(
                            session, accepted, intent, effective, summarizer, events
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

    private static CompletionStage<Accepted> accept(
            AgentSession session,
            Options options
    ) {
        String runId = session.idGenerator().next();
        String resultEntryId = session.idGenerator().next();
        return session.transaction(transaction -> {
            String sourceLeafId = transaction.leafId();
            CompactionPreparation preparation = ContextCompaction.prepare(
                    transaction.findEntriesOnBranch(oldestBranch()), options.settings()
            );
            if (preparation == null) {
                throw new SessionError(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Nothing to compact on lane " + session.lane()
                );
            }
            if (options.reason() == SessionRecordDraft.CompactionReason.THRESHOLD
                    && !ContextCompaction.shouldCompact(
                            preparation.tokensBefore(), options.contextWindow(),
                            options.settings()
                    )) {
                throw new SessionError(
                        SessionError.Code.INVALID_PAYLOAD,
                        "Compaction threshold has not been reached on lane " + session.lane()
                );
            }
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    runId, session.lane(), sourceLeafId,
                    new SessionRecordDraft.OperationIntent.Compaction(
                            options.customInstructions(), resultEntryId
                    )
            ));
            return new Accepted(runId, sourceLeafId, resultEntryId, preparation);
        });
    }

    private static CompletionStage<Outcome> resumeClaimed(
            AgentSession session,
            Accepted accepted,
            SessionRecordDraft.OperationIntent.Compaction intent,
            RecoveryOptions recovery,
            CompactionSummarizer summarizer,
            SessionOperationEventBus events
    ) {
        if (session.state().hasAbortRequest(session.lane(), accepted.runId())) {
            return finishAborted(session, accepted.runId());
        }
        return session.entry(intent.resultEntryId()).thenCompose(existing -> {
            if (existing != null) {
                return CompletableFuture.failedFuture(new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Open compaction " + accepted.runId()
                                + " already has its terminal result entry"
                ));
            }
            return session.findRecords(new SessionRecordQuery(
                    session.lane(), SessionRecordDraft.Type.STEP_ATTEMPT,
                    accepted.runId(), null, null,
                    SessionEntryQuery.Order.OLDEST_FIRST, null
            )).thenCompose(records -> {
                int attempts = 0;
                SessionRecordDraft.CompactionReason durableReason = null;
                for (SessionRecord record : records) {
                    SessionRecordDraft.StepAttempt attempt =
                            (SessionRecordDraft.StepAttempt) record.value();
                    if (attempt.step() != SessionRecordDraft.Step.COMPACTION) continue;
                    attempts = Math.max(attempts, attempt.attempt());
                    durableReason = attempt.compactionReason();
                }
                if (durableReason != null && durableReason != recovery.reason()) {
                    return CompletableFuture.failedFuture(new RecordLogCorruption(
                            RecordLogCorruption.Reason.INCONSISTENT_STEP,
                            "Recovered compaction reason " + recovery.reason()
                                    + " differs from durable reason " + durableReason
                    ));
                }
                if (attempts >= recovery.maxAttempts()) {
                    return finishFailure(
                            session, accepted.runId(),
                            new IllegalStateException(
                                    "Compaction retry attempts exhausted at " + attempts
                            )
                    );
                }
                int nextAttempt = attempts + 1;
                return session.transaction(transaction -> {
                    if (!Objects.equals(
                            transaction.leafId(), accepted.sourceLeafId()
                    )) {
                        throw new RecordLogCorruption(
                                RecordLogCorruption.Reason.INCONSISTENT_STEP,
                                "Compaction source leaf changed while operation was open"
                        );
                    }
                    return ContextCompaction.prepare(
                            transaction.findEntriesOnBranch(oldestBranch()),
                            recovery.settings()
                    );
                }).thenCompose(preparation -> {
                    if (preparation == null) {
                        return CompletableFuture.failedFuture(new RecordLogCorruption(
                                RecordLogCorruption.Reason.INCONSISTENT_STEP,
                                "Open compaction no longer has compactable context"
                        ));
                    }
                    Accepted recovered = new Accepted(
                            accepted.runId(), accepted.sourceLeafId(),
                            accepted.resultEntryId(), preparation
                    );
                    return drive(
                            session, recovered, recovery.reason(),
                            intent.customInstructions(), summarizer, nextAttempt, events
                    );
                });
            });
        });
    }

    private static CompletionStage<Outcome> drive(
            AgentSession session,
            Accepted accepted,
            SessionRecordDraft.CompactionReason reason,
            String customInstructions,
            CompactionSummarizer summarizer,
            int attemptNumber,
            SessionOperationEventBus events
    ) {
        SessionRecordDraft.StepAttempt attempt = new SessionRecordDraft.StepAttempt(
                session.idGenerator().next(), session.lane(), accepted.runId(),
                SessionRecordDraft.Step.COMPACTION, attemptNumber,
                accepted.resultEntryId(), reason
        );
        CompletionStage<Outcome> execution = session.appendRecord(attempt).thenCompose(ignored -> {
            emit(events, new SessionOperationEvent.AttemptStarted(
                    session.lane(), accepted.runId(),
                    SessionRecordDraft.Step.COMPACTION,
                    attemptNumber, reason
            ));
            return ContextCompaction.compact(
                    accepted.preparation(), summarizer, customInstructions
            ).thenCompose(result -> {
                    SessionRecordDraft.UsageRecord usage =
                            new SessionRecordDraft.UsageRecord(
                                    session.idGenerator().next(), session.lane(),
                                    "compaction", result.usage(), accepted.runId(),
                                    null, attemptNumber, null, null, null
                            );
                    return session.appendRecord(usage).thenCompose(usageRecord ->
                            commit(session, accepted, result)
                    );
                });
        });
        return execution.exceptionallyCompose(failure ->
                finishFailure(session, accepted.runId(), unwrap(failure)));
    }

    private static CompletionStage<Outcome> commit(
            AgentSession session,
            Accepted accepted,
            CompactionResult result
    ) {
        session.rawState().reconcileOperationAbort(
                session.lane(), accepted.runId()
        );
        return session.transaction(transaction -> {
            if (transaction.abortRequested(accepted.runId())) {
                transaction.appendRecord(finished(
                        session, accepted.runId(),
                        SessionRecordDraft.OperationOutcome.ABORTED, null
                ));
                return new Outcome.Aborted(accepted.runId(), transaction.leafId());
            }
            if (!Objects.equals(transaction.leafId(), accepted.sourceLeafId())) {
                throw new SessionError(
                        SessionError.Code.STORAGE,
                        "Compaction source leaf changed while operation was active"
                );
            }
            SessionEntry.Compaction entry = (SessionEntry.Compaction) transaction.append(
                    new SessionEntryDraft.Compaction(
                            accepted.resultEntryId(), result.summary(),
                            result.retainedTail(), result.tokensBefore(),
                            result.details(), result.usage()
                    )
            );
            transaction.appendRecord(finished(
                    session, accepted.runId(),
                    SessionRecordDraft.OperationOutcome.COMPLETED, null
            ));
            return new Outcome.Completed(accepted.runId(), entry.id(), entry);
        });
    }

    private static CompletionStage<Outcome> finishAborted(
            AgentSession session,
            String runId
    ) {
        return session.transaction(transaction -> {
            transaction.appendRecord(finished(
                    session, runId, SessionRecordDraft.OperationOutcome.ABORTED, null
            ));
            return new Outcome.Aborted(runId, transaction.leafId());
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
                        session, runId,
                        SessionRecordDraft.OperationOutcome.ABORTED, null
                ));
                return new Outcome.Aborted(runId, transaction.leafId());
            }
            SessionRecordDraft.OperationError error =
                    new SessionRecordDraft.OperationError(
                            failure instanceof SessionError sessionError
                                    ? sessionError.code().name().toLowerCase()
                                    : "compaction_failed",
                            failure.getMessage() == null
                                    ? failure.getClass().getSimpleName()
                                    : failure.getMessage()
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

    private static SessionBranchQuery oldestBranch() {
        return new SessionBranchQuery(
                null, null, null,
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
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
                resultEntryId = completed.entry().id();
            } else if (outcome instanceof Outcome.Aborted) {
                eventOutcome = SessionOperationEvent.Outcome.ABORTED;
            } else {
                eventOutcome = SessionOperationEvent.Outcome.FAILED;
                error = ((Outcome.Failed) outcome).error();
            }
            emit(events, new SessionOperationEvent.Finished(
                    session.lane(), outcome.runId(),
                    io.github.idoly.pi.agent.harness.SuspendedOperation.Kind.COMPACTION,
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
            String resultEntryId,
            CompactionPreparation preparation
    ) {
    }

    public record Options(
            CompactionSettings settings,
            SessionRecordDraft.CompactionReason reason,
            long contextWindow,
            String customInstructions
    ) {
        public Options {
            Objects.requireNonNull(settings, "settings");
            Objects.requireNonNull(reason, "reason");
            if (contextWindow < 0) {
                throw new IllegalArgumentException("contextWindow must not be negative");
            }
            if (reason == SessionRecordDraft.CompactionReason.THRESHOLD
                    && contextWindow == 0) {
                throw new IllegalArgumentException(
                        "contextWindow must be positive for threshold compaction"
                );
            }
        }
    }

    public record RecoveryOptions(
            CompactionSettings settings,
            SessionRecordDraft.CompactionReason reason,
            int maxAttempts
    ) {
        public RecoveryOptions {
            Objects.requireNonNull(settings, "settings");
            Objects.requireNonNull(reason, "reason");
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be positive");
            }
        }
    }

    public sealed interface Outcome {
        String runId();

        String leafId();

        record Completed(
                String runId,
                String leafId,
                SessionEntry.Compaction entry
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
