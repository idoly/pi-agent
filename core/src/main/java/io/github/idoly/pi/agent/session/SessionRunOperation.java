package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.AgentContext;
import io.github.idoly.pi.agent.AgentLoop;
import io.github.idoly.pi.agent.AgentLoopConfig;
import io.github.idoly.pi.agent.AgentLoopRun;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.BeforeToolCallResult;
import io.github.idoly.pi.agent.harness.SuspendedOperation;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Durable prompt, assistant, tool-boundary, and queue lifecycle over the v4 store. */
@ExperimentalSessionApi
public final class SessionRunOperation {
    private SessionRunOperation() {
    }

    public static CompletionStage<Outcome> run(
            AgentSession session,
            List<AgentMessage> prompts,
            Options options
    ) {
        return run(session, prompts, options, null);
    }

    public static CompletionStage<Outcome> run(
            AgentSession session,
            List<AgentMessage> prompts,
            Options options,
            SessionOperationEventBus events
    ) {
        Objects.requireNonNull(session, "session");
        List<AgentMessage> safePrompts = List.copyOf(prompts);
        if (safePrompts.isEmpty()) {
            return failed(SessionError.Code.INVALID_PAYLOAD,
                    "A durable run requires at least one prompt message");
        }
        if (safePrompts.getLast() instanceof AssistantMessage) {
            return failed(SessionError.Code.INVALID_PAYLOAD,
                    "A durable run cannot continue from an assistant prompt");
        }
        Options effective = Objects.requireNonNull(options, "options");
        validateAssistantOnly(effective);
        return accept(session, safePrompts, effective).thenCompose(accepted ->
                launch(session, accepted, effective, events)
        );
    }

    public static CompletionStage<Outcome> runNext(
            AgentSession session,
            Options options,
            Integer limit
    ) {
        return runNext(session, options, limit, null);
    }

    public static CompletionStage<Outcome> runNext(
            AgentSession session,
            Options options,
            Integer limit,
            SessionOperationEventBus events
    ) {
        Objects.requireNonNull(session, "session");
        Options effective = Objects.requireNonNull(options, "options");
        validateAssistantOnly(effective);
        if (limit != null && limit <= 0) {
            return failed(SessionError.Code.INVALID_QUERY,
                    "limit must be positive");
        }
        return acceptNext(session, effective, limit).thenCompose(accepted ->
                launch(session, accepted, effective, events)
        );
    }

    public static CompletionStage<Outcome> resume(
            AgentSession session,
            RecoveryOptions recovery
    ) {
        return resume(session, recovery, null);
    }

    public static CompletionStage<Outcome> resume(
            AgentSession session,
            RecoveryOptions recovery,
            SessionOperationEventBus events
    ) {
        Objects.requireNonNull(session, "session");
        RecoveryOptions effective = Objects.requireNonNull(recovery, "recovery");
        validateAssistantOnly(effective.options());
        return session.validateRecordLog(session.lane()).thenCompose(ignored ->
                session.findOpenOperations(session.lane(), 2)
        ).thenCompose(open -> {
            if (open.isEmpty()) {
                return failed(SessionError.Code.NOT_FOUND,
                        "No open operation on lane " + session.lane());
            }
            SessionRecordDraft.OperationStarted started =
                    (SessionRecordDraft.OperationStarted) open.getFirst().value();
            if (!(started.intent() instanceof SessionRecordDraft.OperationIntent.Run intent)) {
                return failed(SessionError.Code.INVALID_PAYLOAD,
                        "Open operation is not a run: " + started.id());
            }
            String executionLeafId = intent.initialMessages().isEmpty()
                    ? started.sourceLeafId()
                    : intent.initialMessages().getLast().id();
            Accepted accepted = new Accepted(
                    started.id(), started.sourceLeafId(), executionLeafId
            );
            Options durableOptions = new Options(
                    intent.systemPromptOverride(), effective.options().config(),
                    effective.options().tools()
            );
            RecoveryOptions durableRecovery = new RecoveryOptions(
                    durableOptions, effective.maxAttempts()
            );
            return observeTerminal(session, events, executeClaimed(
                    session, started.id(), () -> resumeClaimed(
                            session, accepted, durableRecovery, events
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
                throw new SessionError(SessionError.Code.NOT_FOUND,
                        "Open operation not found: " + runId);
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

    private static CompletionStage<Outcome> launch(
            AgentSession session,
            Accepted accepted,
            Options options,
            SessionOperationEventBus events
    ) {
        emit(events, new SessionOperationEvent.Started(
                session.lane(), accepted.runId(), SuspendedOperation.Kind.RUN,
                accepted.sourceLeafId()
        ));
        return observeTerminal(session, events, executeClaimed(
                session, accepted.runId(), () -> drive(
                        session, accepted, options, 1, true, events
                )
        ));
    }

    private static CompletionStage<Accepted> acceptNext(
            AgentSession session,
            Options options,
            Integer limit
    ) {
        String runId = session.idGenerator().next();
        return session.transaction(transaction -> {
            List<SessionRunQueue.Pending> pending = SessionRunQueue.pending(
                    transaction, session.lane(),
                    SessionRecordDraft.Queue.NEXT_RUN, null, limit
            );
            if (pending.isEmpty()) {
                throw new SessionError(SessionError.Code.NOT_FOUND,
                        "No pending next-run messages on lane " + session.lane());
            }
            ArrayList<SessionEntryDraft> initial = new ArrayList<>();
            ArrayList<AgentMessage> prompts = new ArrayList<>();
            for (SessionRunQueue.Pending item : pending) {
                SessionEntryDraft target = item.target();
                if (!(target instanceof SessionEntryDraft.Message message)) {
                    throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                            "Next-run target is not a message: " + target.id());
                }
                initial.add(target);
                prompts.add(message.message());
            }
            if (prompts.getLast() instanceof AssistantMessage) {
                throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                        "A durable run cannot continue from an assistant prompt");
            }
            String sourceLeafId = transaction.leafId();
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    runId, session.lane(), sourceLeafId,
                    new SessionRecordDraft.OperationIntent.Run(
                            prompts, initial, options.systemPrompt(), null
                    )
            ));
            ArrayList<SessionEntry> consumed = new ArrayList<>();
            for (SessionEntryDraft draft : initial) {
                consumed.add(transaction.append(draft));
            }
            return new NextAcceptance(
                    new Accepted(runId, sourceLeafId, transaction.leafId()),
                    List.copyOf(consumed)
            );
        }).thenApply(result -> {
            SessionRunQueue.emitConsumed(
                    session, SessionRecordDraft.Queue.NEXT_RUN,
                    null, result.consumed()
            );
            return result.accepted();
        });
    }

    private static CompletionStage<Accepted> accept(
            AgentSession session,
            List<AgentMessage> prompts,
            Options options
    ) {
        String runId = session.idGenerator().next();
        ArrayList<SessionEntryDraft> initial = new ArrayList<>();
        for (AgentMessage prompt : prompts) {
            initial.add(new SessionEntryDraft.Message(
                    session.idGenerator().next(), prompt
            ));
        }
        return session.transaction(transaction -> {
            String sourceLeafId = transaction.leafId();
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    runId, session.lane(), sourceLeafId,
                    new SessionRecordDraft.OperationIntent.Run(
                            prompts, initial, options.systemPrompt(), null
                    )
            ));
            for (SessionEntryDraft draft : initial) transaction.append(draft);
            return new Accepted(runId, sourceLeafId, transaction.leafId());
        });
    }

    private static CompletionStage<Outcome> resumeClaimed(
            AgentSession session,
            Accepted accepted,
            RecoveryOptions recovery,
            SessionOperationEventBus events
    ) {
        if (session.state().hasAbortRequest(session.lane(), accepted.runId())) {
            return finishAborted(session, accepted.runId());
        }
        return session.findRecords(new SessionRecordQuery(
                session.lane(), SessionRecordDraft.Type.STEP_ATTEMPT,
                accepted.runId(), null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )).thenCompose(records -> {
            SessionRecord latestRecord = records.stream()
                    .filter(record -> record.value()
                            instanceof SessionRecordDraft.StepAttempt attempt
                            && attempt.step() == SessionRecordDraft.Step.ASSISTANT)
                    .reduce((ignored, value) -> value).orElse(null);
            if (latestRecord == null) {
                if (!Objects.equals(
                        session.rawState().leaf(session.lane()),
                        accepted.executionLeafId()
                )) {
                    return inconsistentRunLeaf();
                }
                return drive(session, accepted, recovery.options(), 1, true, events);
            }
            SessionRecordDraft.StepAttempt latest =
                    (SessionRecordDraft.StepAttempt) latestRecord.value();
            SessionEntry result = session.rawState().getEntry(latest.resultEntryId());
            if (result == null) {
                String effectLeaf = session.rawState().leafAtSequence(
                        session.lane(), latestRecord.sequence() - 1
                );
                if (!Objects.equals(
                        session.rawState().leaf(session.lane()), effectLeaf
                )) {
                    return inconsistentRunLeaf();
                }
                if (latest.attempt() >= recovery.maxAttempts()) {
                    return finishFailure(session, accepted.runId(),
                            new IllegalStateException(
                                    "Assistant retry attempts exhausted at "
                                            + latest.attempt()
                            ));
                }
                Accepted retry = new Accepted(
                        accepted.runId(), accepted.sourceLeafId(), effectLeaf
                );
                return drive(
                        session, retry, recovery.options(),
                        latest.attempt() + 1, false, events
                );
            }
            if (!(result instanceof SessionEntry.Message message)
                    || !(message.message() instanceof AssistantMessage assistant)) {
                return CompletableFuture.failedFuture(new RecordLogCorruption(
                        RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                        "Assistant attempt result has incompatible content: "
                                + latest.resultEntryId()
                ));
            }
            List<ToolCallContent> calls = assistant.content().stream()
                    .filter(ToolCallContent.class::isInstance)
                    .map(ToolCallContent.class::cast).toList();
            if (calls.isEmpty()) {
                return CompletableFuture.failedFuture(new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Open run already has a terminal assistant result"
                ));
            }
            ToolCompletion tools = toolCompletion(
                    session, accepted.runId(), message, calls
            );
            if (!tools.complete()) {
                return CompletableFuture.completedFuture(new Outcome.ToolsPending(
                        accepted.runId(), session.rawState().leaf(session.lane()), message
                ));
            }
            if (tools.terminate()) {
                return finishTerminated(session, accepted.runId());
            }
            Accepted nextTurn = new Accepted(
                    accepted.runId(), accepted.sourceLeafId(),
                    session.rawState().leaf(session.lane())
            );
            return drive(session, nextTurn, recovery.options(), 1, true, events);
        });
    }

    private static CompletionStage<Outcome> inconsistentRunLeaf() {
        return CompletableFuture.failedFuture(new RecordLogCorruption(
                RecordLogCorruption.Reason.INCONSISTENT_STEP,
                "Run lane leaf changed while operation was suspended"
        ));
    }

    private static ToolCompletion toolCompletion(
            AgentSession session,
            String runId,
            SessionEntry.Message assistant,
            List<ToolCallContent> calls
    ) {
        List<SessionRecord> records = session.rawState().findRecords(
                new SessionRecordQuery(
                        session.lane(), SessionRecordDraft.Type.TOOL_STARTED,
                        runId, null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        );
        Map<Integer, SessionRecordDraft.ToolStarted> starts =
                new java.util.HashMap<>();
        for (SessionRecord record : records) {
            SessionRecordDraft.ToolStarted started =
                    (SessionRecordDraft.ToolStarted) record.value();
            if (started.assistantEntryId().equals(assistant.id())) {
                starts.put(started.toolIndex(), started);
            }
        }
        String expectedParent = assistant.id();
        for (int index = 0; index < calls.size(); index++) {
            SessionRecordDraft.ToolStarted started = starts.get(index);
            if (started == null) return ToolCompletion.INCOMPLETE;
            SessionEntry entry = session.rawState().getEntry(started.resultEntryId());
            if (!(entry instanceof SessionEntry.Message message)
                    || !(message.message() instanceof ToolResultMessage result)
                    || !Objects.equals(message.parentId(), expectedParent)
                    || !result.toolCallId().equals(calls.get(index).id())
                    || !result.toolName().equals(calls.get(index).name())) {
                return ToolCompletion.INCOMPLETE;
            }
            expectedParent = message.id();
        }
        if (!Objects.equals(
                session.rawState().leaf(session.lane()), expectedParent
        )) {
            return ToolCompletion.INCOMPLETE;
        }
        boolean terminate = true;
        for (int index = 0; index < calls.size(); index++) {
            SessionRecordDraft.ToolStarted started = starts.get(index);
            SessionEntry.Message result = (SessionEntry.Message)
                    session.rawState().getEntry(started.resultEntryId());
            terminate &= result.terminate();
        }
        return new ToolCompletion(true, terminate);
    }

    private static CompletionStage<Outcome> drive(
            AgentSession session,
            Accepted accepted,
            Options options,
            int attemptNumber,
            boolean drainSteering,
            SessionOperationEventBus events
    ) {
        session.rawState().reconcileOperationAbort(
                session.lane(), accepted.runId()
        );
        if (session.state().hasAbortRequest(session.lane(), accepted.runId())) {
            return finishAborted(session, accepted.runId());
        }
        CompletionStage<Accepted> ready = drainSteering
                ? session.transaction(transaction -> {
                    List<SessionEntry> consumed = SessionRunQueue.drain(
                            transaction, session.lane(),
                            SessionRecordDraft.Queue.STEER,
                            accepted.runId(), null
                    );
                    return new SteeringDrain(
                            new Accepted(
                                    accepted.runId(), accepted.sourceLeafId(),
                                    transaction.leafId()
                            ),
                            consumed
                    );
                }).thenApply(result -> {
                    SessionRunQueue.emitConsumed(
                            session, SessionRecordDraft.Queue.STEER,
                            accepted.runId(), result.consumed()
                    );
                    return result.accepted();
                })
                : CompletableFuture.completedFuture(accepted);
        return ready.thenCompose(effective -> drivePrepared(
                session, effective, options, attemptNumber, events
        ));
    }

    private static CompletionStage<Outcome> drivePrepared(
            AgentSession session,
            Accepted accepted,
            Options options,
            int attemptNumber,
            SessionOperationEventBus events
    ) {
        String resultEntryId = session.idGenerator().next();
        SessionRecordDraft.StepAttempt attempt = new SessionRecordDraft.StepAttempt(
                session.idGenerator().next(), session.lane(), accepted.runId(),
                SessionRecordDraft.Step.ASSISTANT, attemptNumber,
                resultEntryId, null
        );
        CompletionStage<Outcome> execution = session.appendRecord(attempt)
                .thenCompose(ignored -> {
                    emit(events, new SessionOperationEvent.AttemptStarted(
                            session.lane(), accepted.runId(),
                            SessionRecordDraft.Step.ASSISTANT,
                            attemptNumber, null
                    ));
                    return invoke(session, accepted.runId(), options).thenCompose(messages -> {
                        List<AssistantMessage> assistants = messages.stream()
                                .filter(AssistantMessage.class::isInstance)
                                .map(AssistantMessage.class::cast).toList();
                        if (assistants.size() != 1
                                || options.tools().isEmpty() && messages.size() != 1) {
                            return failed(SessionError.Code.INVALID_PAYLOAD,
                                    "Durable run turn produced " + messages.size()
                                            + " invocation messages and "
                                            + assistants.size() + " assistants");
                        }
                        return commit(
                                session, accepted, resultEntryId,
                                assistants.getFirst(), attemptNumber,
                                options, events
                        );
                    });
                });
        return execution.exceptionallyCompose(failure ->
                finishFailure(session, accepted.runId(), unwrap(failure)));
    }

    private static CompletionStage<List<AgentMessage>> invoke(
            AgentSession session,
            String runId,
            Options options
    ) {
        return session.context().thenCompose(context -> {
            AgentContext agentContext = new AgentContext(
                    options.systemPrompt(), context.messages(), options.tools()
            );
            AgentLoopConfig config = options.tools().isEmpty()
                    ? options.config() : stopBeforeToolEffects(options.config());
            AgentLoopRun run = AgentLoop.continueRun(agentContext, config);
            AutoCloseable cancellation = session.rawState()
                    .registerOperationCancellation(
                            session.lane(), runId, run::cancel
                    );
            return run.result().whenComplete((ignored, failure) ->
                    closeQuietly(cancellation));
        });
    }

    private static CompletionStage<Outcome> commit(
            AgentSession session,
            Accepted accepted,
            String resultEntryId,
            AssistantMessage assistant,
            int attemptNumber,
            Options options,
            SessionOperationEventBus events
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
                return new CommitDecision.Done(new Outcome.Aborted(
                        accepted.runId(), transaction.leafId()
                ));
            }
            if (!Objects.equals(transaction.leafId(), accepted.executionLeafId())) {
                throw new SessionError(SessionError.Code.STORAGE,
                        "Run lane leaf changed while assistant effect was active");
            }
            SessionEntry.Message entry = (SessionEntry.Message) transaction.append(
                    new SessionEntryDraft.Message(resultEntryId, assistant)
            );
            transaction.appendRecord(new SessionRecordDraft.UsageRecord(
                    session.idGenerator().next(), session.lane(), "assistant",
                    assistant.usage(), accepted.runId(), entry.id(), attemptNumber,
                    assistant.stopReason().name().toLowerCase(), null, null
            ));
            boolean hasToolCalls = assistant.content().stream()
                    .anyMatch(ToolCallContent.class::isInstance);
            if (hasToolCalls) {
                return new CommitDecision.Done(new Outcome.ToolsPending(
                        accepted.runId(), entry.id(), entry
                ));
            }
            List<SessionEntry> followUps = SessionRunQueue.drain(
                    transaction, session.lane(),
                    SessionRecordDraft.Queue.FOLLOW_UP,
                    accepted.runId(), null
            );
            if (!followUps.isEmpty()) {
                return new CommitDecision.Continue(
                        new Accepted(
                                accepted.runId(), accepted.sourceLeafId(),
                                transaction.leafId()
                        ),
                        followUps
                );
            }
            transaction.appendRecord(finished(
                    session, accepted.runId(),
                    SessionRecordDraft.OperationOutcome.COMPLETED, null
            ));
            return new CommitDecision.Done(new Outcome.Completed(
                    accepted.runId(), entry.id(), entry
            ));
        }).thenCompose(decision -> switch (decision) {
            case CommitDecision.Done done ->
                    CompletableFuture.completedFuture(done.outcome());
            case CommitDecision.Continue next -> {
                SessionRunQueue.emitConsumed(
                        session, SessionRecordDraft.Queue.FOLLOW_UP,
                        accepted.runId(), next.consumed()
                );
                yield drive(
                        session, next.accepted(), options, 1, true, events
                );
            }
        });
    }

    private static CompletionStage<Outcome> finishTerminated(
            AgentSession session,
            String runId
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
            transaction.appendRecord(finished(
                    session, runId,
                    SessionRecordDraft.OperationOutcome.COMPLETED, null
            ));
            return new Outcome.Terminated(runId, transaction.leafId());
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
                                    : "run_failed",
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

    private static CompletionStage<Outcome> observeTerminal(
            AgentSession session,
            SessionOperationEventBus events,
            CompletionStage<Outcome> stage
    ) {
        return stage.thenApply(outcome -> {
            if (outcome instanceof Outcome.ToolsPending) return outcome;
            SessionOperationEvent.Outcome eventOutcome;
            String resultEntryId = null;
            SessionRecordDraft.OperationError error = null;
            if (outcome instanceof Outcome.Completed completed) {
                eventOutcome = SessionOperationEvent.Outcome.COMPLETED;
                resultEntryId = completed.entry().id();
            } else if (outcome instanceof Outcome.Terminated) {
                eventOutcome = SessionOperationEvent.Outcome.COMPLETED;
            } else if (outcome instanceof Outcome.Aborted) {
                eventOutcome = SessionOperationEvent.Outcome.ABORTED;
            } else {
                eventOutcome = SessionOperationEvent.Outcome.FAILED;
                error = ((Outcome.Failed) outcome).error();
            }
            emit(events, new SessionOperationEvent.Finished(
                    session.lane(), outcome.runId(), SuspendedOperation.Kind.RUN,
                    eventOutcome, outcome.leafId(), resultEntryId, error
            ));
            return outcome;
        });
    }

    private static AgentLoopConfig stopBeforeToolEffects(AgentLoopConfig config) {
        return new AgentLoopConfig(
                config.model(), config.thinkingLevel(), config.sessionId(),
                config.modelStream(), config.contextConverter(),
                config.contextTransformer(), config.apiKeyResolver(),
                config.toolExecution(),
                (call, arguments, context, cancellation) ->
                        CompletableFuture.completedFuture(new BeforeToolCallResult(
                                true, "Durable tool dispatch boundary", true
                        )),
                null, null, null, null, null
        );
    }

    private static void validateAssistantOnly(Options options) {
        AgentLoopConfig config = options.config();
        if (config.steeringMessages() != null || config.followUpMessages() != null) {
            throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                    "Durable run does not yet support steering or follow-up suppliers");
        }
        if (!options.tools().isEmpty()
                && (config.beforeToolCall() != null
                || config.afterToolCall() != null
                || config.prepareNextTurn() != null
                || config.shouldStopAfterTurn() != null)) {
            throw new SessionError(SessionError.Code.INVALID_PAYLOAD,
                    "Tool-enabled durable run does not support Agent tool/turn hooks");
        }
    }

    private static CompletionStage<Outcome> executeClaimed(
            AgentSession session,
            String runId,
            Supplier<CompletionStage<Outcome>> operation
    ) {
        if (!session.state().claimOperationExecution(session.lane(), runId)) {
            return failed(SessionError.Code.STORAGE,
                    "Operation is already executing: " + runId);
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

    private static void emit(
            SessionOperationEventBus events,
            SessionOperationEvent event
    ) {
        if (events != null) events.emit(event);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Cancellation registration cleanup cannot change the durable outcome.
        }
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

    private record ToolCompletion(boolean complete, boolean terminate) {
        private static final ToolCompletion INCOMPLETE =
                new ToolCompletion(false, false);
    }

    private record Accepted(
            String runId,
            String sourceLeafId,
            String executionLeafId
    ) {
    }

    private record NextAcceptance(
            Accepted accepted,
            List<SessionEntry> consumed
    ) {
    }

    private record SteeringDrain(
            Accepted accepted,
            List<SessionEntry> consumed
    ) {
    }

    private sealed interface CommitDecision {
        record Done(Outcome outcome) implements CommitDecision {
        }

        record Continue(
                Accepted accepted,
                List<SessionEntry> consumed
        ) implements CommitDecision {
        }
    }

    public record Options(
            String systemPrompt,
            AgentLoopConfig config,
            List<AgentTool> tools
    ) {
        public Options(String systemPrompt, AgentLoopConfig config) {
            this(systemPrompt, config, List.of());
        }

        public Options {
            systemPrompt = systemPrompt == null ? "" : systemPrompt;
            Objects.requireNonNull(config, "config");
            tools = tools == null ? List.of() : List.copyOf(tools);
        }
    }

    public record RecoveryOptions(
            Options options,
            int maxAttempts
    ) {
        public RecoveryOptions {
            Objects.requireNonNull(options, "options");
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
                SessionEntry.Message entry
        ) implements Outcome {
        }

        record ToolsPending(
                String runId,
                String leafId,
                SessionEntry.Message assistantEntry
        ) implements Outcome {
        }

        record Terminated(String runId, String leafId) implements Outcome {
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
