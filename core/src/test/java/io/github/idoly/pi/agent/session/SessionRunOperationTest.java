package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.AgentLoopConfig;
import io.github.idoly.pi.agent.AgentMessageSupplier;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.agent.harness.SuspendedOperation;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionRunOperationTest {
    @TempDir
    Path temporary;

    @Test
    void atomicallyAcceptsPromptAndPublishesAssistantUsageAndFinish() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "run-success");
        ScriptedModelStream stream = stream("answer");
        SessionOperationEventBus events = new SessionOperationEventBus();
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        events.onEvent(event -> {
            received.add(event);
            if (event instanceof SessionOperationEvent.Started) {
                assertEquals(1, join(session.findOpenOperations("main", 2)).size());
                assertEquals(1, join(session.findEntries()).size());
            } else if (event instanceof SessionOperationEvent.AttemptStarted) {
                assertEquals(3, join(session.log(0, null)).size());
            } else if (event instanceof SessionOperationEvent.Finished) {
                assertTrue(join(session.findOpenOperations("main", 2)).isEmpty());
                assertEquals(6, join(session.log(0, null)).size());
            }
        });

        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.run(
                        session,
                        List.of(UserMessage.text("question", 1)),
                        options("durable-system", stream),
                        events
                ))
        );
        assertEquals(completed.entry().id(), join(session.leafId()));
        assertEquals("durable-system", stream.lastContext().systemPrompt());
        assertEquals(1, stream.lastContext().messages().size());
        assertEquals(List.of(
                SessionLogItem.Record.class,
                SessionLogItem.Entry.class,
                SessionLogItem.Record.class,
                SessionLogItem.Entry.class,
                SessionLogItem.Record.class,
                SessionLogItem.Record.class
        ), join(session.log(0, null)).stream().map(Object::getClass).toList());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L),
                join(session.log(0, null)).stream()
                        .map(SessionLogItem::sequence).toList());
        assertEquals(List.of(
                SessionRecordDraft.Type.OPERATION_STARTED,
                SessionRecordDraft.Type.STEP_ATTEMPT,
                SessionRecordDraft.Type.USAGE,
                SessionRecordDraft.Type.OPERATION_FINISHED
        ), recordsOldest(session).stream().map(SessionRecord::type).toList());
        assertEquals(1, join(session.usage()).size());
        SessionOperationInspector.LastResult result = join(session.lastResult());
        assertEquals(completed.runId(), result.runId());
        assertEquals(completed.entry().id(), result.leafId());
        assertNull(result.resultEntryId());
        assertEquals(List.of(
                SessionOperationEvent.Started.class,
                SessionOperationEvent.AttemptStarted.class,
                SessionOperationEvent.Finished.class
        ), received.stream().map(Object::getClass).toList());
        SessionOperationEvent.Finished finished =
                (SessionOperationEvent.Finished) received.getLast();
        assertEquals(SuspendedOperation.Kind.RUN, finished.kind());
        assertEquals(completed.entry().id(), finished.resultEntryId());
    }

    @Test
    void jsonlRunReopensWithCanonicalRecordProjection() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession session = create(repository, "jsonl-run");
        SessionRunOperation.Outcome.Completed outcome = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.run(
                        session,
                        List.of(UserMessage.text("persist", 1)),
                        options("system", stream("persisted"))
                ))
        );
        AgentSession reopened = join(repository.open(join(session.metadata())));
        assertEquals(List.of(outcome.entry().id()), join(reopened.findEntries()).stream()
                .filter(entry -> entry.sequence() == 4)
                .map(SessionEntry::id).toList());
        assertEquals(6, join(reopened.log(0, null)).size());
        assertEquals(outcome.runId(), join(reopened.lastResult()).runId());
        assertTrue(join(reopened.findOpenOperations("main", null)).isEmpty());
        join(reopened.validateRecordLog("main"));
    }

    @Test
    void resumesUnknownAssistantEffectWithLaterAttemptAndDurableSystemPrompt() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "resume-run");
        appendCrashPrefix(session);
        ScriptedModelStream stream = stream("recovered");

        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(
                                options("ignored-system", stream), 2
                        )
                ))
        );
        assertEquals("durable-system", stream.lastContext().systemPrompt());
        assertEquals(List.of(1, 2), recordsOldest(session).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
        assertEquals(7, join(session.log(0, null)).size());
        assertEquals(completed.entry().id(), join(session.leafId()));
        join(session.validateRecordLog("main"));
    }

    @Test
    void jsonlCrashPrefixReopensAndResumesWithoutReplayingAttemptOne() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("recovery-sessions"), temporary
        );
        AgentSession session = create(repository, "jsonl-resume");
        appendCrashPrefix(session);
        AgentSession reopened = join(repository.open(join(session.metadata())));
        ScriptedModelStream stream = stream("reopened");

        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.resume(
                        reopened,
                        new SessionRunOperation.RecoveryOptions(
                                options("ignored", stream), 2
                        )
                ))
        );
        assertEquals(List.of(1, 2), recordsOldest(reopened).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
        AgentSession verified = join(repository.open(join(reopened.metadata())));
        assertEquals(completed.entry().id(), join(verified.leafId()));
        assertEquals(completed.runId(), join(verified.lastResult()).runId());
        join(verified.validateRecordLog("main"));
    }

    @Test
    void exhaustedAttemptCapFinishesFailedWithoutModelDispatch() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "exhausted-run");
        appendCrashPrefix(session);
        ScriptedModelStream stream = stream("must-not-run");

        SessionRunOperation.Outcome.Failed failed = assertInstanceOf(
                SessionRunOperation.Outcome.Failed.class,
                join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(
                                options("ignored", stream), 1
                        )
                ))
        );
        assertEquals("run_failed", failed.error().code());
        assertTrue(failed.error().message().contains("exhausted at 1"));
        assertEquals(0, stream.invocationCount());
        assertTrue(join(session.findOpenOperations("main", null)).isEmpty());
        assertEquals(SessionRecordDraft.OperationOutcome.FAILED,
                join(session.lastResult()).outcome());
    }

    @Test
    void concurrentResumeIsRejectedByProcessLocalExecutionClaim() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "claimed-run");
        appendCrashPrefix(session);
        ControlledModelStream stream = new ControlledModelStream();
        SessionRunOperation.RecoveryOptions recovery =
                new SessionRunOperation.RecoveryOptions(
                        new SessionRunOperation.Options(
                                "ignored",
                                new AgentLoopConfig(ScriptedMessages.model(), stream)
                        ),
                        3
                );
        CompletionStage<SessionRunOperation.Outcome> first =
                SessionRunOperation.resume(session, recovery);
        assertEquals(1, stream.invocations.get());

        CompletionException duplicate = assertThrows(
                CompletionException.class,
                () -> join(SessionRunOperation.resume(session, recovery))
        );
        SessionError error = assertInstanceOf(SessionError.class, duplicate.getCause());
        assertEquals(SessionError.Code.STORAGE, error.code());
        assertTrue(error.getMessage().contains("already executing"));
        assertEquals(1, stream.invocations.get());

        stream.complete("settled");
        assertInstanceOf(SessionRunOperation.Outcome.Completed.class, join(first));
        assertTrue(join(session.findOpenOperations("main", null)).isEmpty());
    }

    @Test
    void durableAbortActivelyCancelsProviderStream() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "provider-cancel-run");
        AtomicBoolean providerCancelled = new AtomicBoolean();
        ModelStream waiting = (model, context, options) -> subscriber ->
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long count) {
                    }

                    @Override
                    public void cancel() {
                        providerCancelled.set(true);
                    }
                });
        SessionRunOperation.Options options = new SessionRunOperation.Options(
                "system",
                new AgentLoopConfig(ScriptedMessages.model(), waiting)
        );
        CompletionStage<SessionRunOperation.Outcome> running =
                SessionRunOperation.run(
                        session, List.of(UserMessage.text("prompt", 1)), options
                );
        String runId = join(session.findOpenOperations("main", 1)).getFirst().id();

        assertTrue(join(SessionRunOperation.requestAbort(session, runId)));
        SessionRunOperation.Outcome.Aborted aborted = assertInstanceOf(
                SessionRunOperation.Outcome.Aborted.class, join(running)
        );
        assertEquals(runId, aborted.runId());
        assertTrue(providerCancelled.get());
        assertEquals(1, join(session.findEntries()).size());
        assertTrue(join(session.usage()).isEmpty());
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED,
                join(session.lastResult()).outcome());
    }

    @Test
    void abortRequestedDuringEffectWinsBeforeAssistantPublication() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "active-abort-run");
        ControlledModelStream stream = new ControlledModelStream();
        CompletionStage<SessionRunOperation.Outcome> running = SessionRunOperation.run(
                session,
                List.of(UserMessage.text("prompt", 1)),
                new SessionRunOperation.Options(
                        "system",
                        new AgentLoopConfig(ScriptedMessages.model(), stream)
                )
        );
        String runId = join(session.findOpenOperations("main", 1)).getFirst().id();
        assertTrue(join(SessionRunOperation.requestAbort(session, runId)));
        stream.complete("discarded");

        SessionRunOperation.Outcome.Aborted aborted = assertInstanceOf(
                SessionRunOperation.Outcome.Aborted.class, join(running)
        );
        assertEquals(runId, aborted.runId());
        assertEquals(1, join(session.findEntries()).size());
        assertTrue(join(session.usage()).isEmpty());
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED,
                join(session.lastResult()).outcome());
    }

    @Test
    void closeDuringEffectReleasesClaimAndLeavesRunRecoverable() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "closed-effect-run");
        SessionMetadata metadata = join(session.metadata());
        ControlledModelStream firstStream = new ControlledModelStream();
        CompletionStage<SessionRunOperation.Outcome> running = SessionRunOperation.run(
                session,
                List.of(UserMessage.text("prompt", 1)),
                new SessionRunOperation.Options(
                        "durable-system",
                        new AgentLoopConfig(ScriptedMessages.model(), firstStream)
                )
        );
        join(session.close());
        firstStream.complete("lost");
        CompletionException closed = assertThrows(
                CompletionException.class, () -> join(running)
        );
        SessionError closedError = assertInstanceOf(
                SessionError.class, closed.getCause()
        );
        assertEquals(SessionError.Code.CLOSED, closedError.code());

        AgentSession reopened = join(repository.open(metadata));
        SessionOperationInspector.OpenOperation suspended = join(
                SessionOperationInspector.inspectLane(reopened)
        );
        assertEquals(SessionOperationInspector.Status.SUSPENDED, suspended.status());
        ScriptedModelStream recoveryStream = stream("recovered");
        SessionRunOperation.Outcome.Completed recovered = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.resume(
                        reopened,
                        new SessionRunOperation.RecoveryOptions(
                                options("ignored", recoveryStream), 2
                        )
                ))
        );
        assertEquals(recovered.entry().id(), join(reopened.leafId()));
        assertEquals(List.of(1, 2), recordsOldest(reopened).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
    }

    @Test
    void durableAbortShortCircuitsRecoveryBeforeModelDispatch() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "abort-run");
        appendCrashPrefix(session);
        String runId = join(session.findOpenOperations("main", 1)).getFirst().id();
        assertTrue(join(SessionRunOperation.requestAbort(session, runId)));
        assertTrue(!join(SessionRunOperation.requestAbort(session, runId)));
        ScriptedModelStream stream = stream("must-not-run");

        SessionRunOperation.Outcome.Aborted aborted = assertInstanceOf(
                SessionRunOperation.Outcome.Aborted.class,
                join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(
                                options("ignored", stream), 2
                        )
                ))
        );
        assertEquals(runId, aborted.runId());
        assertEquals(0, stream.invocationCount());
        assertTrue(join(session.findOpenOperations("main", null)).isEmpty());
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED,
                join(session.lastResult()).outcome());
    }

    @Test
    void movedSuspendedLaneIsRejectedWithoutAnotherAttempt() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "moved-run");
        appendCrashPrefix(session);
        join(session.moveLane(null));
        ScriptedModelStream stream = stream("must-not-run");

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(
                                options("ignored", stream), 2
                        )
                ))
        );
        RecordLogCorruption corruption = assertInstanceOf(
                RecordLogCorruption.class, failure.getCause()
        );
        assertEquals(RecordLogCorruption.Reason.INCONSISTENT_STEP,
                corruption.reason());
        assertEquals(0, stream.invocationCount());
        assertEquals(1, recordsOldest(session).stream()
                .filter(record -> record.type()
                        == SessionRecordDraft.Type.STEP_ATTEMPT)
                .count());
    }

    @Test
    void durableSteeringEnqueuedAtStartIsDrainedBeforeFirstAttempt() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "steered-run");
        ScriptedModelStream stream = stream("answer");
        SessionOperationEventBus events = new SessionOperationEventBus();
        ArrayList<SessionRunQueueEvent> queueEvents = new ArrayList<>();
        session.queueEvents().onEvent(queueEvents::add);
        events.onEvent(event -> {
            if (event instanceof SessionOperationEvent.Started started) {
                join(SessionRunQueue.enqueueMessage(
                        session, SessionRecordDraft.Queue.STEER,
                        started.runId(), UserMessage.text("steer", 2)
                ));
            }
        });

        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.run(
                        session, List.of(UserMessage.text("prompt", 1)),
                        options("system", stream), events
                ))
        );
        assertEquals(2, stream.lastContext().messages().size());
        assertEquals(List.of(
                SessionRecordDraft.Type.OPERATION_STARTED,
                SessionRecordDraft.Type.QUEUE_ENQUEUED,
                SessionRecordDraft.Type.STEP_ATTEMPT,
                SessionRecordDraft.Type.USAGE,
                SessionRecordDraft.Type.OPERATION_FINISHED
        ), recordsOldest(session).stream().map(SessionRecord::type).toList());
        List<SessionEntry> entries = join(session.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        )));
        assertEquals(3, entries.size());
        assertEquals(UserMessage.text("prompt", 1),
                ((SessionEntry.Message) entries.get(0)).message());
        assertEquals(UserMessage.text("steer", 2),
                ((SessionEntry.Message) entries.get(1)).message());
        assertEquals(completed.entry().id(), entries.getLast().id());
        assertEquals(List.of(
                SessionRunQueueEvent.Enqueued.class,
                SessionRunQueueEvent.Consumed.class
        ), queueEvents.stream().map(Object::getClass).toList());
        join(session.validateRecordLog("main"));
    }

    @Test
    void durableFollowUpEnqueuedDuringFirstAttemptTriggersSecondTurn() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "follow-up-run");
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("first", StopReason.STOP)
                )),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("second", StopReason.STOP)
                ))
        ));
        SessionOperationEventBus events = new SessionOperationEventBus();
        AtomicInteger attempts = new AtomicInteger();
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        ArrayList<SessionRunQueueEvent> queueEvents = new ArrayList<>();
        session.queueEvents().onEvent(queueEvents::add);
        events.onEvent(event -> {
            received.add(event);
            if (event instanceof SessionOperationEvent.AttemptStarted started
                    && attempts.getAndIncrement() == 0) {
                join(SessionRunQueue.enqueueMessage(
                        session, SessionRecordDraft.Queue.FOLLOW_UP,
                        started.runId(), UserMessage.text("follow-up", 3)
                ));
            }
        });

        join(SessionRunOperation.run(
                session, List.of(UserMessage.text("prompt", 1)),
                options("system", stream), events
        ));
        assertEquals(2, stream.invocationCount());
        assertEquals(List.of(1, 3), stream.contexts().stream()
                .map(context -> context.messages().size()).toList());
        assertEquals(List.of(1, 1), recordsOldest(session).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
        assertEquals(2, received.stream()
                .filter(SessionOperationEvent.AttemptStarted.class::isInstance).count());
        assertEquals(1, received.stream()
                .filter(SessionOperationEvent.Finished.class::isInstance).count());
        assertTrue(join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.FOLLOW_UP,
                recordsOldest(session).getFirst().id(), null
        )).isEmpty());
        assertEquals(List.of(
                SessionRunQueueEvent.Enqueued.class,
                SessionRunQueueEvent.Consumed.class
        ), queueEvents.stream().map(Object::getClass).toList());
        join(session.validateRecordLog("main"));
    }

    @Test
    void toolEnabledRunStopsAtDurableBoundaryAndResumesNextTurn() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "tool-cycle-run");
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolAssistant())),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("final", StopReason.STOP)
                ))
        ));
        AtomicInteger effects = new AtomicInteger();
        AgentTool tool = countingTool(effects);
        SessionRunOperation.Options options = new SessionRunOperation.Options(
                "system",
                new AgentLoopConfig(ScriptedMessages.model(), stream),
                List.of(tool)
        );

        SessionRunOperation.Outcome.ToolsPending pending = assertInstanceOf(
                SessionRunOperation.Outcome.ToolsPending.class,
                join(SessionRunOperation.run(
                        session, List.of(UserMessage.text("prompt", 1)), options
                ))
        );
        assertEquals(0, effects.get(),
                "Agent crossed the durable tool boundary and executed the real tool");
        assertEquals(1, stream.invocationCount());
        assertEquals(pending.assistantEntry().id(), join(session.leafId()));
        assertTrue(join(session.findOpenOperations("main", null)).size() == 1);
        assertEquals(1, join(session.usage()).size());

        SessionRunOperation.Outcome.ToolsPending stillPending = assertInstanceOf(
                SessionRunOperation.Outcome.ToolsPending.class,
                join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(options, 2)
                ))
        );
        assertEquals(pending.assistantEntry().id(),
                stillPending.assistantEntry().id());
        assertEquals(1, stream.invocationCount());

        SessionToolExecution.Outcome.Completed tools = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.execute(
                        session, pending.runId(), pending.assistantEntry().id(),
                        List.of(tool),
                        new SessionToolExecution.Options(
                                Map.of("lookup", SessionRecordDraft.Replay.SAFE),
                                java.time.Clock.systemUTC()
                        )
                ))
        );
        assertEquals(1, effects.get());
        assertEquals(1, tools.results().size());

        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(options, 2)
                ))
        );
        assertEquals(2, stream.invocationCount());
        assertEquals(3, stream.contexts().get(1).messages().size());
        assertEquals(completed.entry().id(), join(session.leafId()));
        assertEquals(List.of(1, 1), recordsOldest(session).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
        assertTrue(join(session.findOpenOperations("main", null)).isEmpty());
        assertEquals(3, join(session.usage()).size());
        join(session.validateRecordLog("main"));
    }

    @Test
    void jsonlToolCycleRecoversAcrossBothPendingBoundaries() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("tool-cycle-sessions"), temporary
        );
        AgentSession session = create(repository, "jsonl-tool-cycle");
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolAssistant())),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("final", StopReason.STOP)
                ))
        ));
        AtomicInteger effects = new AtomicInteger();
        AgentTool tool = countingTool(effects);
        SessionRunOperation.Options options = new SessionRunOperation.Options(
                "durable-system",
                new AgentLoopConfig(ScriptedMessages.model(), stream),
                List.of(tool)
        );
        SessionRunOperation.Outcome.ToolsPending pending = assertInstanceOf(
                SessionRunOperation.Outcome.ToolsPending.class,
                join(SessionRunOperation.run(
                        session, List.of(UserMessage.text("prompt", 1)), options
                ))
        );
        SessionMetadata metadata = join(session.metadata());

        AgentSession beforeTool = join(repository.open(metadata));
        assertInstanceOf(
                SessionRunOperation.Outcome.ToolsPending.class,
                join(SessionRunOperation.resume(
                        beforeTool,
                        new SessionRunOperation.RecoveryOptions(options, 2)
                ))
        );
        assertEquals(1, stream.invocationCount());
        join(SessionToolExecution.execute(
                beforeTool, pending.runId(), pending.assistantEntry().id(),
                List.of(tool),
                new SessionToolExecution.Options(
                        Map.of("lookup", SessionRecordDraft.Replay.SAFE),
                        java.time.Clock.systemUTC()
                )
        ));
        assertEquals(1, effects.get());

        AgentSession afterTool = join(repository.open(metadata));
        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.resume(
                        afterTool,
                        new SessionRunOperation.RecoveryOptions(options, 2)
                ))
        );
        AgentSession verified = join(repository.open(metadata));
        assertEquals(completed.entry().id(), join(verified.leafId()));
        assertEquals(2, stream.invocationCount());
        assertEquals(3, join(verified.usage()).size());
        assertTrue(join(verified.findOpenOperations("main", null)).isEmpty());
        join(verified.validateRecordLog("main"));
    }

    @Test
    void nextRunClaimsProvisionedMessageIdsInAcceptanceTransaction() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "next-run");
        SessionRunQueue.Pending first = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.NEXT_RUN, null,
                UserMessage.text("first", 1)
        ));
        SessionRunQueue.Pending second = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.NEXT_RUN, null,
                UserMessage.text("second", 2)
        ));
        ArrayList<SessionRunQueueEvent> queueEvents = new ArrayList<>();
        session.queueEvents().onEvent(queueEvents::add);

        SessionRunOperation.Outcome.Completed firstRun = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.runNext(
                        session, options("system", stream("first-answer")), 1
                ))
        );
        List<SessionEntry> firstEntries = join(session.findEntries(
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
        ));
        assertEquals(first.target().id(), firstEntries.getFirst().id());
        assertEquals(firstRun.entry().id(), firstEntries.get(1).id());
        assertEquals(List.of(second.target().id()), join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).stream().map(value -> value.target().id()).toList());

        SessionRunOperation.Outcome.Completed secondRun = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.runNext(
                        session, options("system", stream("second-answer")), null
                ))
        );
        List<SessionEntry> all = join(session.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        )));
        assertEquals(List.of(
                first.target().id(), firstRun.entry().id(),
                second.target().id(), secondRun.entry().id()
        ), all.stream().map(SessionEntry::id).toList());
        assertTrue(join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).isEmpty());
        assertEquals(List.of(
                List.of(first.target().id()), List.of(second.target().id())
        ), queueEvents.stream()
                .map(SessionRunQueueEvent.Consumed.class::cast)
                .map(SessionRunQueueEvent.Consumed::entryIds).toList());
        join(session.validateRecordLog("main"));
    }

    @Test
    void emptyOrNonMessageNextRunFailsWithoutConsumingQueue() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "invalid-next-run");
        long emptySequence = join(session.log(0, null)).size();
        assertCode(SessionError.Code.NOT_FOUND, SessionRunOperation.runNext(
                session, options("system", stream("unused")), null
        ));
        assertEquals(emptySequence, join(session.log(0, null)).size());

        SessionRunQueue.Pending custom = join(SessionRunQueue.enqueue(
                session, SessionRecordDraft.Queue.NEXT_RUN, null,
                new SessionEntryDraft.Custom("custom-next", "note", null)
        ));
        long queuedSequence = join(session.log(0, null)).size();
        assertCode(SessionError.Code.INVALID_PAYLOAD, SessionRunOperation.runNext(
                session, options("system", stream("unused")), null
        ));
        assertEquals(queuedSequence, join(session.log(0, null)).size());
        assertEquals(List.of(custom.target().id()), join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).stream().map(value -> value.target().id()).toList());
    }

    @Test
    void rejectsUndurableQueueSuppliersBeforeAcceptance() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "unsupported-run");
        AgentMessageSupplier supplier = () -> CompletableFuture.completedFuture(List.of());
        AgentLoopConfig config = new AgentLoopConfig(
                ScriptedMessages.model(), "off", null, stream("unused"),
                null, null, null, null, null, null, null, null,
                supplier, null
        );
        SessionError failure = assertThrows(SessionError.class, () ->
                SessionRunOperation.run(
                        session, List.of(UserMessage.text("prompt", 1)),
                        new SessionRunOperation.Options("", config)
                ));
        assertEquals(SessionError.Code.INVALID_PAYLOAD, failure.code());
        assertTrue(join(session.log(0, null)).isEmpty());

        CompletionException assistantPrompt = assertThrows(
                CompletionException.class,
                () -> join(SessionRunOperation.run(
                        session,
                        List.of(ScriptedMessages.assistant("invalid", StopReason.STOP)),
                        options("", stream("unused"))
                ))
        );
        assertEquals(SessionError.Code.INVALID_PAYLOAD,
                assertInstanceOf(SessionError.class, assistantPrompt.getCause()).code());
        assertTrue(join(session.log(0, null)).isEmpty());
    }

    private static AssistantMessage toolAssistant() {
        return new AssistantMessage(
                List.of(new ToolCallContent(
                        "call", "lookup", Map.of("query", "value")
                )),
                "test-api", "test-provider", "test-model",
                Usage.ZERO, StopReason.TOOL_USE, null, 2
        );
    }

    private static AgentTool countingTool(AtomicInteger effects) {
        return new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("lookup", "lookup", Map.of());
            }

            @Override
            public CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                effects.incrementAndGet();
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("tool result")), Map.of(),
                        Usage.ZERO, false
                ));
            }
        };
    }

    private static void appendCrashPrefix(AgentSession session) {
        AgentMessage prompt = UserMessage.text("recover me", 1);
        SessionEntryDraft.Message initial = new SessionEntryDraft.Message(
                "prompt-entry", prompt
        );
        join(session.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", transaction.leafId(),
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(prompt), List.of(initial),
                            "durable-system", null
                    )
            ));
            transaction.append(initial);
            return null;
        }));
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run",
                SessionRecordDraft.Step.ASSISTANT, 1,
                "unknown-assistant", null
        )));
    }

    private static List<SessionRecord> recordsOldest(AgentSession session) {
        return join(session.findRecords(new SessionRecordQuery(
                null, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )));
    }

    private static SessionRunOperation.Options options(
            String systemPrompt,
            ScriptedModelStream stream
    ) {
        return new SessionRunOperation.Options(
                systemPrompt,
                new AgentLoopConfig(ScriptedMessages.model(), stream)
        );
    }

    private static ScriptedModelStream stream(String answer) {
        return new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant(answer, StopReason.STOP)
                )
        ));
    }

    private static AgentSession create(SessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionError assertCode(
            SessionError.Code code,
            CompletionStage<?> stage
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class, () -> stage.toCompletableFuture().join()
        );
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(code, error.code());
        return error;
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private static final class ControlledModelStream implements ModelStream {
        private final AtomicInteger invocations = new AtomicInteger();
        private Flow.Subscriber<? super AssistantStreamEvent> subscriber;

        @Override
        public Flow.Publisher<AssistantStreamEvent> stream(
                io.github.idoly.pi.ai.Model model,
                io.github.idoly.pi.ai.ModelContext context,
                io.github.idoly.pi.ai.StreamOptions options
        ) {
            invocations.incrementAndGet();
            return subscriber -> {
                this.subscriber = subscriber;
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long count) {
                    }

                    @Override
                    public void cancel() {
                    }
                });
            };
        }

        private void complete(String text) {
            subscriber.onNext(new AssistantStreamEvent.Done(
                    ScriptedMessages.assistant(text, StopReason.STOP)
            ));
            subscriber.onComplete();
        }
    }
}
