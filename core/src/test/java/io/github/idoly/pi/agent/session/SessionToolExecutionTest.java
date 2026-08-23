package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.agent.AfterToolCallResult;
import io.github.idoly.pi.agent.BeforeToolCallResult;
import io.github.idoly.pi.agent.ToolExecutionMode;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionToolExecutionTest {
    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-02-01T00:00:00Z"), ZoneOffset.UTC
    );

    @TempDir
    Path temporary;

    @Test
    void persistsStartsBeforeEffectsAndResultsInSourceOrder() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "tools");
        appendOpenRun(session, "assistant", List.of(
                call("call-1", "first", Map.of("value", 1)),
                call("call-2", "second", Map.of("value", 2))
        ));
        AtomicInteger firstEffects = new AtomicInteger();
        AtomicInteger secondEffects = new AtomicInteger();
        AgentTool first = tool("first", firstEffects, (toolSession, arguments) -> {
            assertEquals(1, toolStarts(toolSession).size());
            assertEquals(0, resultEntries(toolSession).size());
            assertEquals(2, arguments.get("prepared"));
            return result("first-result");
        }, session);
        AgentTool second = tool("second", secondEffects, (toolSession, arguments) -> {
            assertEquals(2, toolStarts(toolSession).size());
            assertEquals(1, resultEntries(toolSession).size());
            return result("second-result");
        }, session);
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of(
                        "first", SessionRecordDraft.Replay.SAFE,
                        "second", SessionRecordDraft.Replay.NEVER
                ),
                CLOCK
        );

        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.execute(
                        session, "run", "assistant",
                        List.of(first, second), options
                ))
        );
        assertEquals(1, firstEffects.get());
        assertEquals(1, secondEffects.get());
        assertEquals(List.of("call-1", "call-2"), completed.results().stream()
                .map(SessionToolExecutionTest::toolResult)
                .map(ToolResultMessage::toolCallId).toList());
        assertEquals(List.of("assistant", completed.results().getFirst().id()),
                completed.results().stream().map(SessionEntry::parentId).toList());
        assertEquals(CLOCK.millis(), completed.results().stream()
                .map(SessionToolExecutionTest::toolResult)
                .map(ToolResultMessage::timestamp).distinct().findFirst().orElseThrow());
        assertEquals(2, join(session.usage()).size());
        assertEquals(List.of(
                SessionRecordDraft.Replay.SAFE,
                SessionRecordDraft.Replay.NEVER
        ), toolStarts(session).stream().map(SessionRecordDraft.ToolStarted::replay).toList());
        join(session.validateRecordLog("main"));

        SessionToolExecution.Outcome.Completed replayed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.resume(
                        session, "run", "assistant",
                        List.of(first, second), options
                ))
        );
        assertEquals(2, replayed.results().size());
        assertEquals(1, firstEffects.get());
        assertEquals(1, secondEffects.get());
    }

    @Test
    void toolWatchRegistersBeforeSnapshotAndPreservesReentrantOrder() {
        SessionToolExecutionEventBus events = new SessionToolExecutionEventBus();
        SessionToolExecutionEvent.EffectStarted during =
                new SessionToolExecutionEvent.EffectStarted(
                        "main", "run", "assistant", 0,
                        "call", "tool", SessionRecordDraft.Replay.SAFE, false
                );
        SessionToolExecutionEvent.EffectFinished queued =
                new SessionToolExecutionEvent.EffectFinished(
                        "main", "run", "assistant", 0,
                        "call", "tool", false
                );
        SessionToolExecutionEvent.ResultPublished reentrant =
                new SessionToolExecutionEvent.ResultPublished(
                        "main", "run", "assistant", 0,
                        "call", "tool", "result"
                );
        SessionToolExecutionEventBus.WatchHandle<String> watch = events.watch(() -> {
            events.emit(during);
            return "durable-snapshot";
        });
        events.emit(queued);
        java.util.ArrayList<SessionToolExecutionEvent> received =
                new java.util.ArrayList<>();
        watch.start(event -> {
            received.add(event);
            if (event.equals(during)) events.emit(reentrant);
        });
        assertEquals("durable-snapshot", watch.snapshot());
        assertEquals(List.of(during, queued, reentrant), received);
        watch.close();
        events.emit(during);
        assertEquals(3, received.size());
    }

    @Test
    void parallelEffectsFinishInCompletionOrderAndPublishInSourceOrder() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "parallel-tools");
        appendOpenRun(session, "assistant", List.of(
                call("first-call", "first", Map.of()),
                call("second-call", "second", Map.of())
        ));
        AtomicInteger firstEffects = new AtomicInteger();
        AtomicInteger secondEffects = new AtomicInteger();
        CompletableFuture<AgentToolResult> firstResult = new CompletableFuture<>();
        CompletableFuture<AgentToolResult> secondResult = new CompletableFuture<>();
        AgentTool first = asynchronousTool("first", firstEffects, firstResult);
        AgentTool second = asynchronousTool("second", secondEffects, secondResult);
        SessionToolExecutionEventBus events = new SessionToolExecutionEventBus();
        java.util.ArrayList<SessionToolExecutionEvent> received = new java.util.ArrayList<>();
        events.onEvent(received::add);
        events.onEvent(event -> {
            throw new IllegalStateException("passive listener failure");
        });
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of(
                        "first", SessionRecordDraft.Replay.SAFE,
                        "second", SessionRecordDraft.Replay.SAFE
                ),
                CLOCK, ToolExecutionMode.PARALLEL, events
        );

        CompletionStage<SessionToolExecution.Outcome> running =
                SessionToolExecution.execute(
                        session, "run", "assistant", List.of(first, second), options
                );
        assertEquals(1, firstEffects.get());
        assertEquals(1, secondEffects.get());
        assertEquals(2, toolStarts(session).size());
        assertTrue(resultEntries(session).isEmpty());
        assertEquals(List.of(0, 1), received.stream()
                .filter(SessionToolExecutionEvent.EffectStarted.class::isInstance)
                .map(SessionToolExecutionEvent::toolIndex).toList());

        secondResult.complete(result("second-completed-first"));
        assertTrue(resultEntries(session).isEmpty());
        assertEquals(List.of(1), received.stream()
                .filter(SessionToolExecutionEvent.EffectFinished.class::isInstance)
                .map(SessionToolExecutionEvent::toolIndex).toList());
        firstResult.complete(result("first-completed-second"));

        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class, join(running)
        );
        assertEquals(List.of("first-call", "second-call"), completed.results().stream()
                .map(SessionToolExecutionTest::toolResult)
                .map(ToolResultMessage::toolCallId).toList());
        assertEquals(List.of(1, 0), received.stream()
                .filter(SessionToolExecutionEvent.EffectFinished.class::isInstance)
                .map(SessionToolExecutionEvent::toolIndex).toList());
        assertEquals(List.of(0, 1), received.stream()
                .filter(SessionToolExecutionEvent.ResultPublished.class::isInstance)
                .map(SessionToolExecutionEvent::toolIndex).toList());
        assertEquals(List.of("assistant", completed.results().getFirst().id()),
                completed.results().stream().map(SessionEntry::parentId).toList());
        join(session.validateRecordLog("main"));
    }

    @Test
    void safeUnknownEffectReplaysButLaterNeverEffectSuspends() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "mixed-recovery");
        appendOpenRun(session, "assistant", List.of(
                call("safe-call", "safe", Map.of()),
                call("never-call", "never", Map.of())
        ));
        appendToolStart(session, "start-safe", 0, "safe-call", "safe", "safe-result",
                SessionRecordDraft.Replay.SAFE);
        appendToolStart(session, "start-never", 1, "never-call", "never", "never-result",
                SessionRecordDraft.Replay.NEVER);
        AtomicInteger safeEffects = new AtomicInteger();
        AtomicInteger neverEffects = new AtomicInteger();
        AgentTool safe = tool("safe", safeEffects,
                (ignored, args) -> result("safe-replayed"), session);
        AgentTool never = tool("never", neverEffects,
                (ignored, args) -> result("must-not-run"), session);

        SessionToolExecution.Outcome.Suspended suspended = assertInstanceOf(
                SessionToolExecution.Outcome.Suspended.class,
                join(SessionToolExecution.resume(
                        session, "run", "assistant",
                        List.of(safe, never), SessionToolExecution.Options.DEFAULT
                ))
        );
        assertEquals(1, safeEffects.get());
        assertEquals(0, neverEffects.get());
        assertEquals(1, suspended.results().size());
        assertEquals(1, suspended.unresolved().toolIndex());
        assertEquals("never-result", suspended.unresolved().resultEntryId());
        assertEquals(SessionRecordDraft.Replay.NEVER,
                suspended.unresolved().replay());
        assertEquals("safe-result", join(session.leafId()));
        join(session.validateRecordLog("main"));
    }

    @Test
    void unresolvedNeverEffectCanBeAdministrativelyPublishedWithoutReplay() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("resolution-sessions"), temporary
        );
        AgentSession session = create(repository, "never-resolution");
        appendOpenRun(session, "assistant", List.of(
                call("call", "external", Map.of())
        ));
        appendToolStart(
                session, "start", 0, "call", "external", "resolved-result",
                SessionRecordDraft.Replay.NEVER
        );
        SessionMetadata metadata = join(session.metadata());
        AgentToolResult observed = new AgentToolResult(
                List.of(new TextContent("observed externally")),
                Map.of("resolution", "operator"), Usage.ZERO, true
        );
        SessionToolExecutionEventBus events = new SessionToolExecutionEventBus();
        java.util.ArrayList<SessionToolExecutionEvent> received = new java.util.ArrayList<>();
        events.onEvent(received::add);
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of(), CLOCK, ToolExecutionMode.SEQUENTIAL,
                events, null, null
        );

        AgentSession reopened = join(repository.open(metadata));
        SessionEntry.Message resolved = join(SessionToolExecution.resolveNever(
                reopened, "run", "assistant", 0, observed, false, options
        ));
        assertEquals("resolved-result", resolved.id());
        assertTrue(resolved.terminate());
        assertEquals("observed externally",
                ((TextContent) toolResult(resolved).content().getFirst()).text());
        assertEquals(List.of(SessionToolExecutionEvent.ResultPublished.class),
                received.stream().map(Object::getClass).toList());
        assertEquals("tool_resolution", join(reopened.findRecords(
                new SessionRecordQuery(
                        "main", SessionRecordDraft.Type.USAGE,
                        "run", null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        )).stream().map(SessionRecord::value)
                .map(SessionRecordDraft.UsageRecord.class::cast)
                .findFirst().orElseThrow().cause());

        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.resume(
                        reopened, "run", "assistant", List.of(), options
                ))
        );
        assertEquals(List.of("resolved-result"), completed.results().stream()
                .map(SessionEntry::id).toList());
        assertCode(SessionError.Code.ALREADY_EXISTS,
                SessionToolExecution.resolveNever(
                        reopened, "run", "assistant", 0,
                        observed, false, options
                ));
        AgentSession verified = join(repository.open(metadata));
        assertEquals("resolved-result", join(verified.leafId()));
        join(verified.validateRecordLog("main"));
    }

    @Test
    void administrativeResolutionRejectsSafeAndOutOfOrderTools() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession safeSession = create(repository, "safe-resolution");
        appendOpenRun(safeSession, "assistant", List.of(
                call("call", "safe", Map.of())
        ));
        appendToolStart(
                safeSession, "safe-start", 0, "call", "safe", "safe-result",
                SessionRecordDraft.Replay.SAFE
        );
        assertCode(SessionError.Code.INVALID_PAYLOAD,
                SessionToolExecution.resolveNever(
                        safeSession, "run", "assistant", 0,
                        result("value"), false, null
                ));

        AgentSession ordered = create(repository, "ordered-resolution");
        appendOpenRun(ordered, "assistant", List.of(
                call("first-call", "first", Map.of()),
                call("second-call", "second", Map.of())
        ));
        appendToolStart(
                ordered, "first-start", 0, "first-call", "first", "first-result",
                SessionRecordDraft.Replay.NEVER
        );
        appendToolStart(
                ordered, "second-start", 1, "second-call", "second", "second-result",
                SessionRecordDraft.Replay.NEVER
        );
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(SessionToolExecution.resolveNever(
                        ordered, "run", "assistant", 1,
                        result("second"), false, null
                ))
        );
        assertInstanceOf(RecordLogCorruption.class, failure.getCause());
        assertEquals("assistant", join(ordered.leafId()));
    }

    @Test
    void deterministicUnknownToolFailureIsSafeAndDurable() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "unknown-tool");
        appendOpenRun(session, "assistant", List.of(
                call("missing-call", "missing", Map.of("x", 1))
        ));

        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.execute(
                        session, "run", "assistant", List.of(),
                        new SessionToolExecution.Options(Map.of(), CLOCK)
                ))
        );
        ToolResultMessage result = toolResult(completed.results().getFirst());
        assertTrue(result.error());
        assertEquals("Tool missing not found",
                ((TextContent) result.content().getFirst()).text());
        assertEquals(SessionRecordDraft.Replay.SAFE,
                toolStarts(session).getFirst().replay());
        assertTrue(join(session.usage()).isEmpty());
        join(session.validateRecordLog("main"));
    }

    @Test
    void beforeHookBlockIsDurableAndCanTerminateRunWithoutToolEffect() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "before-block");
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        AtomicInteger beforeCalls = new AtomicInteger();
        AtomicInteger effects = new AtomicInteger();
        AgentTool tool = tool("tool", effects,
                (ignored, args) -> result("must-not-run"), session);
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of("tool", SessionRecordDraft.Replay.NEVER),
                CLOCK, ToolExecutionMode.SEQUENTIAL, null,
                (call, arguments, context, cancellation) -> {
                    beforeCalls.incrementAndGet();
                    assertEquals(1, toolStarts(session).size());
                    return CompletableFuture.completedFuture(
                            new BeforeToolCallResult(true, "blocked", true)
                    );
                },
                null
        );

        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.execute(
                        session, "run", "assistant", List.of(tool), options
                ))
        );
        assertEquals(1, beforeCalls.get());
        assertEquals(0, effects.get());
        SessionEntry.Message entry = completed.results().getFirst();
        assertTrue(entry.terminate());
        ToolResultMessage result = toolResult(entry);
        assertTrue(result.error());
        assertEquals("blocked", ((TextContent) result.content().getFirst()).text());

        io.github.idoly.pi.testkit.ScriptedModelStream unused =
                new io.github.idoly.pi.testkit.ScriptedModelStream(List.of(
                        new io.github.idoly.pi.ai.AssistantStreamEvent.Done(
                                io.github.idoly.pi.testkit.ScriptedMessages.assistant(
                                        "unused", StopReason.STOP
                                )
                        )
                ));
        SessionRunOperation.Outcome.Terminated terminated = assertInstanceOf(
                SessionRunOperation.Outcome.Terminated.class,
                join(SessionRunOperation.resume(
                        session,
                        new SessionRunOperation.RecoveryOptions(
                                new SessionRunOperation.Options(
                                        "system",
                                        new io.github.idoly.pi.agent.AgentLoopConfig(
                                                io.github.idoly.pi.testkit.ScriptedMessages.model(),
                                                unused
                                        ),
                                        List.of(tool)
                                ),
                                2
                        )
                ))
        );
        assertEquals(entry.id(), terminated.leafId());
        assertEquals(0, unused.invocationCount());
        assertEquals(SessionRecordDraft.OperationOutcome.COMPLETED,
                join(session.lastResult()).outcome());
    }

    @Test
    void afterHookPatchIsPersistedIncludingTerminateAndUsage() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "after-patch");
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        AtomicInteger effects = new AtomicInteger();
        AtomicInteger afterCalls = new AtomicInteger();
        AgentTool tool = tool("tool", effects,
                (ignored, args) -> result("original"), session);
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of("tool", SessionRecordDraft.Replay.SAFE),
                CLOCK, ToolExecutionMode.SEQUENTIAL, null, null,
                (call, arguments, original, error, context, cancellation) -> {
                    afterCalls.incrementAndGet();
                    assertEquals("original",
                            ((TextContent) original.content().getFirst()).text());
                    return CompletableFuture.completedFuture(new AfterToolCallResult(
                            List.of(new TextContent("patched")),
                            Map.of("patched", true), Usage.ZERO,
                            true, true
                    ));
                }
        );

        SessionEntry.Message entry = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.execute(
                        session, "run", "assistant", List.of(tool), options
                ))
        ).results().getFirst();
        assertEquals(1, effects.get());
        assertEquals(1, afterCalls.get());
        assertTrue(entry.terminate());
        ToolResultMessage result = toolResult(entry);
        assertTrue(result.error());
        assertEquals("patched", ((TextContent) result.content().getFirst()).text());
        assertEquals(Map.of("patched", true), result.details());
        assertEquals(Usage.ZERO, result.usage());
        assertEquals(1, join(session.usage()).size());
        join(session.validateRecordLog("main"));
    }

    @Test
    void abortBeforeNextToolProducesNoAdditionalStartOrEffect() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "tool-abort");
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        join(SessionRunOperation.requestAbort(session, "run"));
        AtomicInteger effects = new AtomicInteger();
        AgentTool tool = tool("tool", effects,
                (ignored, args) -> result("unused"), session);

        SessionToolExecution.Outcome.Aborted aborted = assertInstanceOf(
                SessionToolExecution.Outcome.Aborted.class,
                join(SessionToolExecution.execute(
                        session, "run", "assistant", List.of(tool), null
                ))
        );
        assertEquals("run", aborted.runId());
        assertEquals(0, effects.get());
        assertTrue(toolStarts(session).isEmpty());
        assertEquals("assistant", join(session.leafId()));
    }

    @Test
    void durableAbortActivelyCancelsEveryParallelToolEffect() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "parallel-abort");
        appendOpenRun(session, "assistant", List.of(
                call("first-call", "first", Map.of()),
                call("second-call", "second", Map.of())
        ));
        AtomicInteger effects = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        AgentTool first = cancelAwareTool("first", effects, cancellations);
        AgentTool second = cancelAwareTool("second", effects, cancellations);
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of(
                        "first", SessionRecordDraft.Replay.NEVER,
                        "second", SessionRecordDraft.Replay.NEVER
                ),
                CLOCK, ToolExecutionMode.PARALLEL
        );
        CompletionStage<SessionToolExecution.Outcome> running =
                SessionToolExecution.execute(
                        session, "run", "assistant", List.of(first, second), options
                );
        assertEquals(2, effects.get());
        assertEquals(2, toolStarts(session).size());

        assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
        SessionToolExecution.Outcome.Aborted aborted = assertInstanceOf(
                SessionToolExecution.Outcome.Aborted.class, join(running)
        );
        assertEquals(2, cancellations.get());
        assertEquals(2, aborted.results().size());
        assertTrue(aborted.results().stream()
                .map(SessionToolExecutionTest::toolResult)
                .allMatch(ToolResultMessage::error));
        assertEquals(2, resultEntries(session).size());
        join(session.validateRecordLog("main"));
    }

    @Test
    void closeAfterNeverEffectStartLeavesUnresolvedWithoutReplay() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "never-close");
        SessionMetadata metadata = join(session.metadata());
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        AtomicInteger effects = new AtomicInteger();
        CompletableFuture<AgentToolResult> effect = new CompletableFuture<>();
        AgentTool tool = asynchronousTool("tool", effects, effect);
        CompletionStage<SessionToolExecution.Outcome> running =
                SessionToolExecution.execute(
                        session, "run", "assistant", List.of(tool),
                        new SessionToolExecution.Options(
                                Map.of("tool", SessionRecordDraft.Replay.NEVER), CLOCK
                        )
                );
        assertEquals(1, effects.get());
        assertEquals(1, toolStarts(session).size());
        join(session.close());
        effect.complete(result("effect-completed"));
        CompletionException closed = assertThrows(
                CompletionException.class, () -> join(running)
        );
        assertEquals(SessionError.Code.CLOSED,
                assertInstanceOf(SessionError.class, closed.getCause()).code());

        AgentSession reopened = join(repository.open(metadata));
        SessionToolExecution.Outcome.Suspended suspended = assertInstanceOf(
                SessionToolExecution.Outcome.Suspended.class,
                join(SessionToolExecution.resume(
                        reopened, "run", "assistant", List.of(tool),
                        new SessionToolExecution.Options(
                                Map.of("tool", SessionRecordDraft.Replay.NEVER), CLOCK
                        )
                ))
        );
        assertEquals(1, effects.get());
        assertEquals("call", suspended.unresolved().toolCallId());
        assertEquals("assistant", join(reopened.leafId()));
    }

    @Test
    void closeAfterSafeEffectStartAllowsExplicitReplay() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "safe-close");
        SessionMetadata metadata = join(session.metadata());
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        AtomicInteger effects = new AtomicInteger();
        CompletableFuture<AgentToolResult> firstEffect = new CompletableFuture<>();
        AgentTool firstTool = asynchronousTool("tool", effects, firstEffect);
        SessionToolExecution.Options options = new SessionToolExecution.Options(
                Map.of("tool", SessionRecordDraft.Replay.SAFE), CLOCK
        );
        CompletionStage<SessionToolExecution.Outcome> running =
                SessionToolExecution.execute(
                        session, "run", "assistant", List.of(firstTool), options
                );
        join(session.close());
        firstEffect.complete(result("unknown-first-result"));
        assertThrows(CompletionException.class, () -> join(running));

        AgentSession reopened = join(repository.open(metadata));
        AgentTool replayTool = tool(
                "tool", effects, (ignored, args) -> result("replayed"), reopened
        );
        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.resume(
                        reopened, "run", "assistant", List.of(replayTool), options
                ))
        );
        assertEquals(2, effects.get());
        assertEquals("replayed", ((TextContent) toolResult(
                completed.results().getFirst()
        ).content().getFirst()).text());
        assertEquals(completed.results().getFirst().id(), join(reopened.leafId()));
    }

    @Test
    void executeRefusesExistingStartsInsteadOfBypassingReplayPolicy() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "wrong-entrypoint");
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        appendToolStart(session, "start", 0, "call", "tool", "result",
                SessionRecordDraft.Replay.NEVER);
        AtomicInteger effects = new AtomicInteger();
        AgentTool tool = tool("tool", effects,
                (ignored, args) -> result("unsafe"), session);

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(SessionToolExecution.execute(
                        session, "run", "assistant", List.of(tool), null
                ))
        );
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(SessionError.Code.INVALID_PAYLOAD, error.code());
        assertTrue(error.getMessage().contains("use resume"));
        assertEquals(0, effects.get());
    }

    @Test
    void jsonlSafeRecoveryPublishesResultAndUsageAtomically() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession session = create(repository, "jsonl-tool");
        appendOpenRun(session, "assistant", List.of(
                call("call", "tool", Map.of())
        ));
        appendToolStart(session, "tool-start", 0, "call", "tool", "tool-result",
                SessionRecordDraft.Replay.SAFE);
        SessionMetadata metadata = join(session.metadata());
        AtomicInteger effects = new AtomicInteger();
        AgentTool tool = tool("tool", effects,
                (ignored, args) -> result("recovered"), session);

        AgentSession reopened = join(repository.open(metadata));
        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.resume(
                        reopened, "run", "assistant", List.of(tool),
                        new SessionToolExecution.Options(
                                Map.of("tool", SessionRecordDraft.Replay.SAFE), CLOCK
                        )
                ))
        );
        assertEquals("tool-result", completed.results().getFirst().id());
        AgentSession verified = join(repository.open(metadata));
        assertEquals("tool-result", join(verified.leafId()));
        assertEquals(1, join(verified.usage()).size());
        assertEquals(1, effects.get());
        join(verified.validateRecordLog("main"));
    }

    private static AgentTool tool(
            String name,
            AtomicInteger effects,
            ToolEffect effect,
            AgentSession session
    ) {
        return new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition(name, name, Map.of());
            }

            @Override
            public Map<String, Object> prepareArguments(Map<String, Object> arguments) {
                LinkedHashMap<String, Object> prepared = new LinkedHashMap<>(arguments);
                if (prepared.containsKey("value")) {
                    prepared.put("prepared", ((Number) prepared.get("value")).intValue() * 2);
                }
                return prepared;
            }

            @Override
            public CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                effects.incrementAndGet();
                return CompletableFuture.completedFuture(effect.apply(session, arguments));
            }
        };
    }

    private static AgentTool cancelAwareTool(
            String name,
            AtomicInteger effects,
            AtomicInteger cancellations
    ) {
        return new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition(name, name, Map.of());
            }

            @Override
            public CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                effects.incrementAndGet();
                CompletableFuture<AgentToolResult> result = new CompletableFuture<>();
                cancellation.onCancel(() -> {
                    cancellations.incrementAndGet();
                    result.completeExceptionally(new CancellationException("aborted"));
                });
                return result;
            }
        };
    }

    private static AgentTool asynchronousTool(
            String name,
            AtomicInteger effects,
            CompletableFuture<AgentToolResult> effect
    ) {
        return new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition(name, name, Map.of());
            }

            @Override
            public CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                effects.incrementAndGet();
                return effect;
            }
        };
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

    private static AgentToolResult result(String text) {
        return new AgentToolResult(
                List.of(new TextContent(text)), Map.of("text", text),
                new Usage(1, 2, 0, 0, 0, 3, Cost.ZERO), false
        );
    }

    private static void appendOpenRun(
            AgentSession session,
            String assistantId,
            List<ToolCallContent> calls
    ) {
        AssistantMessage assistant = new AssistantMessage(
                List.copyOf(calls), "test-api", "test-provider", "test-model",
                Usage.ZERO, StopReason.TOOL_USE, null, 1
        );
        join(session.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(), List.of(), "system", null
                    )
            ));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "assistant-attempt", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1, assistantId, null
            ));
            transaction.append(new SessionEntryDraft.Message(assistantId, assistant));
            return null;
        }));
    }

    private static void appendToolStart(
            AgentSession session,
            String id,
            int index,
            String callId,
            String toolName,
            String resultId,
            SessionRecordDraft.Replay replay
    ) {
        join(session.appendRecord(new SessionRecordDraft.ToolStarted(
                id, "main", "run", "assistant", index,
                callId, toolName, com.fasterxml.jackson.databind.node.JsonNodeFactory
                .instance.objectNode(), resultId, replay
        )));
    }

    private static ToolCallContent call(
            String id,
            String name,
            Map<String, Object> arguments
    ) {
        return new ToolCallContent(id, name, arguments);
    }

    private static List<SessionRecordDraft.ToolStarted> toolStarts(
            AgentSession session
    ) {
        return join(session.findRecords(new SessionRecordQuery(
                "main", SessionRecordDraft.Type.TOOL_STARTED,
                "run", null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        ))).stream().map(SessionRecord::value)
                .map(SessionRecordDraft.ToolStarted.class::cast).toList();
    }

    private static List<SessionEntry.Message> resultEntries(AgentSession session) {
        return join(session.findEntries(new SessionEntryQuery(
                SessionEntry.Type.MESSAGE, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null, null
        ))).stream().filter(SessionEntry.Message.class::isInstance)
                .map(SessionEntry.Message.class::cast)
                .filter(value -> value.message() instanceof ToolResultMessage).toList();
    }

    private static ToolResultMessage toolResult(SessionEntry.Message entry) {
        return (ToolResultMessage) entry.message();
    }

    private static AgentSession create(SessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    @FunctionalInterface
    private interface ToolEffect {
        AgentToolResult apply(AgentSession session, Map<String, Object> arguments);
    }
}
