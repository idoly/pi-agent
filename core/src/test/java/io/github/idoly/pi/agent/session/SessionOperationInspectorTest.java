package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.harness.SuspendedOperation;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionOperationInspectorTest {
    @TempDir
    Path temporary;

    @Test
    void reconstructsOpenOperationsAcrossLanesInLaneOrder() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "multiple");
        SessionEntry root = append(session, "root", "root");
        join(session.createLane("thread", root.id()));
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "navigation", "main", root.id(),
                new SessionRecordDraft.OperationIntent.Navigation(
                        null, false, null, null, null
                )
        )));
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "compaction", "thread", root.id(),
                new SessionRecordDraft.OperationIntent.Compaction(null, "checkpoint")
        )));
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "thread", "compaction",
                SessionRecordDraft.Step.COMPACTION, 1, "checkpoint",
                SessionRecordDraft.CompactionReason.MANUAL
        )));
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-2", "thread", "compaction",
                SessionRecordDraft.Step.COMPACTION, 2, "checkpoint",
                SessionRecordDraft.CompactionReason.MANUAL
        )));
        join(session.appendRecord(new SessionRecordDraft.AbortRequested(
                "abort", "main", "navigation"
        )));

        List<SessionOperationInspector.OpenOperation> operations =
                join(SessionOperationInspector.inspect(session));
        assertEquals(List.of("main", "thread"), operations.stream()
                .map(SessionOperationInspector.OpenOperation::lane).toList());

        SessionOperationInspector.OpenOperation navigation = operations.getFirst();
        assertEquals("navigation", navigation.id());
        assertEquals(SuspendedOperation.Kind.NAVIGATION, navigation.kind());
        assertEquals(SessionOperationInspector.Status.ABORTING, navigation.status());
        assertEquals(root.id(), navigation.sourceLeafId());
        assertNull(navigation.latestAttempt());

        SessionOperationInspector.OpenOperation compaction = operations.getLast();
        assertEquals(SuspendedOperation.Kind.COMPACTION, compaction.kind());
        assertEquals(SessionOperationInspector.Status.SUSPENDED, compaction.status());
        assertEquals(2, compaction.latestAttempt().attempt());
        assertEquals(SessionRecordDraft.Step.COMPACTION,
                compaction.latestAttempt().step());
        assertEquals(SessionRecordDraft.CompactionReason.MANUAL,
                compaction.latestAttempt().compactionReason());
    }

    @Test
    void reportsMissingToolResultsWithExplicitRecoveryActionAfterReopen() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("tool-diagnostics"), temporary
        );
        AgentSession session = create(repository, "tool-diagnostics");
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new ToolCallContent("safe-call", "safe", Map.of()),
                        new ToolCallContent("never-call", "never", Map.of())
                ),
                "api", "provider", "model", Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        join(session.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(), List.of(), "system", null
                    )
            ));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "attempt", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1, "assistant", null
            ));
            transaction.append(new SessionEntryDraft.Message("assistant", assistant));
            transaction.appendRecord(new SessionRecordDraft.ToolStarted(
                    "safe-start", "main", "run", "assistant", 0,
                    "safe-call", "safe",
                    com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                    "safe-result", SessionRecordDraft.Replay.SAFE
            ));
            transaction.appendRecord(new SessionRecordDraft.ToolStarted(
                    "never-start", "main", "run", "assistant", 1,
                    "never-call", "never",
                    com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                    "never-result", SessionRecordDraft.Replay.NEVER
            ));
            return null;
        }));
        SessionMetadata metadata = join(session.metadata());

        AgentSession reopened = join(repository.open(metadata));
        SessionOperationInspector.OpenOperation operation =
                join(SessionOperationInspector.inspectLane(reopened));
        assertEquals(List.of(0, 1), operation.unresolvedTools().stream()
                .map(SessionOperationInspector.UnresolvedToolEffect::toolIndex)
                .toList());
        assertEquals(List.of(
                SessionOperationInspector.ToolRecovery.REPLAY_ALLOWED,
                SessionOperationInspector.ToolRecovery.ADMINISTRATIVE_RESULT_REQUIRED
        ), operation.unresolvedTools().stream()
                .map(SessionOperationInspector.UnresolvedToolEffect::recovery)
                .toList());
        assertEquals(List.of("safe-result", "never-result"),
                operation.unresolvedTools().stream()
                        .map(SessionOperationInspector.UnresolvedToolEffect::resultEntryId)
                        .toList());

        join(reopened.transaction(transaction -> {
            transaction.append(new SessionEntryDraft.Message(
                    "safe-result",
                    new io.github.idoly.pi.ai.ToolResultMessage(
                            "safe-call", "safe", List.of(), Map.of(), null,
                            false, 2
                    )
            ));
            return null;
        }));
        SessionOperationInspector.OpenOperation afterSafe =
                join(SessionOperationInspector.inspectLane(reopened));
        assertEquals(List.of("never-result"), afterSafe.unresolvedTools().stream()
                .map(SessionOperationInspector.UnresolvedToolEffect::resultEntryId)
                .toList());
    }

    @Test
    void projectsPublishedSuspendedOperationShape() {
        AgentSession session = create(new InMemorySessionRepository(), "projection");
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), null, null
                )
        )));
        SuspendedOperation suspended =
                join(SessionOperationInspector.suspended(session)).getFirst();
        assertEquals("main", suspended.lane());
        assertEquals("run", suspended.id());
        assertEquals(SuspendedOperation.Kind.RUN, suspended.kind());
        assertEquals(SuspendedOperation.Reason.CRASH, suspended.reason());
        assertEquals(List.of(), suspended.missingModels());
        assertEquals(List.of(), suspended.missingTools());
    }

    @Test
    void reopenedJsonlOperationIsReportedAsCrashSuspended() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession original = create(repository, "jsonl");
        SessionEntry source = append(original, "source", "source");
        join(original.appendRecord(new SessionRecordDraft.OperationStarted(
                "navigation", "main", source.id(),
                new SessionRecordDraft.OperationIntent.Navigation(
                        null, false, null, null, null
                )
        )));
        SessionMetadata metadata = join(original.metadata());

        AgentSession reopened = join(new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        ).open(metadata));
        SessionOperationInspector.OpenOperation operation =
                join(SessionOperationInspector.inspectLane(reopened));
        assertEquals("navigation", operation.id());
        assertEquals(SessionOperationInspector.Status.SUSPENDED, operation.status());
        assertEquals(source.id(), operation.sourceLeafId());
        assertEquals(SuspendedOperation.Reason.CRASH,
                operation.asSuspendedOperation().reason());
    }

    @Test
    void idleLaneProducesNoSuspendedOperation() {
        AgentSession session = create(new InMemorySessionRepository(), "idle");
        assertNull(join(SessionOperationInspector.inspectLane(session)));
        assertEquals(List.of(), join(SessionOperationInspector.inspect(session)));
        assertEquals(List.of(), join(SessionOperationInspector.suspended(session)));
    }

    @Test
    void corruptionIsPropagatedInsteadOfProducingASnapshot() {
        AgentSession session = create(new InMemorySessionRepository(), "corrupt");
        join(session.appendRecord(new SessionRecordDraft.AbortRequested(
                "abort", "main", "missing"
        )));
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(SessionOperationInspector.inspectLane(session))
        );
        assertInstanceOf(RecordLogCorruption.class, failure.getCause());
        assertEquals(RecordLogCorruption.Reason.UNKNOWN_OPERATION,
                ((RecordLogCorruption) failure.getCause()).reason());
    }

    private static AgentSession create(SessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionEntry append(AgentSession session, String id, String text) {
        return join(session.append(new SessionEntryDraft.Message(
                id, UserMessage.text(text, 1)
        )));
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
