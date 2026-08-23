package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.agent.harness.SuspendedOperation;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionLastResultTest {
    @TempDir
    Path temporary;

    @Test
    void reconstructsLeafAtFinishRatherThanUsingCurrentLeaf() {
        AgentSession session = memory("historical");
        SessionEntry source = append(session, "source", "source");
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "navigation", "main", source.id(),
                new SessionRecordDraft.OperationIntent.Navigation(
                        null, false, null, null, null
                )
        )));
        join(session.transaction(transaction -> {
            transaction.moveLane(null);
            transaction.appendRecord(new SessionRecordDraft.OperationFinished(
                    "finish", "main", "navigation",
                    SessionRecordDraft.OperationOutcome.COMPLETED, null
            ));
            return null;
        }));
        append(session, "later", "later");

        SessionOperationInspector.LastResult result = join(session.lastResult());
        assertEquals("navigation", result.runId());
        assertEquals(SuspendedOperation.Kind.NAVIGATION, result.kind());
        assertEquals(SessionRecordDraft.OperationOutcome.COMPLETED, result.outcome());
        assertNull(result.leafId());
        assertNull(result.resultEntryId());
        assertEquals("later", join(session.leafId()));
        assertEquals("finish", recordsOldest(session).stream()
                .filter(record -> record.sequence() == result.finishSequence())
                .findFirst().orElseThrow().id());
    }

    @Test
    void exposesPublishedCompactionEntryAndTerminalUsage() {
        AgentSession session = memory("compaction");
        append(session, "first", "history");
        append(session, "source", "recent");
        SessionCompactionOperation.Outcome.Completed completed =
                (SessionCompactionOperation.Outcome.Completed) join(
                        SessionCompactionOperation.compact(
                                session,
                                new SessionCompactionOperation.Options(
                                        new CompactionSettings(true, 100, 1),
                                        SessionRecordDraft.CompactionReason.MANUAL,
                                        0, null
                                ),
                                ignored -> CompletableFuture.completedFuture(
                                        new CompactionSummarizer.Summary(
                                                "summary", Usage.ZERO
                                        )
                                )
                        )
                );

        SessionOperationInspector.LastResult result = join(session.lastResult());
        assertEquals(completed.runId(), result.runId());
        assertEquals(SuspendedOperation.Kind.COMPACTION, result.kind());
        assertEquals(completed.entry().id(), result.leafId());
        assertEquals(completed.entry().id(), result.resultEntryId());
        assertNull(result.error());
        assertTrueOrder(result.startedAt(), result.finishedAt());
    }

    @Test
    void preservesFailedErrorAndSourceLeaf() {
        AgentSession session = memory("failed");
        append(session, "first", "history");
        SessionEntry source = append(session, "source", "recent");
        SessionCompactionOperation.Outcome.Failed failed =
                (SessionCompactionOperation.Outcome.Failed) join(
                        SessionCompactionOperation.compact(
                                session,
                                new SessionCompactionOperation.Options(
                                        new CompactionSettings(true, 100, 1),
                                        SessionRecordDraft.CompactionReason.MANUAL,
                                        0, null
                                ),
                                ignored -> CompletableFuture.failedFuture(
                                        new IllegalStateException("provider failed")
                                )
                        )
                );

        SessionOperationInspector.LastResult result = join(session.lastResult());
        assertEquals(SessionRecordDraft.OperationOutcome.FAILED, result.outcome());
        assertEquals(source.id(), result.leafId());
        assertNull(result.resultEntryId());
        assertEquals(failed.error(), result.error());
        assertEquals("provider failed", result.error().message());
    }

    @Test
    void returnsOnlyNewestTerminalResultPerLaneInLaneOrder() {
        AgentSession session = memory("lanes");
        SessionEntry root = append(session, "root", "root");
        join(session.createLane("thread", root.id()));
        finish(session, "old-main", "main", root.id(),
                SessionRecordDraft.OperationOutcome.ABORTED);
        finish(session, "new-main", "main", root.id(),
                SessionRecordDraft.OperationOutcome.DECLINED);
        finish(session, "thread-run", "thread", root.id(),
                SessionRecordDraft.OperationOutcome.COMPLETED);

        List<SessionOperationInspector.LastResult> results =
                join(SessionOperationInspector.lastResults(session));
        assertEquals(List.of("main", "thread"), results.stream()
                .map(SessionOperationInspector.LastResult::lane).toList());
        assertEquals("new-main", results.getFirst().runId());
        assertEquals(SessionRecordDraft.OperationOutcome.DECLINED,
                results.getFirst().outcome());
        assertEquals("thread-run", results.getLast().runId());
        assertEquals(root.id(), results.getLast().leafId());
    }

    @Test
    void survivesJsonlReopenAndOpenOperationDoesNotReplaceLastTerminalResult() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession original = join(repository.create(
                new SessionRepository.CreateOptions("jsonl-result", null)
        ));
        SessionEntry root = append(original, "root", "root");
        finish(original, "finished", "main", root.id(),
                SessionRecordDraft.OperationOutcome.COMPLETED);
        join(original.appendRecord(new SessionRecordDraft.OperationStarted(
                "open", "main", root.id(),
                new SessionRecordDraft.OperationIntent.Navigation(
                        null, false, null, null, null
                )
        )));

        AgentSession reopened = join(new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        ).open(join(original.metadata())));
        SessionOperationInspector.LastResult result = join(reopened.lastResult());
        assertEquals("finished", result.runId());
        assertEquals(root.id(), result.leafId());
        assertEquals(1, join(reopened.findOpenOperations("main", 2)).size());
    }

    @Test
    void failedTerminalTransactionRollsBackLastResultIndex() {
        AgentSession session = memory("rollback-index");
        SessionEntry root = append(session, "root", "root");
        finish(session, "old", "main", root.id(),
                SessionRecordDraft.OperationOutcome.ABORTED);
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "new", "main", root.id(),
                new SessionRecordDraft.OperationIntent.Navigation(
                        null, false, null, null, null
                )
        )));

        assertThrows(java.util.concurrent.CompletionException.class, () ->
                join(session.transaction(transaction -> {
                    transaction.moveLane(null);
                    transaction.appendRecord(new SessionRecordDraft.OperationFinished(
                            "new-finish", "main", "new",
                            SessionRecordDraft.OperationOutcome.COMPLETED, null
                    ));
                    throw new IllegalStateException("rollback");
                }))
        );

        SessionOperationInspector.LastResult result = join(session.lastResult());
        assertEquals("old", result.runId());
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED, result.outcome());
        assertEquals(root.id(), result.leafId());
        assertEquals(root.id(), join(session.leafId()));
        assertEquals(List.of("new"), join(session.findOpenOperations("main", 2)).stream()
                .map(SessionRecord::id).toList());
    }

    @Test
    void completedStructuralResultMustExist() {
        AgentSession session = memory("missing-result");
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "compaction", "main", null,
                new SessionRecordDraft.OperationIntent.Compaction(null, "missing")
        )));
        join(session.appendRecord(new SessionRecordDraft.OperationFinished(
                "finish", "main", "compaction",
                SessionRecordDraft.OperationOutcome.COMPLETED, null
        )));

        java.util.concurrent.CompletionException failure = assertThrows(
                java.util.concurrent.CompletionException.class,
                () -> join(session.lastResult())
        );
        RecordLogCorruption corruption = assertInstanceOf(
                RecordLogCorruption.class, failure.getCause()
        );
        assertEquals(RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                corruption.reason());
    }

    @Test
    void sessionWithoutFinishHasNoLastResult() {
        AgentSession session = memory("none");
        assertNull(join(session.lastResult()));
        assertEquals(List.of(), join(SessionOperationInspector.lastResults(session)));
    }

    private static void finish(
            AgentSession session,
            String runId,
            String lane,
            String source,
            SessionRecordDraft.OperationOutcome outcome
    ) {
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                runId, lane, source,
                new SessionRecordDraft.OperationIntent.Navigation(
                        source, false, null, null, null
                )
        )));
        join(session.appendRecord(new SessionRecordDraft.OperationFinished(
                runId + "-finish", lane, runId, outcome,
                outcome == SessionRecordDraft.OperationOutcome.FAILED
                        ? new SessionRecordDraft.OperationError("failed", "failed")
                        : null
        )));
    }

    private static AgentSession memory(String id) {
        AtomicInteger ids = new AtomicInteger();
        InMemorySessionRepository repository = new InMemorySessionRepository(
                Clock.systemUTC(), ignored -> id + "-id-" + ids.incrementAndGet()
        );
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionEntry append(AgentSession session, String id, String text) {
        return join(session.append(new SessionEntryDraft.Message(
                id, UserMessage.text(text, 1)
        )));
    }

    private static List<SessionRecord> recordsOldest(AgentSession session) {
        return join(session.findRecords(new SessionRecordQuery(
                null, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )));
    }

    private static void assertTrueOrder(long startedAt, long finishedAt) {
        if (finishedAt < startedAt) {
            throw new AssertionError("finish timestamp precedes acceptance");
        }
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
