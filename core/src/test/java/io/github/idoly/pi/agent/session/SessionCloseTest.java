package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionCloseTest {
    @TempDir
    Path temporary;

    @Test
    void closeIsIdempotentAndSharedByEveryLaneView() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession main = create(repository, "shared");
        SessionEntry root = append(main, "root", "root");
        join(main.createLane("thread", root.id()));
        AgentSession thread = main.view("thread");

        assertFalse(main.isClosed());
        join(thread.close());
        join(thread.close());
        assertTrue(main.isClosed());
        assertTrue(thread.isClosed());
        assertEquals("main", main.lane());
        assertEquals("thread", thread.lane());

        assertClosed(main.metadata());
        assertClosed(main.leafId());
        assertClosed(main.lanes());
        assertClosed(main.findEntries());
        assertClosed(main.findRecords());
        assertClosed(main.log(0, null));
        assertClosed(main.context());
        assertClosed(main.lastResult());
        assertClosed(main.append(new SessionEntryDraft.Message(
                "closed", UserMessage.text("closed", 2)
        )));
        assertClosed(main.transaction(ignored -> null));
        SessionError viewFailure = assertThrows(
                SessionError.class, () -> main.view("main")
        );
        assertEquals(SessionError.Code.CLOSED, viewFailure.code());
        SessionError operationFailure = assertThrows(
                SessionError.class,
                () -> SessionNavigation.navigate(
                        main, null, SessionNavigation.Options.DEFAULT, null
                )
        );
        assertEquals(SessionError.Code.CLOSED, operationFailure.code());
    }

    @Test
    void independentlyOpenedHandleRemainsUsableAndReopenRestoresClosedData() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession first = create(repository, "independent");
        append(first, "root", "root");
        SessionMetadata metadata = join(first.metadata());
        AgentSession second = join(repository.open(metadata));

        join(first.close());
        assertTrue(first.isClosed());
        assertFalse(second.isClosed());
        SessionEntry next = append(second, "next", "next");
        assertEquals(2, next.sequence());

        AgentSession third = join(repository.open(metadata));
        assertEquals(List.of("next", "root"), join(third.findEntries()).stream()
                .map(SessionEntry::id).toList());
    }

    @Test
    void jsonlCloseDoesNotDeleteOrInvalidateDurableSession() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession session = create(repository, "jsonl");
        append(session, "root", "root");
        SessionMetadata metadata = join(session.metadata());
        join(session.close());
        assertClosed(session.findEntries());

        AgentSession reopened = join(repository.open(metadata));
        assertFalse(reopened.isClosed());
        assertEquals(List.of("root"), join(reopened.findEntries()).stream()
                .map(SessionEntry::id).toList());
        append(reopened, "next", "next");
        assertEquals(2, join(reopened.log(0, null)).size());
    }

    @Test
    void closeDuringEffectLeavesRecoverableOpenOperationAndReleasesClaim() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "inflight");
        append(session, "first", "history");
        append(session, "source", "recent");
        SessionMetadata metadata = join(session.metadata());
        CompletableFuture<CompactionSummarizer.Summary> effect = new CompletableFuture<>();
        var operation = SessionCompactionOperation.compact(
                session,
                new SessionCompactionOperation.Options(
                        new CompactionSettings(true, 100, 1),
                        SessionRecordDraft.CompactionReason.MANUAL,
                        0, null
                ),
                ignored -> effect
        );
        join(session.close());
        effect.complete(new CompactionSummarizer.Summary("lost", Usage.ZERO));
        assertClosed(operation);

        AgentSession reopened = join(repository.open(metadata));
        SessionOperationInspector.OpenOperation suspended =
                join(SessionOperationInspector.inspectLane(reopened));
        assertEquals(SessionOperationInspector.Status.SUSPENDED, suspended.status());
        assertEquals(1, suspended.latestAttempt().attempt());

        SessionCompactionOperation.Outcome.Completed recovered = assertInstanceOf(
                SessionCompactionOperation.Outcome.Completed.class,
                join(SessionCompactionOperation.resume(
                        reopened,
                        new SessionCompactionOperation.RecoveryOptions(
                                new CompactionSettings(true, 100, 1),
                                SessionRecordDraft.CompactionReason.MANUAL, 2
                        ),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("recovered", Usage.ZERO)
                        )
                ))
        );
        assertEquals("recovered", recovered.entry().summary());
        assertEquals(2, recordsOldest(reopened).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .count());
    }

    private static void assertClosed(java.util.concurrent.CompletionStage<?> stage) {
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> stage.toCompletableFuture().join()
        );
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(SessionError.Code.CLOSED, error.code());
    }

    private static List<SessionRecord> recordsOldest(AgentSession session) {
        return join(session.findRecords(new SessionRecordQuery(
                null, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )));
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
