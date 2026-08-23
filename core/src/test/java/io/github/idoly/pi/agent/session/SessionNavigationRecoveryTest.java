package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionNavigationRecoveryTest {
    @TempDir
    Path temporary;

    @Test
    void reopensUnknownEffectAsALaterAttemptAndPreservesPriorUsage() {
        JsonlSessionRepository repository = repository();
        AgentSession original = create(repository, "retry");
        SessionEntry target = append(original, "target", "target");
        SessionEntry source = append(original, "source", "source");
        accept(original, "run", source.id(), target.id(), true, "summary");
        join(original.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run", SessionRecordDraft.Step.BRANCH_SUMMARY,
                1, "summary", null
        )));
        join(original.appendRecord(new SessionRecordDraft.UsageRecord(
                "usage-1", "main", "branch_summary", usage(3),
                "run", null, 1, "error", null, null
        )));

        AgentSession reopened = join(repository().open(join(original.metadata())));
        AtomicInteger dispatches = new AtomicInteger();
        SessionNavigation.Outcome.Completed result = assertInstanceOf(
                SessionNavigation.Outcome.Completed.class,
                join(SessionNavigation.resume(
                        reopened,
                        new SessionNavigation.RecoveryOptions(1000, 200, 3),
                        request -> {
                            dispatches.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("recovered", usage(5))
                            );
                        }
                ))
        );

        assertEquals(1, dispatches.get());
        assertEquals("summary", result.summaryEntry().id());
        assertEquals(target.id(), result.summaryEntry().parentId());
        assertEquals(source.id(), result.summaryEntry().fromId());
        assertEquals("summary", join(reopened.leafId()));
        assertEquals(List.of(1, 2), recordsOldest(reopened).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
        assertEquals(List.of(3L, 5L), join(reopened.usage()).stream()
                .map(row -> row.usage().totalTokens()).toList());
        assertTrue(join(reopened.findOpenOperations("main", 2)).isEmpty());
        join(reopened.validateRecordLog("main"));
    }

    @Test
    void resumesUnsummarizedAcceptanceWithoutProviderDispatch() {
        JsonlSessionRepository repository = repository();
        AgentSession original = create(repository, "plain");
        SessionEntry target = append(original, "target", "target");
        SessionEntry source = append(original, "source", "source");
        accept(original, "run", source.id(), target.id(), false, null);

        AgentSession reopened = join(repository().open(join(original.metadata())));
        SessionNavigation.Outcome.Completed result = assertInstanceOf(
                SessionNavigation.Outcome.Completed.class,
                join(SessionNavigation.resume(
                        reopened,
                        new SessionNavigation.RecoveryOptions(0, 0, 1), null
                ))
        );
        assertEquals(target.id(), result.leafId());
        assertNull(result.summaryEntry());
        assertEquals(target.id(), join(reopened.leafId()));
    }

    @Test
    void exhaustedUnknownEffectFailsWithoutDispatchingAgain() {
        AgentSession session = memory("exhausted");
        SessionEntry target = append(session, "target", "target");
        SessionEntry source = append(session, "source", "source");
        accept(session, "run", source.id(), target.id(), true, "summary");
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run", SessionRecordDraft.Step.BRANCH_SUMMARY,
                1, "summary", null
        )));
        AtomicInteger dispatches = new AtomicInteger();

        SessionNavigation.Outcome.Failed result = assertInstanceOf(
                SessionNavigation.Outcome.Failed.class,
                join(SessionNavigation.resume(
                        session,
                        new SessionNavigation.RecoveryOptions(100, 100, 1),
                        ignored -> {
                            dispatches.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("unused", Usage.ZERO)
                            );
                        }
                ))
        );
        assertEquals(0, dispatches.get());
        assertTrue(result.error().message().contains("exhausted at 1"));
        assertEquals(source.id(), result.leafId());
        assertEquals(source.id(), join(session.leafId()));
        assertTrue(join(session.findOpenOperations("main", 2)).isEmpty());
    }

    @Test
    void durableAbortShortCircuitsRecoveryBeforeAttemptOrProvider() {
        AgentSession session = memory("abort-recovery");
        SessionEntry target = append(session, "target", "target");
        SessionEntry source = append(session, "source", "source");
        accept(session, "run", source.id(), target.id(), true, "summary");
        assertTrue(join(SessionNavigation.requestAbort(session, "run")));
        assertEquals(false, join(SessionNavigation.requestAbort(session, "run")));
        AtomicInteger dispatches = new AtomicInteger();

        SessionNavigation.Outcome.Aborted result = assertInstanceOf(
                SessionNavigation.Outcome.Aborted.class,
                join(SessionNavigation.resume(
                        session,
                        new SessionNavigation.RecoveryOptions(100, 100, 3),
                        ignored -> {
                            dispatches.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("unused", Usage.ZERO)
                            );
                        }
                ))
        );
        assertEquals(0, dispatches.get());
        assertEquals(source.id(), result.leafId());
        assertEquals(0, recordsOldest(session).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance).count());
    }

    @Test
    void rejectsConcurrentResumeOfTheSameLoadedOperation() {
        AgentSession session = memory("concurrent");
        SessionEntry target = append(session, "target", "target");
        SessionEntry source = append(session, "source", "source");
        accept(session, "run", source.id(), target.id(), true, "summary");
        CompletableFuture<CompactionSummarizer.Summary> effect = new CompletableFuture<>();
        var first = SessionNavigation.resume(
                session, new SessionNavigation.RecoveryOptions(100, 100, 3),
                ignored -> effect
        );

        CompletionException duplicate = assertThrows(
                CompletionException.class,
                () -> join(SessionNavigation.resume(
                        session, new SessionNavigation.RecoveryOptions(100, 100, 3),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("duplicate", Usage.ZERO)
                        )
                ))
        );
        assertEquals(SessionError.Code.STORAGE,
                ((SessionError) duplicate.getCause()).code());
        assertTrue(duplicate.getCause().getMessage().contains("already executing"));

        effect.complete(new CompactionSummarizer.Summary("first", Usage.ZERO));
        assertInstanceOf(SessionNavigation.Outcome.Completed.class, join(first));
    }

    @Test
    void rejectsInconsistentDurableSummaryReservationWithoutFinishingOperation() {
        AgentSession session = memory("inconsistent");
        SessionEntry target = append(session, "target", "target");
        SessionEntry source = append(session, "source", "source");
        accept(session, "run", source.id(), target.id(), true, null);

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(SessionNavigation.resume(
                        session,
                        new SessionNavigation.RecoveryOptions(100, 100, 2),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("unused", Usage.ZERO)
                        )
                ))
        );
        assertInstanceOf(RecordLogCorruption.class, failure.getCause());
        assertEquals(RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                ((RecordLogCorruption) failure.getCause()).reason());
        assertEquals(1, join(session.findOpenOperations("main", 2)).size());
        assertEquals(source.id(), join(session.leafId()));
    }

    @Test
    void abortRejectsUnknownOrFinishedOperation() {
        AgentSession session = memory("unknown-abort");
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(SessionNavigation.requestAbort(session, "missing"))
        );
        assertEquals(SessionError.Code.NOT_FOUND,
                ((SessionError) failure.getCause()).code());
    }

    private JsonlSessionRepository repository() {
        return new JsonlSessionRepository(temporary.resolve("sessions"), temporary);
    }

    private static AgentSession memory(String id) {
        AtomicInteger ids = new AtomicInteger();
        InMemorySessionRepository repository = new InMemorySessionRepository(
                Clock.systemUTC(), ignored -> id + "-id-" + ids.incrementAndGet()
        );
        return create(repository, id);
    }

    private static AgentSession create(SessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionEntry append(AgentSession session, String id, String text) {
        return join(session.append(new SessionEntryDraft.Message(
                id, UserMessage.text(text, 1)
        )));
    }

    private static void accept(
            AgentSession session,
            String runId,
            String source,
            String target,
            boolean summarize,
            String summaryEntryId
    ) {
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                runId, "main", source,
                new SessionRecordDraft.OperationIntent.Navigation(
                        target, summarize, null, null, summaryEntryId
                )
        )));
    }

    private static List<SessionRecord> recordsOldest(AgentSession session) {
        return join(session.findRecords(new SessionRecordQuery(
                null, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )));
    }

    private static Usage usage(long tokens) {
        return new Usage(tokens, 0, 0, 0, tokens, Cost.ZERO);
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
