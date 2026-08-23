package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.agent.harness.CompactionSettings;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionCompactionOperationTest {
    private static final CompactionSettings SETTINGS =
            new CompactionSettings(true, 100, 1);

    @TempDir
    Path temporary;

    @Test
    void publishesCheckpointAndFinishInOneTerminalTransaction() {
        AgentSession session = memory("complete");
        append(session, "first", "history that should be summarized");
        SessionEntry source = append(session, "second", "recent");
        AtomicReference<CompactionSummarizer.Request> request = new AtomicReference<>();

        SessionCompactionOperation.Outcome.Completed outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Completed.class,
                join(SessionCompactionOperation.compact(
                        session,
                        new SessionCompactionOperation.Options(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL,
                                0, "focus"
                        ),
                        value -> {
                            request.set(value);
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("summary", usage(5))
                            );
                        }
                ))
        );

        SessionEntry.Compaction checkpoint = outcome.entry();
        assertEquals(source.id(), checkpoint.parentId());
        assertEquals(checkpoint.id(), outcome.leafId());
        assertEquals(checkpoint.id(), join(session.leafId()));
        assertEquals("summary", checkpoint.summary());
        assertEquals(usage(5), checkpoint.usage());
        assertEquals("focus", request.get().customInstructions());
        assertEquals(80, request.get().maxTokens());
        assertTrue(join(session.findOpenOperations("main", 2)).isEmpty());
        assertEquals(List.of(
                SessionRecordDraft.Type.OPERATION_STARTED,
                SessionRecordDraft.Type.STEP_ATTEMPT,
                SessionRecordDraft.Type.USAGE,
                SessionRecordDraft.Type.OPERATION_FINISHED
        ), recordsOldest(session).stream().map(SessionRecord::type).toList());
        assertEquals(1, join(session.usage()).size());
        join(session.validateRecordLog("main"));
    }

    @Test
    void thresholdAndNothingToCompactRejectWithoutWritingAcceptance() {
        AgentSession session = memory("threshold");
        append(session, "short", "short");
        long before = join(session.log(0, null)).size();
        assertCode(SessionError.Code.INVALID_PAYLOAD,
                SessionCompactionOperation.compact(
                        session,
                        new SessionCompactionOperation.Options(
                                new CompactionSettings(true, 10, 1),
                                SessionRecordDraft.CompactionReason.THRESHOLD,
                                1000, null
                        ),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("unused", Usage.ZERO)
                        )
                ));
        assertEquals(before, join(session.log(0, null)).size());
        assertEquals(0, join(session.findRecords()).size());

        AgentSession empty = memory("empty");
        assertCode(SessionError.Code.INVALID_PAYLOAD,
                SessionCompactionOperation.compact(
                        empty,
                        new SessionCompactionOperation.Options(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL,
                                0, null
                        ),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("unused", Usage.ZERO)
                        )
                ));
        assertEquals(0, join(empty.log(0, null)).size());
    }

    @Test
    void thresholdCompactionRunsWhenCalculatedContextCrossesReserve() {
        AgentSession session = memory("threshold-run");
        append(session, "first", "x".repeat(200));
        append(session, "second", "recent");
        SessionCompactionOperation.Outcome.Completed outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Completed.class,
                join(SessionCompactionOperation.compact(
                        session,
                        new SessionCompactionOperation.Options(
                                new CompactionSettings(true, 10, 1),
                                SessionRecordDraft.CompactionReason.THRESHOLD,
                                40, null
                        ),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("threshold", Usage.ZERO)
                        )
                ))
        );
        assertEquals("threshold", outcome.entry().summary());
        SessionRecordDraft.StepAttempt attempt = recordsOldest(session).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .findFirst().orElseThrow();
        assertEquals(SessionRecordDraft.CompactionReason.THRESHOLD,
                attempt.compactionReason());
    }

    @Test
    void effectFailureFinishesWithoutCheckpointOrLeafMovement() {
        AgentSession session = memory("failure");
        append(session, "first", "history");
        SessionEntry source = append(session, "source", "recent");
        SessionCompactionOperation.Outcome.Failed outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Failed.class,
                join(SessionCompactionOperation.compact(
                        session,
                        new SessionCompactionOperation.Options(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL,
                                0, null
                        ),
                        ignored -> CompletableFuture.failedFuture(
                                new IllegalStateException("provider failed")
                        )
                ))
        );
        assertEquals(source.id(), outcome.leafId());
        assertEquals(source.id(), join(session.leafId()));
        assertEquals("provider failed", outcome.error().message());
        assertEquals(0, compactions(session).size());
        assertTrue(join(session.findOpenOperations("main", 2)).isEmpty());
    }

    @Test
    void abortDuringEffectDiscardsPreparedCheckpointButKeepsUsage() {
        AgentSession session = memory("abort");
        append(session, "first", "history");
        SessionEntry source = append(session, "source", "recent");
        CompletableFuture<CompactionSummarizer.Summary> effect = new CompletableFuture<>();
        var operation = SessionCompactionOperation.compact(
                session,
                new SessionCompactionOperation.Options(
                        SETTINGS, SessionRecordDraft.CompactionReason.MANUAL,
                        0, null
                ),
                ignored -> effect
        );
        String runId = join(session.findOpenOperations("main", 1)).getFirst().id();
        assertTrue(join(SessionCompactionOperation.requestAbort(session, runId)));
        assertEquals(false, join(SessionCompactionOperation.requestAbort(session, runId)));
        effect.complete(new CompactionSummarizer.Summary("discard", usage(4)));

        SessionCompactionOperation.Outcome.Aborted outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Aborted.class, join(operation)
        );
        assertEquals(source.id(), outcome.leafId());
        assertEquals(source.id(), join(session.leafId()));
        assertEquals(0, compactions(session).size());
        assertEquals(List.of(4L), join(session.usage()).stream()
                .map(row -> row.usage().totalTokens()).toList());
    }

    @Test
    void reopensUnknownEffectAsLaterAttempt() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession original = join(repository.create(
                new SessionRepository.CreateOptions("recover", null)
        ));
        append(original, "first", "history");
        SessionEntry source = append(original, "source", "recent");
        join(original.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", source.id(),
                new SessionRecordDraft.OperationIntent.Compaction(null, "result")
        )));
        join(original.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run", SessionRecordDraft.Step.COMPACTION,
                1, "result", SessionRecordDraft.CompactionReason.MANUAL
        )));
        join(original.appendRecord(new SessionRecordDraft.UsageRecord(
                "usage-1", "main", "compaction", usage(3),
                "run", null, 1, "error", null, null
        )));

        AgentSession reopened = join(new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        ).open(join(original.metadata())));
        AtomicInteger dispatches = new AtomicInteger();
        SessionCompactionOperation.Outcome.Completed outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Completed.class,
                join(SessionCompactionOperation.resume(
                        reopened,
                        new SessionCompactionOperation.RecoveryOptions(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL, 3
                        ),
                        ignored -> {
                            dispatches.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("recovered", usage(6))
                            );
                        }
                ))
        );
        assertEquals(1, dispatches.get());
        assertEquals("result", outcome.entry().id());
        assertEquals(List.of(1, 2), recordsOldest(reopened).stream()
                .map(SessionRecord::value)
                .filter(SessionRecordDraft.StepAttempt.class::isInstance)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
        assertEquals(List.of(3L, 6L), join(reopened.usage()).stream()
                .map(row -> row.usage().totalTokens()).toList());
        join(reopened.validateRecordLog("main"));
    }

    @Test
    void recoveryHonorsAbortAttemptCapAndDurableReason() {
        AgentSession aborted = crashPrefix("aborted", false);
        assertTrue(join(SessionCompactionOperation.requestAbort(aborted, "run")));
        AtomicInteger dispatches = new AtomicInteger();
        assertInstanceOf(SessionCompactionOperation.Outcome.Aborted.class,
                join(SessionCompactionOperation.resume(
                        aborted,
                        new SessionCompactionOperation.RecoveryOptions(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL, 3
                        ),
                        ignored -> {
                            dispatches.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("unused", Usage.ZERO)
                            );
                        }
                )));
        assertEquals(0, dispatches.get());

        AgentSession exhausted = crashPrefix("exhausted", true);
        SessionCompactionOperation.Outcome.Failed failed = assertInstanceOf(
                SessionCompactionOperation.Outcome.Failed.class,
                join(SessionCompactionOperation.resume(
                        exhausted,
                        new SessionCompactionOperation.RecoveryOptions(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL, 1
                        ),
                        ignored -> {
                            dispatches.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CompactionSummarizer.Summary("unused", Usage.ZERO)
                            );
                        }
                ))
        );
        assertTrue(failed.error().message().contains("exhausted at 1"));
        assertEquals(0, dispatches.get());

        AgentSession mismatch = crashPrefix("mismatch", true);
        CompletionException corruption = assertThrows(
                CompletionException.class,
                () -> join(SessionCompactionOperation.resume(
                        mismatch,
                        new SessionCompactionOperation.RecoveryOptions(
                                SETTINGS, SessionRecordDraft.CompactionReason.THRESHOLD, 3
                        ),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("unused", Usage.ZERO)
                        )
                ))
        );
        assertInstanceOf(RecordLogCorruption.class, corruption.getCause());
        assertEquals(1, join(mismatch.findOpenOperations("main", 2)).size());
    }

    private static AgentSession crashPrefix(String id, boolean attempt) {
        AgentSession session = memory(id);
        append(session, "first", "history");
        SessionEntry source = append(session, "source", "recent");
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", source.id(),
                new SessionRecordDraft.OperationIntent.Compaction(null, "result")
        )));
        if (attempt) {
            join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                    "attempt-1", "main", "run", SessionRecordDraft.Step.COMPACTION,
                    1, "result", SessionRecordDraft.CompactionReason.MANUAL
            )));
        }
        return session;
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

    private static List<SessionEntry> compactions(AgentSession session) {
        return join(session.findEntries(new SessionEntryQuery(
                SessionEntry.Type.COMPACTION, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null, null
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

    private static void assertCode(
            SessionError.Code code,
            java.util.concurrent.CompletionStage<?> stage
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class, () -> join(stage)
        );
        assertEquals(code, ((SessionError) failure.getCause()).code());
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
