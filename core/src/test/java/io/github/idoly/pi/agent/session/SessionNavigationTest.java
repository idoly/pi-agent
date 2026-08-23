package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.BranchSummarization;
import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionNavigationTest {
    @Test
    void atomicallyPublishesMoveSummaryLabelAndTerminalOutcome() {
        AgentSession session = session("summarized");
        SessionEntry target = append(session, "target", "target");
        SessionEntry oldLeaf = append(session, "old", "abandoned work");
        AtomicReference<CompactionSummarizer.Request> request = new AtomicReference<>();
        CompactionSummarizer summarizer = value -> {
            request.set(value);
            return CompletableFuture.completedFuture(new CompactionSummarizer.Summary(
                    "Generated summary", usage(7)
            ));
        };

        SessionNavigation.Outcome.Completed result = assertInstanceOf(
                SessionNavigation.Outcome.Completed.class,
                join(SessionNavigation.navigate(
                        session, target.id(),
                        new SessionNavigation.Options(
                                true, "focus", "return point", 1000, 400
                        ),
                        summarizer
                ))
        );
        SessionEntry.BranchSummary summary = result.summaryEntry();
        assertEquals(target.id(), summary.parentId());
        assertEquals(oldLeaf.id(), summary.fromId());
        assertTrue(summary.summary().startsWith(BranchSummarization.PREAMBLE));
        assertTrue(summary.summary().contains("Generated summary"));
        assertEquals(usage(7), summary.usage());
        assertEquals(summary.id(), result.leafId());
        assertEquals(summary.id(), join(session.leafId()));
        assertEquals("return point", join(session.label(summary.id())));
        assertEquals(CompactionSummarizer.Kind.BRANCH, request.get().kind());
        assertEquals("focus", request.get().customInstructions());
        assertEquals(400, request.get().maxTokens());
        assertEquals(1, request.get().messages().size());
        assertEquals(0, join(session.findOpenOperations("main", 2)).size());
        join(session.validateRecordLog("main"));

        List<SessionRecord> records = recordsOldest(session);
        assertEquals(List.of(
                SessionRecordDraft.Type.OPERATION_STARTED,
                SessionRecordDraft.Type.STEP_ATTEMPT,
                SessionRecordDraft.Type.USAGE,
                SessionRecordDraft.Type.OPERATION_FINISHED
        ), records.stream().map(SessionRecord::type).toList());
        SessionRecordDraft.OperationFinished finished =
                (SessionRecordDraft.OperationFinished) records.getLast().value();
        assertEquals(SessionRecordDraft.OperationOutcome.COMPLETED, finished.outcome());
        SessionRecordDraft.OperationStarted started =
                (SessionRecordDraft.OperationStarted) records.getFirst().value();
        SessionRecordDraft.OperationIntent.Navigation intent =
                (SessionRecordDraft.OperationIntent.Navigation) started.intent();
        assertEquals(summary.id(), intent.summaryEntryId());
    }

    @Test
    void summaryFailureTerminatesWithoutMovingTheLane() {
        AgentSession session = session("failed");
        SessionEntry target = append(session, "target", "target");
        SessionEntry source = append(session, "source", "source");
        CompactionSummarizer summarizer = ignored ->
                CompletableFuture.failedFuture(new IllegalStateException("provider failed"));

        SessionNavigation.Outcome.Failed result = assertInstanceOf(
                SessionNavigation.Outcome.Failed.class,
                join(SessionNavigation.navigate(
                        session, target.id(),
                        new SessionNavigation.Options(true, null, null, 1000, 100),
                        summarizer
                ))
        );
        assertEquals(source.id(), result.leafId());
        assertEquals("provider failed", result.error().message());
        assertEquals(source.id(), join(session.leafId()));
        assertEquals(0, join(session.findEntries(new SessionEntryQuery(
                SessionEntry.Type.BRANCH_SUMMARY, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null, null
        ))).size());
        assertEquals(0, join(session.findOpenOperations("main", 2)).size());
        SessionRecordDraft.OperationFinished finished =
                (SessionRecordDraft.OperationFinished) recordsOldest(session).getLast().value();
        assertEquals(SessionRecordDraft.OperationOutcome.FAILED, finished.outcome());
    }

    @Test
    void abortDuringSummaryWinsAndPreparedSummaryIsDiscarded() {
        AgentSession session = session("aborted");
        SessionEntry target = append(session, "target", "target");
        SessionEntry source = append(session, "source", "source");
        CompletableFuture<CompactionSummarizer.Summary> effect = new CompletableFuture<>();
        CompletionStageHolder navigation = new CompletionStageHolder(
                SessionNavigation.navigate(
                        session, target.id(),
                        new SessionNavigation.Options(true, null, "ignored", 1000, 100),
                        ignored -> effect
                )
        );
        SessionRecord operation = join(session.findOpenOperations("main", 1)).getFirst();
        join(session.appendRecord(new SessionRecordDraft.AbortRequested(
                "abort", "main", operation.id()
        )));
        effect.complete(new CompactionSummarizer.Summary("discard me", usage(3)));

        SessionNavigation.Outcome.Aborted result = assertInstanceOf(
                SessionNavigation.Outcome.Aborted.class, join(navigation.stage)
        );
        assertEquals(source.id(), result.leafId());
        assertEquals(source.id(), join(session.leafId()));
        assertNull(join(session.label(source.id())));
        assertEquals(0, join(session.findEntries(new SessionEntryQuery(
                SessionEntry.Type.BRANCH_SUMMARY, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null, null
        ))).size());
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED,
                ((SessionRecordDraft.OperationFinished)
                        recordsOldest(session).getLast().value()).outcome());
    }

    @Test
    void unsummarizedNavigationCanMoveToRoot() {
        AgentSession session = session("root");
        append(session, "entry", "entry");
        SessionNavigation.Outcome.Completed result = assertInstanceOf(
                SessionNavigation.Outcome.Completed.class,
                join(SessionNavigation.navigate(
                        session, null, SessionNavigation.Options.DEFAULT, null
                ))
        );
        assertNull(result.leafId());
        assertNull(result.summaryEntry());
        assertNull(join(session.leafId()));
    }

    @Test
    void invalidTargetsAreRejectedBeforeAnyRecordIsWritten() {
        AgentSession session = session("reject");
        SessionEntry leaf = append(session, "leaf", "leaf");
        long before = join(session.log(0, null)).size();
        assertCode(SessionError.Code.INVALID_PAYLOAD, SessionNavigation.navigate(
                session, leaf.id(), SessionNavigation.Options.DEFAULT, null
        ));
        assertCode(SessionError.Code.NOT_FOUND, SessionNavigation.navigate(
                session, "missing", SessionNavigation.Options.DEFAULT, null
        ));
        assertCode(SessionError.Code.INVALID_PAYLOAD, SessionNavigation.navigate(
                session, null,
                new SessionNavigation.Options(true, null, null, 1, 1),
                ignored -> CompletableFuture.completedFuture(
                        new CompactionSummarizer.Summary("unused", Usage.ZERO)
                )
        ));
        assertCode(SessionError.Code.INVALID_PAYLOAD, SessionNavigation.navigate(
                session, null,
                new SessionNavigation.Options(false, null, "root", 0, 0), null
        ));
        assertEquals(before, join(session.log(0, null)).size());
        assertEquals(0, join(session.findRecords()).size());
    }

    private static List<SessionRecord> recordsOldest(AgentSession session) {
        return join(session.findRecords(new SessionRecordQuery(
                null, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )));
    }

    private static AgentSession session(String id) {
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

    private static final class CompletionStageHolder {
        private final java.util.concurrent.CompletionStage<SessionNavigation.Outcome> stage;

        private CompletionStageHolder(
                java.util.concurrent.CompletionStage<SessionNavigation.Outcome> stage
        ) {
            this.stage = stage;
        }
    }
}
