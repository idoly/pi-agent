package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionTransactionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void commitsAContiguousBatchAndRetainsV4ReplayCompatibility() throws Exception {
        Path root = temporaryDirectory.resolve("sessions");
        JsonlSessionRepository repository = new JsonlSessionRepository(root, temporaryDirectory);
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("transaction", null)
        ));
        SessionEntry rootEntry = join(session.append(
                new SessionEntryDraft.Message("root", UserMessage.text("root", 1))
        ));
        SessionRecord started = join(session.appendRecord(start("navigation", rootEntry.id())));

        SessionEntry.BranchSummary summary = join(session.transaction(transaction -> {
            transaction.moveLane(null);
            SessionEntry.BranchSummary value = (SessionEntry.BranchSummary) transaction.append(
                    new SessionEntryDraft.BranchSummary(
                            "summary", rootEntry.id(), "summary", null, null
                    )
            );
            transaction.label(value.id(), "return");
            transaction.appendRecord(new SessionRecordDraft.OperationFinished(
                    "finished", "main", started.id(),
                    SessionRecordDraft.OperationOutcome.COMPLETED, null
            ));
            return value;
        }));

        assertEquals("summary", join(session.leafId()));
        assertEquals("return", join(session.label("summary")));
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L),
                join(session.log(0, null)).stream().map(SessionLogItem::sequence).toList());
        assertEquals(0, join(session.findOpenOperations("main", 2)).size());

        SessionMetadata metadata = join(session.metadata());
        AgentSession reopened = join(repository.open(metadata));
        assertEquals(summary, join(reopened.entry("summary")));
        assertEquals("summary", join(reopened.leafId()));
        assertEquals("return", join(reopened.label("summary")));
        assertEquals(0, join(reopened.findOpenOperations("main", 2)).size());

        Path file;
        try (var paths = Files.walk(root)) {
            file = paths.filter(path -> path.toString().endsWith(".jsonl"))
                    .findFirst().orElseThrow();
        }
        List<String> lines = Files.readAllLines(file);
        assertEquals(7, lines.size());
        for (int index = 1; index < lines.size(); index++) {
            assertEquals(index, JsonlSessionCodec.decodeMutation(lines.get(index)).sequence());
        }
    }

    @Test
    void rollsBackEveryIndexWhenCallbackFailsAndDoesNotConsumeSequence() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("rollback", null)
        ));
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1)
        )));
        CompletionException failure = assertThrows(CompletionException.class, () ->
                join(session.transaction(transaction -> {
                    transaction.moveLane(null);
                    transaction.append(new SessionEntryDraft.Message(
                            "discarded", UserMessage.text("discarded", 2)
                    ));
                    transaction.name("discarded-name");
                    throw new IllegalStateException("stop");
                }))
        );
        assertEquals("stop", failure.getCause().getMessage());
        assertEquals("root", join(session.leafId()));
        assertNull(join(session.name()));
        assertNull(join(session.entry("discarded")));
        assertEquals(1, join(session.log(0, null)).size());

        SessionEntry reused = join(session.append(new SessionEntryDraft.Message(
                "discarded", UserMessage.text("committed", 3)
        )));
        assertEquals(2, reused.sequence());
    }

    @Test
    void rollsBackWhenAtomicPersistenceRejectsTheBatch() {
        ArrayList<SessionLogItem> persisted = new ArrayList<>();
        InMemorySessionState.PersistenceSink sink = new InMemorySessionState.PersistenceSink() {
            @Override
            public void persist(SessionLogItem mutation) {
                persisted.add(mutation);
            }

            @Override
            public void persistBatch(List<SessionLogItem> mutations) {
                throw new SessionError(SessionError.Code.STORAGE, "batch failed");
            }
        };
        InMemorySessionState state = new InMemorySessionState(
                new SessionMetadata("failure", 1, 4, null), Clock.systemUTC(), sink
        );
        AgentSession session = new AgentSession(state, ignored -> "generated", "main");
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1)
        )));

        CompletionException failure = assertThrows(CompletionException.class, () ->
                join(session.transaction(transaction -> {
                    transaction.moveLane(null);
                    return transaction.appendMessage(UserMessage.text("new", 2));
                }))
        );
        assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(SessionError.Code.STORAGE, ((SessionError) failure.getCause()).code());
        assertEquals("root", join(session.leafId()));
        assertEquals(1, join(session.log(0, null)).size());
        assertEquals(1, persisted.size());
    }

    @Test
    void rejectsMultiMutationTransactionsBeforeUsingANonAtomicSink() {
        ArrayList<SessionLogItem> persisted = new ArrayList<>();
        InMemorySessionState state = new InMemorySessionState(
                new SessionMetadata("unsupported", 1, 4, null),
                Clock.systemUTC(), persisted::add
        );
        AgentSession session = new AgentSession(
                state, ignored -> "generated", "main"
        );
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1)
        )));

        CompletionException failure = assertThrows(CompletionException.class, () ->
                join(session.transaction(transaction -> {
                    transaction.moveLane(null);
                    return transaction.appendMessage(UserMessage.text("new", 2));
                }))
        );
        assertEquals(SessionError.Code.STORAGE,
                ((SessionError) failure.getCause()).code());
        assertEquals(1, persisted.size());
        assertEquals("root", join(session.leafId()));
        assertEquals(1, join(session.log(0, null)).size());
    }

    @Test
    void transactionViewExpiresAtCallbackReturnAndNestedTransactionsFail() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("lifetime", null)
        ));
        AtomicReference<SessionTransaction> escaped = new AtomicReference<>();
        join(session.transaction(transaction -> {
            escaped.set(transaction);
            CompletionException nested = assertThrows(
                    CompletionException.class,
                    () -> join(session.transaction(ignored -> null))
            );
            assertInstanceOf(IllegalStateException.class, nested.getCause());
            return null;
        }));
        assertThrows(IllegalStateException.class, escaped.get()::leafId);
    }

    private static SessionRecordDraft.OperationStarted start(String id, String source) {
        return new SessionRecordDraft.OperationStarted(
                id, "main", source,
                new SessionRecordDraft.OperationIntent.Navigation(
                        null, true, null, null, "summary"
                )
        );
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
