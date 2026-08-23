package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlWriterLeaseTest {
    @TempDir
    Path temporary;

    @Test
    void concurrentRepositoriesCannotCreateTheSameSessionIdTwice() throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Object> first = executor.submit(() -> createAfterGate(
                    repository(), "same-id", ready, start
            ));
            Future<Object> second = executor.submit(() -> createAfterGate(
                    repository(), "same-id", ready, start
            ));
            ready.await();
            start.countDown();
            List<Object> results = List.of(first.get(), second.get());
            assertEquals(1, results.stream().filter(AgentSession.class::isInstance).count());
            SessionError duplicate = assertInstanceOf(
                    SessionError.class,
                    results.stream().filter(SessionError.class::isInstance)
                            .findFirst().orElseThrow()
            );
            assertEquals(SessionError.Code.ALREADY_EXISTS, duplicate.code());
        }
        assertEquals(List.of("same-id"), join(repository().list()).stream()
                .map(SessionMetadata::id).toList());
        try (var paths = Files.walk(temporary)) {
            assertEquals(1, paths.filter(path -> path.toString().endsWith(".jsonl")).count());
        }
    }

    @Test
    void staleOpenedWriterCannotAppendDuplicateSequence() throws Exception {
        JsonlSessionRepository firstRepository = repository();
        AgentSession first = create(firstRepository, "stale");
        SessionMetadata metadata = join(first.metadata());
        AgentSession stale = join(repository().open(metadata));

        SessionEntry committed = join(first.append(new SessionEntryDraft.Message(
                "first", UserMessage.text("first", 1)
        )));
        assertEquals(1, committed.sequence());
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(stale.append(new SessionEntryDraft.Message(
                        "stale", UserMessage.text("stale", 2)
                )))
        );
        assertStale(failure);
        assertEquals(0, join(stale.log(0, null)).size());

        AgentSession reopened = join(repository().open(metadata));
        assertEquals(List.of("first"), join(reopened.findEntries()).stream()
                .map(SessionEntry::id).toList());
        SessionEntry next = join(reopened.append(new SessionEntryDraft.Message(
                "next", UserMessage.text("next", 3)
        )));
        assertEquals(2, next.sequence());
        assertDiskSequence(List.of(1L, 2L));
    }

    @Test
    void concurrentIndependentWritersSerializeAndOneBecomesStale() throws Exception {
        AgentSession creator = create(repository(), "concurrent");
        SessionMetadata metadata = join(creator.metadata());
        AgentSession left = join(repository().open(metadata));
        AgentSession right = join(repository().open(metadata));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Object> leftResult = executor.submit(() -> appendAfterGate(
                    left, "left", ready, start
            ));
            Future<Object> rightResult = executor.submit(() -> appendAfterGate(
                    right, "right", ready, start
            ));
            ready.await();
            start.countDown();
            List<Object> results = List.of(leftResult.get(), rightResult.get());
            assertEquals(1, results.stream().filter(SessionEntry.class::isInstance).count());
            assertEquals(1, results.stream().filter(SessionError.class::isInstance).count());
            SessionError stale = (SessionError) results.stream()
                    .filter(SessionError.class::isInstance).findFirst().orElseThrow();
            assertEquals(SessionError.Code.STORAGE, stale.code());
            assertTrue(stale.getMessage().contains("Stale JSONL session writer"));
        }

        AgentSession reopened = join(repository().open(metadata));
        assertEquals(1, join(reopened.findEntries()).size());
        assertDiskSequence(List.of(1L));
    }

    @Test
    void transactionAndAppendCompeteAsWholeCommits() throws Exception {
        AgentSession creator = create(repository(), "batch-race");
        join(creator.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1)
        )));
        SessionMetadata metadata = join(creator.metadata());
        AgentSession transactionWriter = join(repository().open(metadata));
        AgentSession appendWriter = join(repository().open(metadata));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Object> transaction = executor.submit(() -> {
                ready.countDown();
                start.await();
                try {
                    return join(transactionWriter.transaction(scope -> {
                        scope.moveLane(null);
                        return scope.append(new SessionEntryDraft.Message(
                                "transaction", UserMessage.text("transaction", 2)
                        ));
                    }));
                } catch (CompletionException failure) {
                    return failure.getCause();
                }
            });
            Future<Object> append = executor.submit(() -> appendAfterGate(
                    appendWriter, "append", ready, start
            ));
            ready.await();
            start.countDown();
            List<Object> results = List.of(transaction.get(), append.get());
            assertEquals(1, results.stream().filter(SessionEntry.class::isInstance).count());
            assertEquals(1, results.stream().filter(SessionError.class::isInstance).count());
        }

        AgentSession reopened = join(repository().open(metadata));
        List<SessionLogItem> log = join(reopened.log(0, null));
        assertTrue(log.size() == 2 || log.size() == 3);
        assertEquals(java.util.stream.LongStream.rangeClosed(1, log.size()).boxed().toList(),
                log.stream().map(SessionLogItem::sequence).toList());
        List<String> ids = join(reopened.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        ))).stream().map(SessionEntry::id).toList();
        assertTrue(ids.equals(List.of("root", "append"))
                || ids.equals(List.of("root", "transaction")));
        assertDiskSequence(log.stream().map(SessionLogItem::sequence).toList());
        try (var paths = Files.walk(temporary)) {
            assertEquals(0, paths.filter(path -> path.getFileName().toString()
                    .endsWith(".txn.tmp")).count());
        }
    }

    @Test
    void tailSequenceReaderExpandsForLargeMutationLines() {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "large-tail");
        join(session.append(new SessionEntryDraft.Message(
                "large", UserMessage.text("x".repeat(20_000), 1)
        )));
        SessionEntry next = join(session.append(new SessionEntryDraft.Message(
                "next", UserMessage.text("next", 2)
        )));
        assertEquals(2, next.sequence());
    }

    @Test
    void writerDoesNotOverwriteExternallyCorruptedCompleteTail() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "corrupt-tail");
        Path file;
        try (var paths = Files.walk(temporary)) {
            file = paths.filter(path -> path.toString().endsWith(".jsonl"))
                    .findFirst().orElseThrow();
        }
        Files.writeString(
                file, "{\"kind\":\"not-a-mutation\"}\n",
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND
        );

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> join(session.append(new SessionEntryDraft.Message(
                        "rejected", UserMessage.text("rejected", 1)
                )))
        );
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(SessionError.Code.STORAGE, error.code());
        assertTrue(error.getMessage().contains("Cannot write corrupt JSONL session"));
        assertEquals(0, join(session.log(0, null)).size());
        assertTrue(Files.readString(file).endsWith(
                "{\"kind\":\"not-a-mutation\"}\n"
        ));
    }

    @Test
    void lockFilesDoNotAppearAsSessions() {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "listing");
        join(session.append(new SessionEntryDraft.Message(
                "entry", UserMessage.text("entry", 1)
        )));
        assertEquals(List.of("listing"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());
    }

    private Object createAfterGate(
            JsonlSessionRepository repository,
            String id,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        start.await();
        try {
            return join(repository.create(new SessionRepository.CreateOptions(id, null)));
        } catch (CompletionException failure) {
            return failure.getCause();
        }
    }

    private Object appendAfterGate(
            AgentSession session,
            String id,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        start.await();
        try {
            return join(session.append(new SessionEntryDraft.Message(
                    id, UserMessage.text(id, 1)
            )));
        } catch (CompletionException failure) {
            return failure.getCause();
        }
    }

    private void assertDiskSequence(List<Long> expected) throws Exception {
        Path file;
        try (var paths = Files.walk(temporary)) {
            file = paths.filter(path -> path.toString().endsWith(".jsonl"))
                    .findFirst().orElseThrow();
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        ArrayList<Long> sequence = new ArrayList<>();
        for (int index = 1; index < lines.size(); index++) {
            sequence.add(JsonlSessionCodec.decodeMutation(lines.get(index)).sequence());
        }
        assertEquals(expected, sequence);
    }

    private static void assertStale(CompletionException failure) {
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(SessionError.Code.STORAGE, error.code());
        assertTrue(error.getMessage().contains("Stale JSONL session writer"));
        assertTrue(error.getMessage().contains("reopen the session"));
    }

    private JsonlSessionRepository repository() {
        return new JsonlSessionRepository(temporary.resolve("sessions"), temporary);
    }

    private static AgentSession create(JsonlSessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
