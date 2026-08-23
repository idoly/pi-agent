package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionRetainedCopyTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path temporary;

    @Test
    void inMemoryCopyRetainsExactReplayableMutationSet() {
        assertRetainedCopy(new InMemorySessionRepository(), "memory-copy");
    }

    @Test
    void jsonlCopyRetainsExactReplayableMutationSetAndPublishesCanonicalFile()
            throws Exception {
        JsonlSessionRepository repository = jsonlRepository();
        AgentSession source = populated(repository, "jsonl-source");
        SessionMetadata sourceMetadata = join(source.metadata());
        Path sourcePath = findFile("jsonl-source");
        String sourceContent = Files.readString(sourcePath);

        AgentSession copied = join(repository.copyRetained(
                sourceMetadata, options("jsonl-copy")
        ));
        assertProjection(copied, sourceMetadata.id());
        assertEquals(sourceContent, Files.readString(sourcePath));
        Path copyPath = findFile("jsonl-copy");
        assertFalse(Files.exists(Path.of(copyPath + ".tmp")));
        assertEquals(10, Files.readAllLines(copyPath).size());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
                join(copied.log(0, null)).stream()
                        .map(SessionLogItem::sequence).toList());

        AgentSession reopened = join(repository.open(join(copied.metadata())));
        assertProjection(reopened, sourceMetadata.id());
    }

    @Test
    void rejectsNonReplayableSetsAndNeverPublishesDestination() throws Exception {
        JsonlSessionRepository repository = jsonlRepository();
        AgentSession source = populated(repository, "invalid-source");
        SessionMetadata metadata = join(source.metadata());

        assertCode(SessionError.Code.NOT_FOUND, repository.copyRetained(
                metadata,
                new SessionRetainedCopyOptions(Set.of(3L), "missing-entry", null)
        ));
        assertFalse(fileExists("missing-entry"));

        assertInstanceOf(RecordLogCorruption.class, failure(repository.copyRetained(
                metadata,
                new SessionRetainedCopyOptions(
                        Set.of(1L, 3L, 4L, 9L), "missing-acceptance", null
                )
        )));
        assertFalse(fileExists("missing-acceptance"));

        assertCode(SessionError.Code.INVALID_QUERY, repository.copyRetained(
                metadata,
                new SessionRetainedCopyOptions(Set.of(1L, 999L), "unknown-sequence", null)
        ));
        assertFalse(fileExists("unknown-sequence"));
        assertEquals(List.of("invalid-source"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());
    }

    @Test
    void crashPrefixCanPreserveAndSettleSuspendedOperationIndependently() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession source = populated(repository, "prefix-source");
        AgentSession prefix = join(repository.copyRetained(
                join(source.metadata()),
                new SessionRetainedCopyOptions(
                        Set.of(1L, 3L, 4L, 5L, 6L, 7L, 8L),
                        "prefix-copy", null
                )
        ));
        AgentSession thread = prefix.view("thread");
        SessionOperationInspector.OpenOperation suspended = join(
                SessionOperationInspector.inspectLane(thread)
        );
        assertEquals(SessionOperationInspector.Status.SUSPENDED, suspended.status());
        assertEquals("run", suspended.id());
        assertEquals(List.of("finish", "usage", "run"),
                join(source.findRecords()).stream().map(SessionRecord::id).toList());
        assertEquals(List.of("usage", "run"),
                join(thread.findRecords()).stream().map(SessionRecord::id).toList());

        join(thread.appendRecord(new SessionRecordDraft.OperationFinished(
                "copy-finish", "thread", "run",
                SessionRecordDraft.OperationOutcome.COMPLETED, null
        )));
        assertTrue(join(thread.findOpenOperations("thread", null)).isEmpty());
        assertEquals("copy-finish", join(thread.findRecords()).getFirst().id());
        assertEquals("thread-entry", join(thread.lastResult()).leafId());
        assertEquals("finish", join(source.findRecords()).getFirst().id());
    }

    @Test
    void concurrentJsonlPublishersCreateExactlyOneDestination() throws Exception {
        JsonlSessionRepository first = jsonlRepository();
        AgentSession source = populated(first, "race-source");
        SessionMetadata metadata = join(source.metadata());
        JsonlSessionRepository second = jsonlRepository();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var left = executor.submit(() -> copyAfterGate(
                    first, metadata, "race-copy", ready, start
            ));
            var right = executor.submit(() -> copyAfterGate(
                    second, metadata, "race-copy", ready, start
            ));
            ready.await();
            start.countDown();
            List<Object> results = List.of(left.get(), right.get());
            assertEquals(1, results.stream().filter(AgentSession.class::isInstance).count());
            SessionError duplicate = assertInstanceOf(
                    SessionError.class,
                    results.stream().filter(SessionError.class::isInstance)
                            .findFirst().orElseThrow()
            );
            assertEquals(SessionError.Code.ALREADY_EXISTS, duplicate.code());
        }
        assertEquals(List.of("race-copy", "race-source"), join(first.list()).stream()
                .map(SessionMetadata::id).sorted().toList());
        AgentSession published = join(first.open(join(first.list()).stream()
                .filter(value -> value.id().equals("race-copy"))
                .findFirst().orElseThrow()));
        assertProjection(published, "race-source");
        assertFalse(Files.exists(Path.of(findFile("race-copy") + ".tmp")));
    }

    @Test
    void destinationConflictDoesNotChangeExistingSession() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession source = populated(repository, "source");
        AgentSession existing = join(repository.create(
                new SessionRepository.CreateOptions("existing", null)
        ));
        join(existing.append(new SessionEntryDraft.Message(
                "existing-entry", UserMessage.text("existing", 20)
        )));

        assertCode(SessionError.Code.ALREADY_EXISTS, repository.copyRetained(
                join(source.metadata()), options("existing")
        ));
        assertEquals(List.of("existing-entry"), join(existing.findEntries()).stream()
                .map(SessionEntry::id).toList());
    }

    @Test
    void optionsRejectNonPositiveSequences() {
        assertThrows(IllegalArgumentException.class, () ->
                new SessionRetainedCopyOptions(Set.of(0L), "copy", null));
    }

    private void assertRetainedCopy(SessionRepository repository, String destination) {
        AgentSession source = populated(repository, "memory-source");
        SessionMetadata sourceMetadata = join(source.metadata());
        AgentSession copy = join(repository.copyRetained(
                sourceMetadata, options(destination)
        ));
        assertProjection(copy, sourceMetadata.id());
        assertEquals(10, join(source.log(0, null)).size());
        assertEquals(List.of("second", "root"), join(source.findEntries()).stream()
                .map(SessionEntry::id).toList().subList(1, 3));
    }

    private static SessionRetainedCopyOptions options(String id) {
        return new SessionRetainedCopyOptions(
                new LinkedHashSet<>(List.of(1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
                id, null
        );
    }

    private static void assertProjection(AgentSession copy, String parentId) {
        SessionMetadata metadata = join(copy.metadata());
        assertEquals(parentId, metadata.parentSessionId());
        List<SessionEntry> entries = join(copy.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        )));
        assertEquals(List.of("root", "thread-entry"), entries.stream()
                .map(SessionEntry::id).toList());
        assertEquals(List.of(1L, 3L), entries.stream()
                .map(SessionEntry::sequence).toList());
        assertEquals("Copied source", join(copy.name()));
        assertEquals("keep", join(copy.label("root")));
        assertEquals(List.of(
                new AgentSession.LanePointer("main", "root"),
                new AgentSession.LanePointer("thread", "thread-entry")
        ), join(copy.lanes()));
        assertEquals(List.of("finish", "usage", "run"),
                join(copy.findRecords()).stream().map(SessionRecord::id).toList());
        assertEquals(List.of(6L, 7L, 8L), join(copy.findRecords(
                new SessionRecordQuery(
                        null, null, null, null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        )).stream().map(SessionRecord::sequence).toList());
        assertEquals(1, join(copy.usage()).size());
        assertTrue(join(copy.findOpenOperations("thread", null)).isEmpty());
        AgentSession thread = copy.view("thread");
        assertEquals("run", join(thread.lastResult()).runId());
        assertEquals("thread-entry", join(thread.lastResult()).leafId());
        assertNull(join(copy.entry("second")));
    }

    private static AgentSession populated(SessionRepository repository, String id) {
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions(id, null)
        ));
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1)
        )));
        join(session.append(new SessionEntryDraft.Message(
                "second", UserMessage.text("second", 2)
        )));
        join(session.createLane("thread", "root"));
        AgentSession thread = session.view("thread");
        join(thread.append(new SessionEntryDraft.Message(
                "thread-entry", UserMessage.text("thread", 3)
        )));
        join(session.name("Copied source"));
        join(session.label("root", "keep"));
        join(thread.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "thread", null,
                new SessionRecordDraft.OperationIntent.Run(List.of(), List.of(), null, null)
        )));
        join(thread.appendRecord(new SessionRecordDraft.UsageRecord(
                "usage", "thread", "assistant", Usage.ZERO,
                "run", "thread-entry", null, null, null,
                MAPPER.valueToTree(java.util.Map.of("kept", true))
        )));
        join(thread.appendRecord(new SessionRecordDraft.OperationFinished(
                "finish", "thread", "run",
                SessionRecordDraft.OperationOutcome.COMPLETED, null
        )));
        join(session.moveLane("root"));
        return session;
    }

    private static Object copyAfterGate(
            JsonlSessionRepository repository,
            SessionMetadata source,
            String destination,
            CountDownLatch ready,
            CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        start.await();
        try {
            return join(repository.copyRetained(source, options(destination)));
        } catch (CompletionException failure) {
            return failure.getCause();
        }
    }

    private JsonlSessionRepository jsonlRepository() {
        return new JsonlSessionRepository(temporary.resolve("sessions"), temporary);
    }

    private Path findFile(String id) throws Exception {
        try (var paths = Files.walk(temporary)) {
            return paths.filter(path -> path.getFileName().toString()
                            .endsWith("_" + id + ".jsonl"))
                    .findFirst().orElseThrow();
        }
    }

    private boolean fileExists(String id) throws Exception {
        try (var paths = Files.walk(temporary)) {
            return paths.anyMatch(path -> path.getFileName().toString()
                    .endsWith("_" + id + ".jsonl"));
        }
    }

    private static Throwable failure(CompletionStage<?> stage) {
        CompletionException failure = assertThrows(
                CompletionException.class, () -> stage.toCompletableFuture().join()
        );
        return failure.getCause();
    }

    private static SessionError assertCode(
            SessionError.Code code,
            CompletionStage<?> stage
    ) {
        SessionError error = assertInstanceOf(SessionError.class, failure(stage));
        assertEquals(code, error.code());
        return error;
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
