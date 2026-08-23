package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlSessionRepositoryTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC
    );

    @TempDir
    Path temporary;

    @Test
    void persistsMutationsAndRestoresQueriesOpenOperationsAndStats() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "session");
        String entryId = join(session.appendCustom(
                "note", MAPPER.valueToTree(java.util.Map.of("value", 1))
        ));
        join(session.createLane("thread", entryId));
        join(session.appendRecord(operationStarted("run", "thread")));
        join(session.name("Example"));
        join(session.label(entryId, "checkpoint"));
        Usage usage = new Usage(
                10, 2, 3, 1, 13,
                new Cost(1, 2, 3, 4, 10)
        );
        join(session.appendRecord(new SessionRecordDraft.UsageRecord(
                "usage", "thread", "adjustment", usage,
                null, entryId, null, null, null,
                MAPPER.valueToTree(java.util.Map.of("source", "test"))
        )));
        join(session.moveLane(null));

        SessionMetadata metadata = join(session.metadata());
        Path path = onlySessionFile();
        List<JsonNode> lines = Files.readAllLines(path, StandardCharsets.UTF_8)
                .stream().map(JsonlSessionRepositoryTest::json).toList();
        assertEquals(
                List.of("header", "entry", "lane", "record", "fact", "fact", "record", "lane"),
                lines.stream().map(node -> node.path("kind").asText()).toList()
        );
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
                lines.subList(1, lines.size()).stream()
                        .map(node -> node.path("seq").asLong()).toList());
        assertEquals(4, lines.getFirst().path("version").asInt());

        AgentSession reopened = join(repository().open(metadata));
        assertNull(join(reopened.leafId()));
        assertEquals("Example", join(reopened.name()));
        assertEquals("checkpoint", join(reopened.label(entryId)));
        assertEquals(List.of("run"), join(reopened.findOpenOperations("thread", 2))
                .stream().map(SessionRecord::id).toList());
        assertEquals(List.of("usage", "run"), join(reopened.findRecords())
                .stream().map(SessionRecord::id).toList());
        assertEquals(new SessionStats(0, 3, 11, 13, 10), join(reopened.stats()));
        SessionRecord finish = join(reopened.appendRecord(
                new SessionRecordDraft.OperationFinished(
                        "finish", "thread", "run",
                        SessionRecordDraft.OperationOutcome.COMPLETED, null
                )
        ));
        assertEquals(8, finish.sequence());
        assertTrue(join(reopened.findOpenOperations("thread", 2)).isEmpty());

        AgentSession verified = join(repository().open(metadata));
        assertTrue(join(verified.findOpenOperations("thread", 2)).isEmpty());
        assertEquals(8, join(verified.log(0, null)).size());
    }

    @Test
    void roundTripsAllEntryMessageAndRecordPayloadFamilies() {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "payloads");
        join(session.append(new SessionEntryDraft.Message(
                "user", UserMessage.text("hello", 1L)
        )));
        AssistantMessage assistant = new AssistantMessage(
                List.of(new TextContent("answer")), "openai-responses", "openai", "gpt-5",
                new Usage(1, 2, 0, 0, 1, 3, Cost.ZERO), StopReason.TOOL_USE,
                null, 2L, "response", "tool_use"
        );
        join(session.append(new SessionEntryDraft.Message("assistant", assistant)));
        join(session.append(new SessionEntryDraft.Compaction(
                "compact", "summary", List.of(UserMessage.text("retained", 3L)),
                100, MAPPER.valueToTree(java.util.Map.of("kind", "manual")), Usage.ZERO
        )));
        join(session.append(new SessionEntryDraft.ModelChange("model", "openai", "gpt-5")));
        join(session.append(new SessionEntryDraft.ThinkingLevelChange("thinking", "high")));
        join(session.append(new SessionEntryDraft.ActiveToolsChange(
                "tools", List.of("read", "bash")
        )));
        join(session.appendRecord(new SessionRecordDraft.ToolStarted(
                "tool-start", "main", "run", "assistant", 0,
                "call", "read", MAPPER.valueToTree(java.util.Map.of("path", "README.md")),
                "tool-result", SessionRecordDraft.Replay.SAFE
        )));
        join(session.appendRecord(new SessionRecordDraft.QueueEnqueued(
                "queue", "main", SessionRecordDraft.Queue.NEXT_RUN, null,
                new SessionEntryDraft.Message("queued-message", UserMessage.text("next", 4L))
        )));

        AgentSession reopened = join(repository().open(join(session.metadata())));
        List<SessionEntry> entries = join(reopened.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        )));
        assertEquals(List.of("user", "assistant", "compact", "model", "thinking", "tools"),
                entries.stream().map(SessionEntry::id).toList());
        AssistantMessage restored = assertInstanceOf(
                AssistantMessage.class,
                ((SessionEntry.Message) entries.get(1)).message()
        );
        assertEquals(StopReason.TOOL_USE, restored.stopReason());
        assertEquals(1, restored.usage().reasoning());
        assertEquals("response", restored.responseId());
        assertEquals(List.of("queue", "tool-start"), join(reopened.findRecords())
                .stream().map(SessionRecord::id).toList());
    }

    @Test
    void repairsUnterminatedAndTornFinalLines() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "repair");
        join(session.appendCustom("note", MAPPER.valueToTree(java.util.Map.of("kept", true))));
        SessionMetadata metadata = join(session.metadata());
        Path path = onlySessionFile();

        String unterminated = Files.readString(path).stripTrailing();
        Files.writeString(path, unterminated, StandardOpenOption.TRUNCATE_EXISTING);
        join(repository().open(metadata));
        assertTrue(Files.readString(path).endsWith("\n"));

        String validPrefix = Files.readString(path);
        Files.writeString(path, "{\"kind\":\"entry\"", StandardOpenOption.APPEND);
        AgentSession recovered = join(repository().open(metadata));
        assertEquals(validPrefix, Files.readString(path));
        SessionEntry after = join(recovered.append(new SessionEntryDraft.Custom(
                "after", "note", null
        )));
        assertEquals(2, after.sequence());
    }

    @Test
    void rejectsCompleteOrInteriorCorruptionWithoutChangingTheFile() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "corrupt");
        join(session.append(new SessionEntryDraft.Custom("one", "note", null)));
        join(session.append(new SessionEntryDraft.Custom("two", "note", null)));
        SessionMetadata metadata = join(session.metadata());
        Path path = onlySessionFile();
        List<String> lines = Files.readAllLines(path);
        String corrupted = lines.get(0) + "\n" + lines.get(1)
                + "\nnot-json\n" + lines.get(2) + "\n";
        Files.writeString(path, corrupted);
        assertCode(SessionError.Code.INVALID_ENTRY, repository().open(metadata));
        assertEquals(corrupted, Files.readString(path));

        String completeInvalid = lines.get(0) + "\n"
                + "{\"kind\":\"unknown\",\"seq\":1}\n";
        Files.writeString(path, completeInvalid);
        assertCode(SessionError.Code.INVALID_ENTRY, repository().open(metadata));
        assertEquals(completeInvalid, Files.readString(path));
    }

    @Test
    void rejectsReplayInvariantViolations() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "invalid-replay");
        SessionMetadata metadata = join(session.metadata());
        Path path = onlySessionFile();
        String header = Files.readAllLines(path).getFirst();
        String orphan = "{\"kind\":\"entry\",\"id\":\"orphan\","
                + "\"seq\":1,\"timestamp\":1,\"type\":\"custom\","
                + "\"customType\":\"note\",\"parentId\":\"missing\"}";
        Files.writeString(path, header + "\n" + orphan + "\n");
        SessionError failure = assertCode(
                SessionError.Code.INVALID_ENTRY, repository().open(metadata)
        );
        assertTrue(failure.getMessage().contains("missing parent"));
    }

    @Test
    void atomicallyPublishesForksWithoutRecordsOrUsage() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "source");
        join(source.append(new SessionEntryDraft.Message(
                "one", UserMessage.text("one", 1L)
        )));
        join(source.append(new SessionEntryDraft.Message(
                "two", UserMessage.text("two", 2L)
        )));
        join(source.createLane("thread", "one"));
        join(source.name("Source"));
        join(source.label("one", "checkpoint"));
        join(source.appendRecord(operationStarted("run", "main")));
        join(source.appendRecord(new SessionRecordDraft.UsageRecord(
                "usage", "thread", "adjustment", Usage.ZERO,
                null, null, null, null, null, null
        )));

        AgentSession fork = join(repository.fork(
                join(source.metadata()),
                new SessionForkOptions(
                        SessionForkOptions.Scope.TREE, null, null, "fork", null
                )
        ));
        SessionMetadata forkMetadata = join(fork.metadata());
        assertEquals("source", forkMetadata.parentSessionId());
        assertEquals(List.of("one", "two"), join(fork.findEntries(
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
        )).stream().map(SessionEntry::id).toList());
        assertEquals(List.of("main", "thread"), join(fork.lanes()).stream()
                .map(AgentSession.LanePointer::lane).toList());
        assertTrue(join(fork.findRecords()).isEmpty());
        assertTrue(join(fork.usage()).isEmpty());
        assertEquals("Source", join(fork.name()));
        assertEquals("checkpoint", join(fork.label("one")));
        assertFalse(Files.exists(Path.of(findFile("fork") + ".tmp")));

        AgentSession reopened = join(repository().open(forkMetadata));
        assertEquals(2, join(reopened.stats()).messageCount());
    }

    @Test
    void abortReconciliationAcceptsOnlyAbortRecordsForTheClaimedRun() {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "abort-reconciliation");
        join(source.appendRecord(operationStarted("run", "main")));
        SessionMetadata metadata = join(source.metadata());
        AgentSession owner = join(repository.open(metadata));
        AgentSession aborter = join(repository.open(metadata));

        assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
        owner.rawState().reconcileOperationAbort("main", "run");
        assertTrue(owner.rawState().hasAbortRequest("main", "run"));
        assertEquals(join(aborter.log(0, null)), join(owner.log(0, null)));

        AgentSession mixedSource = create(repository, "mixed-reconciliation");
        join(mixedSource.appendRecord(operationStarted("mixed-run", "main")));
        SessionMetadata mixedMetadata = join(mixedSource.metadata());
        AgentSession staleOwner = join(repository.open(mixedMetadata));
        AgentSession externalWriter = join(repository.open(mixedMetadata));
        join(externalWriter.name("external mutation"));
        int staleLogSize = join(staleOwner.log(0, null)).size();

        SessionError stale = assertThrows(
                SessionError.class,
                () -> staleOwner.rawState().reconcileOperationAbort(
                        "main", "mixed-run"
                )
        );
        assertEquals(SessionError.Code.STORAGE, stale.code());
        assertTrue(stale.getMessage().contains("suffix is not an abort"));
        assertEquals(staleLogSize, join(staleOwner.log(0, null)).size());
    }

    @Test
    void abortSignalIsGenerationScopedAcrossDeleteAndRecreate() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession old = create(repository, "abort-generation");
        join(old.appendRecord(operationStarted("old-run", "main")));
        SessionMetadata oldMetadata = join(old.metadata());
        assertTrue(join(SessionRunOperation.requestAbort(old, "old-run")));
        JsonlSessionRepository.MaintenanceArtifact oldSignal =
                join(repository.inspectMaintenance()).artifacts().stream()
                        .filter(artifact -> artifact.kind()
                                == JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL)
                        .findFirst().orElseThrow();
        assertTrue(oldSignal.associatedDataPresent());

        join(repository.delete(oldMetadata));
        AgentSession replacement = create(repository, "abort-generation");
        join(replacement.appendRecord(operationStarted("old-run", "main")));
        assertFalse(replacement.rawState().hasAbortRequest("main", "old-run"));
        assertEquals(1, join(replacement.findOpenOperations("main", null)).size());
        JsonlSessionRepository.MaintenanceReport report =
                join(repository.inspectMaintenance());
        assertEquals(1, report.count(
                JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL
        ));
        JsonlSessionRepository.MaintenanceArtifact retained = report.artifacts().stream()
                .filter(artifact -> artifact.kind()
                        == JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL)
                .findFirst().orElseThrow();
        assertEquals(oldSignal.path(), retained.path());
        assertFalse(retained.associatedDataPresent());
        assertFalse(retained.target().equals(findFile("abort-generation")));
    }

    @Test
    void deleteAndSameMillisecondRecreateUsesANewGenerationPath() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession stale = create(repository, "reused");
        SessionMetadata oldMetadata = join(stale.metadata());
        Path oldPath = findFile("reused");
        join(repository.delete(oldMetadata));
        assertFalse(Files.exists(oldPath));
        assertTrue(Files.exists(Path.of(oldPath + ".lock")));

        AgentSession replacement = create(repository, "reused");
        SessionMetadata newMetadata = join(replacement.metadata());
        Path newPath = findFile("reused");
        assertEquals(oldMetadata.createdAt(), newMetadata.createdAt());
        assertFalse(oldPath.equals(newPath));
        assertTrue(Files.exists(Path.of(newPath + ".lock")));

        SessionError fenced = assertCode(
                SessionError.Code.STORAGE,
                stale.append(new SessionEntryDraft.Message(
                        "stale", UserMessage.text("stale", 1)
                ))
        );
        assertTrue(fenced.getMessage().contains("Failed to append session"));
        assertEquals(0, join(stale.log(0, null)).size());
        SessionEntry committed = join(replacement.append(
                new SessionEntryDraft.Message(
                        "replacement", UserMessage.text("replacement", 2)
                )
        ));
        assertEquals(1, committed.sequence());
        assertEquals(List.of("replacement"), join(repository.open(newMetadata))
                .findEntries().toCompletableFuture().join().stream()
                .map(SessionEntry::id).toList());
        assertEquals(List.of("reused"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());
    }

    @Test
    void listsSkipsMalformedDeletesAndValidatesIds() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "valid");
        SessionMetadata metadata = join(session.metadata());
        Thread.sleep(5);
        create(repository, "newer");
        assertEquals(List.of("newer", "valid"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());

        Path malformed = temporary.resolve("malformed.jsonl");
        Files.writeString(malformed, "not-json\n");
        assertEquals(2, join(repository.list()).size());
        assertCode(SessionError.Code.INVALID_PAYLOAD,
                repository.create(new SessionRepository.CreateOptions("../escape", null)));

        join(repository.delete(metadata));
        assertCode(SessionError.Code.NOT_FOUND, repository.open(metadata));
    }

    private JsonlSessionRepository repository() {
        return new JsonlSessionRepository(
                temporary, temporary.resolve("workspace"), CLOCK,
                timestamp -> "generated-id"
        );
    }

    private Path onlySessionFile() throws IOException {
        try (var paths = Files.walk(temporary)) {
            return paths.filter(path -> path.toString().endsWith(".jsonl"))
                    .findFirst().orElseThrow();
        }
    }

    private Path findFile(String id) throws IOException {
        try (var paths = Files.walk(temporary)) {
            return paths.filter(path -> path.getFileName().toString()
                            .endsWith("_" + id + ".jsonl"))
                    .findFirst().orElseThrow();
        }
    }

    private static AgentSession create(JsonlSessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionRecordDraft.OperationStarted operationStarted(
            String id,
            String lane
    ) {
        return new SessionRecordDraft.OperationStarted(
                id, lane, null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), null, null
                )
        );
    }

    private static JsonNode json(String value) {
        try {
            return MAPPER.readTree(value);
        } catch (IOException failure) {
            throw new AssertionError(failure);
        }
    }

    private static SessionError assertCode(
            SessionError.Code code,
            CompletionStage<?> stage
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> stage.toCompletableFuture().join()
        );
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(code, error.code());
        return error;
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
