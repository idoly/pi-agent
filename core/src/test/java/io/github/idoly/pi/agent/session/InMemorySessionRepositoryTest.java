package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemorySessionRepositoryTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC
    );

    @Test
    void assignsParentsAndOneSequenceAcrossEntriesLanesAndFacts() {
        InMemorySessionRepository repo = repository();
        AgentSession session = create(repo, "session");
        SessionEntry root = join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1L)
        )));
        join(session.createLane("thread", root.id()));
        AgentSession thread = session.view("thread");
        SessionEntry child = join(thread.append(new SessionEntryDraft.Custom(
                "child", "note", MAPPER.valueToTree(java.util.Map.of("value", 1))
        )));
        join(session.name("Example"));
        join(session.label(root.id(), "checkpoint"));
        join(session.moveLane(child.id()));

        assertEquals(1, root.sequence());
        assertNull(root.parentId());
        assertEquals(3, child.sequence());
        assertEquals("root", child.parentId());
        assertEquals(List.of(
                new AgentSession.LanePointer("main", "child"),
                new AgentSession.LanePointer("thread", "child")
        ), join(session.lanes()));
        assertEquals(
                List.of(1L, 2L, 3L, 4L, 5L, 6L),
                join(session.log(0, null)).stream().map(SessionLogItem::sequence).toList()
        );
    }

    @Test
    void supportsDivergentLanesBoundsFiltersAndCursors() {
        AgentSession session = create(repository(), "session");
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1L)
        )));
        join(session.createLane("thread", "root"));
        join(session.append(new SessionEntryDraft.Custom(
                "old-note", "note", MAPPER.valueToTree(1)
        )));
        join(session.append(new SessionEntryDraft.Compaction(
                "compact", "summary", List.of(), 10, null, null
        )));
        join(session.append(new SessionEntryDraft.Custom(
                "new-note", "note", MAPPER.valueToTree(2)
        )));
        join(session.append(new SessionEntryDraft.Message(
                "main-tail", assistant("main")
        )));
        AgentSession thread = session.view("thread");
        join(thread.append(new SessionEntryDraft.Message(
                "thread-tail", UserMessage.text("thread", 2L)
        )));

        assertEquals(
                List.of("root", "old-note", "compact", "new-note", "main-tail"),
                ids(join(session.findEntriesOnBranch(new SessionBranchQuery(
                        null, null, null,
                        new SessionEntryQuery(
                                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                        )
                ))))
        );
        assertEquals(
                List.of("root", "thread-tail"),
                ids(join(thread.findEntriesOnBranch(new SessionBranchQuery(
                        null, null, null,
                        new SessionEntryQuery(
                                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                        )
                ))))
        );
        assertEquals(
                List.of("new-note", "old-note"),
                ids(join(session.findEntries(new SessionEntryQuery(
                        null, "note", null, null, null
                ))))
        );
        assertEquals(
                List.of("compact", "new-note"),
                ids(join(session.findEntries(new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, 2, 3L
                ))))
        );
        assertEquals(
                List.of("main-tail"),
                ids(join(session.findEntriesOnBranch(new SessionBranchQuery(
                        null, null, SessionEntry.Type.COMPACTION,
                        new SessionEntryQuery(
                                SessionEntry.Type.MESSAGE, null, null, null, null
                        )
                ))))
        );
    }

    @Test
    void returnsDetachedCustomPayloadsAndTracksLedgerStats() {
        AgentSession session = create(repository(), "session");
        ObjectNode original = MAPPER.createObjectNode();
        original.putObject("nested").put("value", 1);
        join(session.append(new SessionEntryDraft.Custom("custom", "note", original)));
        ((ObjectNode) original.path("nested")).put("value", 50);

        SessionEntry.Custom first = assertInstanceOf(
                SessionEntry.Custom.class, join(session.entry("custom"))
        );
        ((ObjectNode) first.data().path("nested")).put("value", 99);
        SessionEntry.Custom second = assertInstanceOf(
                SessionEntry.Custom.class, join(session.entry("custom"))
        );
        assertEquals(1, second.data().path("nested").path("value").asInt());

        join(session.append(new SessionEntryDraft.Message(
                "message", UserMessage.text("question", 1L)
        )));
        Usage provider = new Usage(
                10, 5, 3, 2, 20,
                new Cost(1, 2, 3, 4, 10)
        );
        join(session.recordUsage(provider, "message", false, null));
        Usage correction = new Usage(
                -2, 0, 0, 0, -2,
                new Cost(-0.5, 0, 0, 0, -0.5)
        );
        join(session.recordUsage(correction, null, true, MAPPER.valueToTree(
                java.util.Map.of("reason", "correction")
        )));

        assertEquals(new SessionStats(1, 3, 10, 18, 9.5), join(session.stats()));
        assertEquals(2, join(session.usage()).size());
    }

    @Test
    void forksBranchesAndTreesWithoutCopyingUsage() {
        InMemorySessionRepository repo = repository();
        AgentSession source = create(repo, "source");
        String root = join(source.appendMessage(UserMessage.text("root", 1L)));
        String shared = join(source.appendMessage(assistant("shared")));
        join(source.createLane("thread", shared));
        String threadChild = join(source.view("thread").appendMessage(
                UserMessage.text("thread", 2L)
        ));
        String mainChild = join(source.appendMessage(UserMessage.text("main", 3L)));
        join(source.name("Source"));
        join(source.label(shared, "copied"));
        join(source.label(threadChild, "excluded"));
        join(source.recordUsage(Usage.ZERO, shared, false, null));
        SessionMetadata metadata = join(source.metadata());

        AgentSession branch = join(repo.fork(metadata, new SessionForkOptions(
                SessionForkOptions.Scope.BRANCH, mainChild,
                SessionForkOptions.Position.AT, "branch", null
        )));
        assertEquals(List.of(root, shared, mainChild), ids(join(branch.findEntries(
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
        ))));
        assertEquals("Source", join(branch.name()));
        assertEquals("copied", join(branch.label(shared)));
        assertNull(join(branch.label(threadChild)));
        assertEquals(0, join(branch.usage()).size());
        assertEquals(3, join(branch.stats()).messageCount());
        assertEquals("source", join(branch.metadata()).parentSessionId());

        AgentSession tree = join(repo.fork(metadata, new SessionForkOptions(
                SessionForkOptions.Scope.TREE, null, null, "tree", null
        )));
        assertEquals(4, join(tree.findEntries()).size());
        assertEquals(List.of(
                new AgentSession.LanePointer("main", mainChild),
                new AgentSession.LanePointer("thread", threadChild)
        ), join(tree.lanes()));
    }

    @Test
    void buildsCompactionAwareContextAndDerivesConfigurationFromFullPath() {
        AgentSession session = create(repository(), "session");
        join(session.append(new SessionEntryDraft.Message(
                "old", UserMessage.text("old", 1L)
        )));
        join(session.append(new SessionEntryDraft.Compaction(
                "compact", "summary",
                List.of(UserMessage.text("retained", 2L), assistant("answer")),
                100, null, null
        )));
        join(session.append(new SessionEntryDraft.ModelChange(
                "model", "openai", "gpt-5"
        )));
        join(session.append(new SessionEntryDraft.ThinkingLevelChange(
                "thinking", "high"
        )));
        join(session.append(new SessionEntryDraft.ActiveToolsChange(
                "tools", List.of("read", "bash")
        )));
        join(session.append(new SessionEntryDraft.Message(
                "tail", UserMessage.text("tail", 3L)
        )));

        SessionContext context = join(session.context());
        assertEquals(
                List.of("compaction", "user", "assistant", "user"),
                context.messages().stream().map(message -> {
                    if (message instanceof CompactionSummaryMessage) return "compaction";
                    if (message instanceof UserMessage) return "user";
                    return "assistant";
                }).toList()
        );
        assertEquals(new SessionContext.ModelRef("openai", "gpt-5"), context.model());
        assertEquals("high", context.thinkingLevel());
        assertEquals(List.of("read", "bash"), context.activeToolNames());
        List<io.github.idoly.pi.ai.Message> providerMessages = join(
                SessionContextConverters.standardMessages().convert(context.messages())
        );
        UserMessage summary = assertInstanceOf(UserMessage.class, providerMessages.getFirst());
        assertEquals(
                SessionContextConverters.COMPACTION_SUMMARY_PREFIX + "summary"
                        + SessionContextConverters.COMPACTION_SUMMARY_SUFFIX,
                ((TextContent) summary.content().getFirst()).text()
        );
    }

    @Test
    void storesAndQueriesOperationRecordsAndOpenOperations() {
        AgentSession session = create(repository(), "session");
        join(session.createLane("thread", null));
        SessionRecordDraft.OperationStarted main = operationStarted("main-run", "main");
        SessionRecordDraft.OperationStarted thread = operationStarted("thread-run", "thread");
        SessionRecord committedMain = join(session.appendRecord(main));
        join(session.appendRecord(thread));
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt", "main", "main-run", SessionRecordDraft.Step.ASSISTANT,
                0, "assistant-result", null
        )));

        assertEquals(List.of("attempt", "thread-run", "main-run"),
                recordIds(join(session.findRecords())));
        assertEquals(List.of("attempt"), recordIds(join(session.findRecords(
                new SessionRecordQuery(
                        "main", null, "main-run", null, committedMain.sequence(),
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        ))));
        assertEquals(List.of("thread-run"), recordIds(join(session.findRecords(
                new SessionRecordQuery(
                        null, SessionRecordDraft.Type.OPERATION_STARTED, null,
                        SessionRecordDraft.OperationKind.RUN, null, null, 1
                )
        ))));
        assertEquals(List.of("main-run"), recordIds(join(
                session.findOpenOperations("main", 2)
        )));
        assertCode(SessionError.Code.STORAGE, session.appendRecord(
                operationStarted("second-main", "main")
        ));

        join(session.appendRecord(new SessionRecordDraft.OperationFinished(
                "finish", "main", "main-run",
                SessionRecordDraft.OperationOutcome.COMPLETED, null
        )));
        assertTrue(join(session.findOpenOperations("main", 2)).isEmpty());
        assertEquals(List.of("thread-run"), recordIds(join(
                session.findOpenOperations("thread", 2)
        )));
        assertThrows(SessionError.class, () -> new SessionRecordQuery(
                null, null, null, SessionRecordDraft.OperationKind.RUN,
                null, null, null
        ));
    }

    @Test
    void doesNotCopyOperationRecordsIntoForks() {
        InMemorySessionRepository repository = repository();
        AgentSession source = create(repository, "source");
        join(source.appendRecord(operationStarted("run", "main")));
        AgentSession fork = join(repository.fork(
                join(source.metadata()),
                new SessionForkOptions(
                        SessionForkOptions.Scope.TREE, null, null, "fork", null
                )
        ));
        assertTrue(join(fork.findRecords()).isEmpty());
        assertTrue(join(fork.findOpenOperations("main", 2)).isEmpty());
    }

    @Test
    void publishesPersistenceBeforeMutatingStateOrAllocatingSequence() {
        java.util.concurrent.atomic.AtomicBoolean fail =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        InMemorySessionState state = new InMemorySessionState(
                new SessionMetadata("session", 1L),
                Clock.fixed(Instant.ofEpochMilli(1L), ZoneOffset.UTC),
                ignored -> {
                    if (fail.getAndSet(false)) {
                        throw new SessionError(
                                SessionError.Code.STORAGE, "injected append failure"
                        );
                    }
                }
        );
        AgentSession session = new AgentSession(
                state, ignored -> "generated", "main"
        );
        assertCode(SessionError.Code.STORAGE, session.append(
                new SessionEntryDraft.Custom("failed", "note", null)
        ));
        assertNull(join(session.leafId()));
        assertTrue(join(session.findEntries()).isEmpty());

        SessionEntry committed = join(session.append(
                new SessionEntryDraft.Custom("committed", "note", null)
        ));
        assertEquals(1, committed.sequence());
        assertEquals("committed", join(session.leafId()));
    }

    @Test
    void rejectsNonDurablePayloadsBeforeAllocatingASequence() {
        AgentSession session = create(repository(), "session");
        assertCode(SessionError.Code.INVALID_ENTRY, session.append(
                new SessionEntryDraft.Message(
                        "pending",
                        new AssistantMessage(
                                List.of(), "api", "provider", "model", Usage.ZERO,
                                StopReason.PENDING, null, 1L
                        )
                )
        ));
        assertCode(SessionError.Code.INVALID_PAYLOAD, session.append(
                new SessionEntryDraft.Message("custom-message", new MutableMessage(1L))
        ));
        assertThrows(SessionError.class, () -> new SessionEntryDraft.Custom(
                "pojo", "note", new POJONode(new Object())
        ));

        SessionEntry valid = join(session.append(new SessionEntryDraft.Custom(
                "valid", "note", MAPPER.valueToTree(java.util.Map.of("value", 1))
        )));
        assertEquals(1, valid.sequence());
    }

    @Test
    void linearizesConcurrentWritesAndUsesUuidV7Identifiers() {
        InMemorySessionRepository repo = new InMemorySessionRepository();
        AgentSession session = join(repo.create(SessionRepository.CreateOptions.DEFAULT));
        List<CompletableFuture<String>> writes = new ArrayList<>();
        for (int index = 0; index < 50; index++) {
            int value = index;
            writes.add(CompletableFuture.supplyAsync(() -> join(session.appendMessage(
                    UserMessage.text("message " + value, value)
            ))));
        }
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();

        List<SessionEntry> entries = join(session.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        )));
        assertEquals(50, entries.size());
        assertEquals(
                java.util.stream.LongStream.rangeClosed(1, 50).boxed().toList(),
                entries.stream().map(SessionEntry::sequence).toList()
        );
        UUID sessionId = UUID.fromString(join(session.metadata()).id());
        assertEquals(7, sessionId.version());
        assertEquals(2, sessionId.variant());
        assertTrue(entries.stream().map(SessionEntry::id).distinct().count() == 50);
    }

    @Test
    void validatesRepositoryAndForkFailures() {
        InMemorySessionRepository repo = repository();
        AgentSession source = create(repo, "source");
        join(source.append(new SessionEntryDraft.Custom("custom", "note", null)));
        SessionMetadata metadata = join(source.metadata());

        assertCode(SessionError.Code.ALREADY_EXISTS, repo.create(
                new SessionRepository.CreateOptions("source", null)
        ));
        assertCode(SessionError.Code.INVALID_FORK_TARGET, repo.fork(
                metadata,
                new SessionForkOptions(null, null, null, "fork", null)
        ));
        join(repo.delete(metadata));
        assertCode(SessionError.Code.NOT_FOUND, repo.open(metadata));
        join(repo.delete(metadata));
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

    private static List<String> recordIds(List<SessionRecord> records) {
        return records.stream().map(SessionRecord::id).toList();
    }

    private record MutableMessage(long timestamp) implements io.github.idoly.pi.ai.AgentMessage {
    }

    private static InMemorySessionRepository repository() {
        AtomicInteger ids = new AtomicInteger();
        return new InMemorySessionRepository(
                CLOCK, timestamp -> "generated-" + ids.incrementAndGet()
        );
    }

    private static AgentSession create(InMemorySessionRepository repo, String id) {
        return join(repo.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static AssistantMessage assistant(String text) {
        return new AssistantMessage(
                List.of(new TextContent(text)), "api", "provider", "model",
                Usage.ZERO, StopReason.STOP, null, 1L
        );
    }

    private static List<String> ids(List<SessionEntry> entries) {
        return entries.stream().map(SessionEntry::id).toList();
    }

    private static void assertCode(
            SessionError.Code code,
            java.util.concurrent.CompletionStage<?> operation
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> operation.toCompletableFuture().join()
        );
        assertEquals(code, assertInstanceOf(SessionError.class, failure.getCause()).code());
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
