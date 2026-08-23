package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SessionUpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void memoryTreeQueriesContextFactsStatsAndForksMatchUpstream() throws Exception {
        JsonNode fixture = MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"),
                "session-memory-0.84.2.json"
        ).toFile());
        assertEquals("@earendil-works/pi-agent-core", fixture.path("upstream").path("package").asText());
        assertEquals("0.84.2", fixture.path("upstream").path("version").asText());
        assertEquals(fixture.path("scenario").toString(), runScenario().toString());
    }

    private JsonNode runScenario() {
        AtomicInteger generatedIds = new AtomicInteger();
        InMemorySessionRepository repo = new InMemorySessionRepository(
                Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC),
                timestamp -> "generated-" + generatedIds.incrementAndGet()
        );
        AgentSession session = join(repo.create(new SessionRepository.CreateOptions(
                "source", null
        )));
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1L)
        )));
        join(session.createLane("thread", "root"));
        join(session.append(new SessionEntryDraft.Custom(
                "old-note", "note", MAPPER.valueToTree(java.util.Map.of("value", 1))
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
        join(session.append(new SessionEntryDraft.Custom(
                "new-note", "note", MAPPER.valueToTree(java.util.Map.of("value", 2))
        )));
        join(session.append(new SessionEntryDraft.Message(
                "main-tail", UserMessage.text("tail", 3L)
        )));
        join(session.view("thread").append(new SessionEntryDraft.Message(
                "thread-tail", UserMessage.text("thread", 4L)
        )));
        join(session.name("Source"));
        join(session.label("compact", "checkpoint"));
        Usage usage = new Usage(
                10, 5, 3, 2, 20,
                new Cost(1, 2, 3, 4, 10)
        );
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", "main-tail",
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), null, null
                )
        )));
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt", "main", "run", SessionRecordDraft.Step.ASSISTANT,
                0, "future-assistant", null
        )));
        join(session.appendRecord(new SessionRecordDraft.OperationFinished(
                "finish", "main", "run",
                SessionRecordDraft.OperationOutcome.COMPLETED, null
        )));
        join(session.appendRecord(new SessionRecordDraft.UsageRecord(
                "usage", "main", "adjustment", usage,
                null, null, null, null, null, null
        )));
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "thread-run", "thread", "thread-tail",
                new SessionRecordDraft.OperationIntent.Navigation(
                        "root", false, null, null, null
                )
        )));

        ObjectNode actual = MAPPER.createObjectNode();
        ArrayNode entries = actual.putArray("entries");
        join(session.findEntries(oldest())).forEach(entry -> entries.add(normalizeEntry(entry)));
        actual.set("lanes", MAPPER.valueToTree(join(session.lanes())));
        ArrayNode log = actual.putArray("log");
        join(session.log(0, null)).forEach(item -> log.addObject()
                .put("kind", logKind(item))
                .put("seq", item.sequence()));
        actual.set("noteIds", ids(join(session.findEntries(new SessionEntryQuery(
                null, "note", null, null, null
        )))));
        actual.set("cursorIds", ids(join(session.findEntries(new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, 2, 3L
        )))));
        actual.set("boundedMessageIds", ids(join(session.findEntriesOnBranch(
                new SessionBranchQuery(
                        null, null, SessionEntry.Type.COMPACTION,
                        new SessionEntryQuery(
                                SessionEntry.Type.MESSAGE, null, null, null, null
                        )
                )
        ))));
        actual.set("recordIds", recordIds(join(session.findRecords())));
        actual.set("runRecordIds", recordIds(join(session.findRecords(
                new SessionRecordQuery(
                        null, null, "run", null, 13L,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        ))));
        actual.set("openMainIds", recordIds(join(
                session.findOpenOperations("main", 2)
        )));
        actual.set("openThreadIds", recordIds(join(
                session.findOpenOperations("thread", 2)
        )));
        SessionContext context = join(session.context());
        ObjectNode contextNode = actual.putObject("context");
        ArrayNode roles = contextNode.putArray("roles");
        context.messages().forEach(message -> roles.add(
                message instanceof CompactionSummaryMessage ? "compactionSummary"
                        : message instanceof UserMessage ? "user" : "assistant"
        ));
        contextNode.put("thinkingLevel", context.thinkingLevel());
        contextNode.set("model", MAPPER.valueToTree(context.model()));
        contextNode.set("activeToolNames", MAPPER.valueToTree(context.activeToolNames()));
        actual.set("stats", normalizeStats(join(session.stats())));
        actual.put("name", join(session.name()));
        actual.put("label", join(session.label("compact")));

        SessionMetadata source = join(session.metadata());
        AgentSession branch = join(repo.fork(source, new SessionForkOptions(
                SessionForkOptions.Scope.BRANCH, "main-tail",
                SessionForkOptions.Position.AT, "branch-fork", null
        )));
        ObjectNode branchNode = actual.putObject("branchFork");
        branchNode.set("entries", ids(join(branch.findEntries(oldest()))));
        branchNode.set("lanes", MAPPER.valueToTree(join(branch.lanes())));
        branchNode.set("stats", normalizeStats(join(branch.stats())));
        SessionMetadata branchMetadata = join(branch.metadata());
        branchNode.putObject("metadata")
                .put("id", branchMetadata.id())
                .put("parentSessionId", branchMetadata.parentSessionId());

        AgentSession tree = join(repo.fork(source, new SessionForkOptions(
                SessionForkOptions.Scope.TREE, null, null, "tree-fork", null
        )));
        ObjectNode treeNode = actual.putObject("treeFork");
        treeNode.set("entries", ids(join(tree.findEntries(oldest()))));
        treeNode.set("lanes", MAPPER.valueToTree(join(tree.lanes())));
        treeNode.set("stats", normalizeStats(join(tree.stats())));
        return actual;
    }

    private static SessionEntryQuery oldest() {
        return new SessionEntryQuery(
                null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
        );
    }

    private static ObjectNode normalizeStats(SessionStats stats) {
        ObjectNode node = MAPPER.createObjectNode()
                .put("messageCount", stats.messageCount())
                .put("cachedTokens", stats.cachedTokens())
                .put("uncachedTokens", stats.uncachedTokens())
                .put("totalTokens", stats.totalTokens());
        if (stats.costTotal() == Math.rint(stats.costTotal())) {
            node.put("costTotal", (long) stats.costTotal());
        } else {
            node.put("costTotal", stats.costTotal());
        }
        return node;
    }

    private static ObjectNode normalizeEntry(SessionEntry entry) {
        ObjectNode node = MAPPER.createObjectNode()
                .put("id", entry.id())
                .put("type", type(entry.type()));
        if (entry.parentId() == null) node.putNull("parentId");
        else node.put("parentId", entry.parentId());
        node.put("seq", entry.sequence());
        if (entry instanceof SessionEntry.Custom custom) {
            node.put("customType", custom.customType());
            if (custom.data() != null) node.set("data", custom.data());
        }
        return node;
    }

    private static String type(SessionEntry.Type type) {
        return switch (type) {
            case MESSAGE -> "message";
            case MODEL_CHANGE -> "model_change";
            case THINKING_LEVEL_CHANGE -> "thinking_level_change";
            case ACTIVE_TOOLS_CHANGE -> "active_tools_change";
            case COMPACTION -> "compaction";
            case BRANCH_SUMMARY -> "branch_summary";
            case CUSTOM -> "custom";
        };
    }

    private static String logKind(SessionLogItem item) {
        if (item instanceof SessionLogItem.Entry) return "entry";
        if (item instanceof SessionLogItem.Lane) return "lane";
        if (item instanceof SessionLogItem.Record || item instanceof SessionLogItem.Usage) {
            return "record";
        }
        return "fact";
    }

    private static ArrayNode recordIds(List<SessionRecord> records) {
        ArrayNode values = MAPPER.createArrayNode();
        records.forEach(record -> values.add(record.id()));
        return values;
    }

    private static ArrayNode ids(List<SessionEntry> entries) {
        ArrayNode values = MAPPER.createArrayNode();
        entries.forEach(entry -> values.add(entry.id()));
        return values;
    }

    private static AssistantMessage assistant(String text) {
        return new AssistantMessage(
                List.of(new TextContent(text)), "openai-responses", "openai", "gpt-5",
                Usage.ZERO, StopReason.STOP, null, 1L
        );
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
