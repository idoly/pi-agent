package io.github.idoly.pi.agent.compaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.InMemorySessionRepository;
import io.github.idoly.pi.agent.session.JsonlSessionRepository;
import io.github.idoly.pi.agent.session.SessionEntry;
import io.github.idoly.pi.agent.session.SessionEntryDraft;
import io.github.idoly.pi.agent.session.SessionEntryQuery;
import io.github.idoly.pi.agent.session.SessionRepository;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedModelStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContextCompactionTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path temporary;

    @Test
    void pureAlgorithmsMatchUpstreamFixture() throws Exception {
        JsonNode fixture = MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"),
                "compaction-0.84.2.json"
        ).toFile());
        List<AgentMessage> estimates = estimateMessages();
        assertEquals(
                fixture.path("estimates").path("messageTokens").toString(),
                MAPPER.valueToTree(estimates.stream()
                        .map(ContextCompaction::estimateTokens).toList()).toString()
        );
        ContextCompaction.ContextUsageEstimate context =
                ContextCompaction.estimateContextTokens(estimates);
        JsonNode expectedContext = fixture.path("estimates").path("context");
        assertEquals(expectedContext.path("tokens").asLong(), context.tokens());
        assertEquals(expectedContext.path("usageTokens").asLong(), context.usageTokens());
        assertEquals(expectedContext.path("trailingTokens").asLong(), context.trailingTokens());
        assertEquals(expectedContext.path("lastUsageIndex").asInt(), context.lastUsageIndex());
        CompactionSettings threshold = new CompactionSettings(true, 10, 20);
        assertTrue(ContextCompaction.shouldCompact(95, 100, threshold));
        assertFalse(ContextCompaction.shouldCompact(90, 100, threshold));

        List<SessionEntry> cutEntries = cutEntries();
        ContextCompaction.CutPoint cut = ContextCompaction.findCutPoint(
                cutEntries, 0, cutEntries.size(), 1
        );
        JsonNode expectedCut = fixture.path("cutPoint");
        assertEquals(expectedCut.path("firstKeptEntryIndex").asInt(), cut.firstKeptEntryIndex());
        assertEquals(expectedCut.path("turnStartIndex").asInt(), cut.turnStartIndex());
        assertEquals(expectedCut.path("isSplitTurn").asBoolean(), cut.splitTurn());

        CompactionPreparation preparation = ContextCompaction.prepare(
                preparationEntries(), new CompactionSettings(true, 100, 1)
        );
        assertNotNull(preparation);
        JsonNode expected = fixture.path("preparation");
        assertEquals(expected.path("previousSummary").asText(), preparation.previousSummary());
        assertEquals(expected.path("isSplitTurn").asBoolean(), preparation.splitTurn());
        assertEquals(expected.path("tokensBefore").asLong(), preparation.tokensBefore());
        assertEquals(jsonRoles(expected.path("messagesToSummarize")), roles(preparation.messagesToSummarize()));
        assertEquals(jsonRoles(expected.path("turnPrefixMessages")), roles(preparation.turnPrefixMessages()));
        assertEquals(jsonRoles(expected.path("retainedTail")), roles(preparation.retainedTail()));
        ContextCompaction.FileLists files = ContextCompaction.computeFileLists(
                preparation.fileOperations()
        );
        assertEquals(jsonRoles(expected.path("fileLists").path("readFiles")), files.readFiles());
        assertEquals(jsonRoles(expected.path("fileLists").path("modifiedFiles")), files.modifiedFiles());

        BranchSummarization.BranchPreparation branch =
                BranchSummarization.prepare(cutEntries, 8);
        assertEquals(
                jsonRoles(fixture.path("branchPreparation").path("roles")),
                roles(branch.messages())
        );
        assertEquals(
                fixture.path("branchPreparation").path("totalTokens").asLong(),
                branch.totalTokens()
        );
        assertEquals(fixture.path("serialized").asText(),
                ContextCompaction.serializeConversation(
                        estimates.stream().filter(io.github.idoly.pi.ai.Message.class::isInstance)
                                .map(io.github.idoly.pi.ai.Message.class::cast).toList()
                ));
        assertEquals(fixture.path("formattedFiles").asText(),
                ContextCompaction.formatFileOperations(List.of("a.ts"), List.of("b.ts")));
    }

    @Test
    void combinesSplitTurnSummariesUsageAndFileMetadata() {
        CompactionPreparation preparation = ContextCompaction.prepare(
                preparationEntries(), new CompactionSettings(true, 100, 1)
        );
        ArrayList<CompactionSummarizer.Request> requests = new ArrayList<>();
        CompactionSummarizer summarizer = request -> {
            requests.add(request);
            return java.util.concurrent.CompletableFuture.completedFuture(
                    request.kind() == CompactionSummarizer.Kind.HISTORY
                            ? new CompactionSummarizer.Summary(
                                    "history", usage(1, 2, 3, 4)
                            )
                            : new CompactionSummarizer.Summary(
                                    "prefix", usage(5, 6, 7, 8)
                            )
            );
        };
        CompactionResult result = join(ContextCompaction.compact(
                preparation, summarizer, "focus"
        ));
        assertEquals(List.of(
                CompactionSummarizer.Kind.HISTORY,
                CompactionSummarizer.Kind.TURN_PREFIX
        ), requests.stream().map(CompactionSummarizer.Request::kind).toList());
        assertEquals(List.of(80L, 50L), requests.stream()
                .map(CompactionSummarizer.Request::maxTokens).toList());
        assertEquals("focus", requests.getFirst().customInstructions());
        assertNull(requests.getLast().customInstructions());
        assertEquals(usage(6, 8, 10, 12), result.usage());
        assertTrue(result.summary().startsWith(
                "history\n\n---\n\n**Turn Context (split turn):**\n\nprefix"
        ));
        assertTrue(result.summary().contains("<read-files>\nold-read.ts"));
        assertTrue(result.summary().contains("old-edit.ts\nwritten.ts"));
        assertEquals(List.of("assistant"), roles(result.retainedTail()));
    }

    @Test
    void persistsCheckpointAndReopensItFromJsonl() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(100), ZoneOffset.UTC);
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary, temporary.resolve("workspace"), clock,
                timestamp -> "id-" + (timestamp == null ? "next" : timestamp)
        );
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("session", null)
        ));
        join(session.append(new SessionEntryDraft.Message(
                "user", UserMessage.text("request", 1L)
        )));
        join(session.append(new SessionEntryDraft.Message(
                "assistant", assistant(
                        List.of(new TextContent("answer")), usage(100, 50)
                )
        )));
        SessionEntry.Compaction checkpoint = join(SessionCompaction.compact(
                session, new CompactionSettings(true, 100, 1),
                request -> java.util.concurrent.CompletableFuture.completedFuture(
                        new CompactionSummarizer.Summary("checkpoint", usage(2, 1))
                ), null
        ));
        assertNotNull(checkpoint);
        assertTrue(checkpoint.summary().endsWith("checkpoint"));
        assertEquals("id-next", checkpoint.id());
        assertEquals(checkpoint.id(), join(session.leafId()));

        AgentSession reopened = join(new JsonlSessionRepository(
                temporary, temporary.resolve("workspace"), clock,
                timestamp -> "unused"
        ).open(join(session.metadata())));
        SessionEntry restored = join(reopened.findEntry(new SessionEntryQuery(
                SessionEntry.Type.COMPACTION, null, null, 1, null
        )));
        assertEquals(checkpoint, restored);
        assertEquals("compactionSummary", role(join(reopened.context()).messages().getFirst()));
        assertNull(join(SessionCompaction.compact(
                reopened, new CompactionSettings(true, 100, 1),
                request -> java.util.concurrent.CompletableFuture.failedFuture(
                        new AssertionError("summarizer must not run")
                ), null
        )));
    }

    @Test
    void collectsAbandonedBranchAndAppendsSummaryAtCurrentTarget() {
        AgentSession session = join(new InMemorySessionRepository().create(
                new SessionRepository.CreateOptions("session", null)
        ));
        join(session.append(new SessionEntryDraft.Message(
                "root", UserMessage.text("root", 1L)
        )));
        join(session.append(new SessionEntryDraft.Message(
                "common", UserMessage.text("common", 1L)
        )));
        join(session.append(new SessionEntryDraft.Message(
                "abandoned-1", UserMessage.text("abandoned 1", 1L)
        )));
        join(session.append(new SessionEntryDraft.Message(
                "abandoned-2", UserMessage.text("abandoned 2", 1L)
        )));
        join(session.createLane("target", "common"));
        AgentSession target = session.view("target");
        join(target.append(new SessionEntryDraft.Message(
                "target", UserMessage.text("target", 1L)
        )));

        BranchSummarization.CollectedEntries collected = join(
                BranchSummarization.collect(session, "abandoned-2", "target")
        );
        assertEquals("common", collected.commonAncestorId());
        assertEquals(List.of("abandoned-1", "abandoned-2"), collected.entries()
                .stream().map(SessionEntry::id).toList());

        SessionEntry.BranchSummary summary = join(
                BranchSummarization.summarizeAndAppend(
                        target, "abandoned-2", "target", 1_000, 2_048,
                        null, request -> java.util.concurrent.CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("branch work", Usage.ZERO)
                        )
                )
        );
        assertEquals("target", summary.parentId());
        assertEquals("abandoned-2", summary.fromId());
        assertTrue(summary.summary().startsWith(BranchSummarization.PREAMBLE));
        assertEquals(summary.id(), join(target.leafId()));
    }

    @Test
    void modelSummarizerUsesAgentLoopPromptAndOutputBudget() {
        AssistantMessage response = new AssistantMessage(
                List.of(new TextContent("generated summary")),
                "openai-responses", "openai", "gpt-5", usage(3, 2),
                StopReason.STOP, null, 2L
        );
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(response)
        ));
        Model model = new Model(
                "gpt-5", "GPT-5", "openai-responses", "openai",
                "https://api.openai.com/v1", true, List.of("text", "image"),
                100_000, 50
        );
        ModelCompactionSummarizer summarizer = new ModelCompactionSummarizer(
                model, stream, "high", null
        );
        CompactionSummarizer.Summary summary = join(summarizer.summarize(
                new CompactionSummarizer.Request(
                        CompactionSummarizer.Kind.HISTORY,
                        List.of(UserMessage.text("work", 1L)), "old summary",
                        "focus", 100
                )
        ));
        assertEquals("generated summary", summary.text());
        assertEquals(usage(3, 2), summary.usage());
        assertEquals(50, stream.models().getFirst().maxTokens());
        assertEquals("high", stream.options().getFirst().thinkingLevel());
        assertEquals(ModelCompactionSummarizer.SYSTEM_PROMPT,
                stream.lastContext().systemPrompt());
        String prompt = ((TextContent) ((UserMessage) stream.lastContext()
                .messages().getFirst()).content().getFirst()).text();
        assertTrue(prompt.contains("<conversation>\n[User]: work\n</conversation>"));
        assertTrue(prompt.contains("<previous-summary>\nold summary\n</previous-summary>"));
        assertTrue(prompt.contains("Additional focus: focus"));
    }

    @Test
    void serializesAndTruncatesToolResults() {
        ToolResultMessage result = new ToolResultMessage(
                "call", "read", List.of(new TextContent("x".repeat(5_000))),
                Map.of(), null, false, 1L
        );
        String serialized = ContextCompaction.serializeConversation(List.of(result));
        assertTrue(serialized.contains("[Tool result]:"));
        assertTrue(serialized.contains("[... 3000 more characters truncated]"));
    }

    private static List<AgentMessage> estimateMessages() {
        return List.of(
                UserMessage.text("12345678", 1L),
                assistant(List.of(
                        new ThinkingContent("1234", null),
                        new ToolCallContent("call", "read", Map.of("path", "a.ts"))
                ), usage(10, 5, 3, 2)),
                new ToolResultMessage(
                        "call", "read",
                        List.of(
                                new TextContent("ok"),
                                new ImageContent("abc", "image/png")
                        ), Map.of(), null, false, 1L
                )
        );
    }

    private static List<SessionEntry> cutEntries() {
        return List.of(
                message("u1", null, 1, UserMessage.text("first request", 1L)),
                message("a1", "u1", 2, assistant(
                        List.of(new TextContent("first answer")), usage(100, 50)
                )),
                message("tr1", "a1", 3, new ToolResultMessage(
                        "call", "read", List.of(new TextContent("result")),
                        Map.of(), null, false, 1L
                )),
                message("u2", "tr1", 4, UserMessage.text("second request", 1L)),
                message("a2", "u2", 5, assistant(
                        List.of(new TextContent("second answer")), usage(100, 50)
                ))
        );
    }

    private static List<SessionEntry> preparationEntries() {
        AgentMessage retainedUser = UserMessage.text("retained user", 1L);
        AgentMessage retainedAssistant = assistant(
                List.of(new ToolCallContent(
                        "write", "write", Map.of("path", "written.ts")
                )), usage(100, 50)
        );
        JsonNode details = MAPPER.valueToTree(Map.of(
                "readFiles", List.of("old-read.ts"),
                "modifiedFiles", List.of("old-edit.ts")
        ));
        SessionEntry previous = new SessionEntry.Compaction(
                "compact", null, 1, 1, "previous summary",
                List.of(retainedUser, retainedAssistant), 1_000, details, null
        );
        SessionEntry user = message(
                "new-user", "compact", 2, UserMessage.text("new user", 1L)
        );
        SessionEntry assistant = message(
                "new-assistant", "new-user", 3,
                assistant(List.of(new TextContent("new assistant response")), usage(200, 50))
        );
        return List.of(previous, user, assistant);
    }

    private static SessionEntry.Message message(
            String id,
            String parent,
            long sequence,
            AgentMessage message
    ) {
        return new SessionEntry.Message(id, parent, sequence, 1L, message, false);
    }

    private static AssistantMessage assistant(
            List<ContentBlock> content,
            Usage usage
    ) {
        return new AssistantMessage(
                content, "openai-responses", "openai", "gpt-5",
                usage, StopReason.STOP, null, 1L
        );
    }

    private static Usage usage(long input, long output) {
        return usage(input, output, 0, 0);
    }

    private static Usage usage(
            long input,
            long output,
            long cacheRead,
            long cacheWrite
    ) {
        return new Usage(
                input, output, cacheRead, cacheWrite,
                input + output + cacheRead + cacheWrite, Cost.ZERO
        );
    }

    private static List<String> roles(List<AgentMessage> messages) {
        return messages.stream().map(ContextCompactionTest::role).toList();
    }

    private static String role(AgentMessage message) {
        if (message instanceof UserMessage) return "user";
        if (message instanceof AssistantMessage) return "assistant";
        if (message instanceof ToolResultMessage) return "toolResult";
        if (message instanceof io.github.idoly.pi.agent.session.CompactionSummaryMessage) {
            return "compactionSummary";
        }
        return "branchSummary";
    }

    private static List<String> jsonRoles(JsonNode node) {
        ArrayList<String> values = new ArrayList<>();
        node.forEach(value -> values.add(value.asText()));
        return values;
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
