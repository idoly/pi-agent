package io.github.idoly.pi.agent.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.agent.QueueMode;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.InMemorySessionRepository;
import io.github.idoly.pi.agent.session.SessionRecordDraft;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentHarnessScaffoldTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void publicScaffoldMatchesUpstreamFixture() throws Exception {
        JsonNode expected = MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"),
                "agent-harness-0.84.2.json"
        ).toFile());
        AgentHarness harness = harness();
        ObjectNode actual = MAPPER.createObjectNode();
        actual.putObject("upstream")
                .put("package", "@earendil-works/pi-agent-core")
                .put("version", "0.84.2");
        ObjectNode defaults = actual.putObject("defaults");
        defaults.put("name", harness.name());
        defaults.putNull("leafId");
        defaults.putArray("suspended");
        defaults.put("thinkingLevel", harness.thinkingLevel());
        defaults.putArray("activeToolNames");
        defaults.putObject("retryPolicy")
                .put("enabled", harness.retryPolicy().enabled())
                .put("maxRetries", harness.retryPolicy().maxRetries())
                .put("baseDelayMs", harness.retryPolicy().baseDelayMillis());
        defaults.putObject("compactionSettings")
                .put("enabled", harness.compactionSettings().enabled())
                .put("reserveTokens", harness.compactionSettings().reserveTokens())
                .put("keepRecentTokens", harness.compactionSettings().keepRecentTokens());
        defaults.put("steeringMode", queueMode(harness.steeringMode()));
        defaults.put("followUpMode", queueMode(harness.followUpMode()));

        ObjectNode unavailable = actual.putObject("unavailable");
        unavailable.set("prompt", errorNode(harness.prompt("hello")));
        unavailable.set("compact", errorNode(harness.compact(null)));
        unavailable.set("resume", errorNode(harness.resume()));
        unavailable.set("waitForIdle", errorNode(harness.waitForIdle()));
        unavailable.set("watchSession", errorNode(harness.watchSession()));

        AgentSession recorded = session("oracle-recorded");
        join(recorded.appendRecord(operationStarted("run")));
        actual.set("restore", errorNode(AgentHarness.create(
                new AgentHarnessOptions(recorded, model("one"))
        )));
        harness.close();
        ObjectNode closed = actual.putObject("closed");
        closed.set("prompt", errorNode(harness.prompt("hello")));
        closed.set("waitForIdle", errorNode(harness.waitForIdle()));
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    void createsOnlyFromRecordFreeSessions() {
        AgentSession session = session("empty");
        AgentHarness.CreateResult created = join(AgentHarness.create(
                new AgentHarnessOptions(session, model("one"))
        ));
        assertTrue(created.suspended().isEmpty());
        assertEquals("main", created.harness().name());
        assertSame(session, created.harness().session());
        assertEquals(null, join(created.harness().leafId()));

        AgentSession recorded = session("recorded");
        join(recorded.appendRecord(operationStarted("run")));
        HarnessNotImplemented failure = assertFailure(
                HarnessNotImplemented.class,
                AgentHarness.create(new AgentHarnessOptions(recorded, model("one")))
        );
        assertEquals("create.restore", failure.operation());
    }

    @Test
    void keepsScaffoldConfigurationAsDefensiveCopies() {
        AgentHarness harness = harness();
        Model replacement = model("two");
        harness.model(replacement);
        assertSame(replacement, harness.model());
        harness.thinkingLevel("high");
        assertEquals("high", harness.thinkingLevel());

        ArrayList<String> names = new ArrayList<>(List.of("one"));
        harness.activeToolNames(names);
        names.add("mutated");
        assertEquals(List.of("one"), harness.activeToolNames());
        assertThrows(UnsupportedOperationException.class,
                () -> harness.activeToolNames().add("mutated"));

        ArrayList<Skill> skills = new ArrayList<>(List.of(
                new Skill("skill", "desc", "body", "/tmp/SKILL.md")
        ));
        HarnessResources resources = new HarnessResources(
                skills, List.of(new PromptTemplate("template", "body"))
        );
        harness.resources(resources);
        skills.add(new Skill("changed", "desc", "body", "/tmp/OTHER.md"));
        assertEquals(List.of("skill"), harness.resources().skills().stream()
                .map(Skill::name).toList());

        harness.retryPolicy(new RetryPolicy(true, 2, 10));
        harness.compactionSettings(new CompactionSettings(false, 1, 2));
        harness.steeringMode(QueueMode.ALL);
        harness.followUpMode(QueueMode.ALL);
        assertEquals(new RetryPolicy(true, 2, 10), harness.retryPolicy());
        assertEquals(new CompactionSettings(false, 1, 2), harness.compactionSettings());
        assertEquals(QueueMode.ALL, harness.steeringMode());
        assertEquals(QueueMode.ALL, harness.followUpMode());
    }

    @Test
    void rejectsUnfinishedOperationsExplicitlyAndChangesErrorAfterClose() {
        AgentHarness harness = harness();
        assertUnavailable("prompt", harness.prompt("hello"));
        assertUnavailable("compact", harness.compact(null));
        assertUnavailable("resume", harness.resume());
        assertUnavailable("waitForIdle", harness.waitForIdle());
        assertUnavailable("watchSession", harness.watchSession());
        HarnessNotImplemented hookFailure = assertThrows(
                HarnessNotImplemented.class,
                () -> harness.hooks().on(
                        "before_run", ignored -> java.util.concurrent.CompletableFuture.completedFuture(null)
                )
        );
        assertEquals("hooks.on", hookFailure.operation());

        harness.close();
        assertFailure(HarnessClosed.class, harness.prompt(
                UserMessage.text("hello", 1L)
        ));
        assertFailure(HarnessClosed.class, harness.waitForIdle());
        assertThrows(HarnessClosed.class, () -> harness.events().on(
                "event", ignored -> java.util.concurrent.CompletableFuture.completedFuture(null)
        ));
    }

    private static ObjectNode errorNode(CompletionStage<?> stage) {
        Throwable failure;
        try {
            stage.toCompletableFuture().join();
            throw new AssertionError("Expected operation to fail");
        } catch (CompletionException error) {
            failure = error.getCause();
        }
        ObjectNode node = MAPPER.createObjectNode()
                .put("name", failure.getClass().getSimpleName());
        if (failure instanceof HarnessNotImplemented unavailable) {
            node.put("operation", unavailable.operation());
        }
        node.put("message", failure.getMessage());
        return node;
    }

    private static String queueMode(QueueMode mode) {
        return mode == QueueMode.ALL ? "all" : "one-at-a-time";
    }

    private static void assertUnavailable(String operation, CompletionStage<?> stage) {
        HarnessNotImplemented failure = assertFailure(HarnessNotImplemented.class, stage);
        assertEquals(operation, failure.operation());
    }

    private static AgentHarness harness() {
        return join(AgentHarness.create(
                new AgentHarnessOptions(session("session"), model("one"))
        )).harness();
    }

    private static AgentSession session(String id) {
        return join(new InMemorySessionRepository().create(
                new io.github.idoly.pi.agent.session.SessionRepository.CreateOptions(id, null)
        ));
    }

    private static SessionRecordDraft.OperationStarted operationStarted(String id) {
        return new SessionRecordDraft.OperationStarted(
                id, "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), null, null
                )
        );
    }

    private static Model model(String id) {
        return new Model(
                id, id, "openai-responses", "openai", "https://api.openai.com/v1",
                true, List.of("text", "image"), 100_000, 10_000
        );
    }

    private static <T extends Throwable> T assertFailure(
            Class<T> type,
            CompletionStage<?> stage
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> stage.toCompletableFuture().join()
        );
        return assertInstanceOf(type, failure.getCause());
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
