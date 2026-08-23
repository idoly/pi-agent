package io.github.idoly.pi.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AgentLoopUpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void promptAndContinueMatchPublishedTypeScriptLoop() throws Exception {
        JsonNode scenarios = fixture().path("scenarios");
        assertEquals(scenarios.path("lowLevelPrompt"), runPromptScenario());
        assertEquals(scenarios.path("lowLevelContinue"), runContinueScenario());
    }

    private JsonNode runPromptScenario() {
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("low-level 1", StopReason.STOP)
                )),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("low-level 2", StopReason.STOP)
                ))
        ));
        AtomicInteger steeringCalls = new AtomicInteger();
        AtomicInteger followUpCalls = new AtomicInteger();
        AgentLoopConfig config = new AgentLoopConfig(
                ScriptedMessages.model(), "off", null, stream, null, null, null,
                ToolExecutionMode.PARALLEL, null, null, null, null,
                () -> CompletableFuture.completedFuture(
                        steeringCalls.getAndIncrement() == 0
                                ? List.of(UserMessage.text("initial steering", 2L))
                                : List.of()
                ),
                () -> CompletableFuture.completedFuture(
                        followUpCalls.getAndIncrement() == 0
                                ? List.of(UserMessage.text("follow up", 3L))
                                : List.of()
                )
        );
        AgentLoopRun run = AgentLoop.run(
                List.of(UserMessage.text("prompt", 1L)),
                new AgentContext("system", List.of(), List.of()), config
        );
        EventCollector collector = new EventCollector();
        run.subscribe(collector);
        List<AgentMessage> result = run.result().toCompletableFuture().join();
        collector.completed.join();

        ObjectNode actual = MAPPER.createObjectNode();
        actual.set("events", normalizeEvents(collector.events));
        actual.set("messages", normalizeMessages(result));
        ArrayNode providerContexts = actual.putArray("providerContexts");
        stream.contexts().forEach(context -> {
            ArrayNode texts = providerContexts.addArray();
            context.messages().stream()
                    .filter(UserMessage.class::isInstance)
                    .map(UserMessage.class::cast)
                    .map(message -> ((TextContent) message.content().getFirst()).text())
                    .forEach(texts::add);
        });
        actual.put("steeringCalls", steeringCalls.get());
        actual.put("followUpCalls", followUpCalls.get());
        return actual;
    }

    private JsonNode runContinueScenario() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("continued", StopReason.STOP)
                )
        ));
        AgentLoopRun run = AgentLoop.continueRun(
                new AgentContext(
                        "", List.of(UserMessage.text("existing", 1L)), List.of()
                ),
                new AgentLoopConfig(ScriptedMessages.model(), stream)
        );
        EventCollector collector = new EventCollector();
        run.subscribe(collector);
        List<AgentMessage> result = run.result().toCompletableFuture().join();
        collector.completed.join();

        ObjectNode actual = MAPPER.createObjectNode();
        actual.set("events", normalizeEvents(collector.events));
        actual.set("messages", normalizeMessages(result));
        return actual;
    }

    private static ArrayNode normalizeEvents(List<AgentEvent> events) {
        ArrayNode normalized = MAPPER.createArrayNode();
        for (AgentEvent event : events) {
            ObjectNode node = normalized.addObject();
            if (event instanceof AgentEvent.AgentStart) {
                node.put("type", "agent_start");
            } else if (event instanceof AgentEvent.AgentEnd) {
                node.put("type", "agent_end");
            } else if (event instanceof AgentEvent.TurnStart) {
                node.put("type", "turn_start");
            } else if (event instanceof AgentEvent.TurnEnd end) {
                node.put("type", "turn_end").put("role", "assistant");
                ArrayNode results = node.putArray("toolResults");
                end.toolResults().forEach(result -> results.add(result.toolName()));
            } else if (event instanceof AgentEvent.MessageStart start) {
                node.put("type", "message_start").put("role", role(start.message()));
            } else if (event instanceof AgentEvent.MessageEnd end) {
                node.put("type", "message_end").put("role", role(end.message()));
            } else if (event instanceof AgentEvent.MessageUpdate) {
                node.put("type", "message_update");
            } else {
                throw new AssertionError("Unexpected low-level event: " + event);
            }
        }
        return normalized;
    }

    private static ArrayNode normalizeMessages(List<AgentMessage> messages) {
        ArrayNode normalized = MAPPER.createArrayNode();
        for (AgentMessage message : messages) {
            ObjectNode node = normalized.addObject();
            node.put("role", role(message));
            if (message instanceof UserMessage user) {
                node.put("text", user.content().stream()
                        .filter(TextContent.class::isInstance)
                        .map(TextContent.class::cast)
                        .map(TextContent::text)
                        .reduce((left, right) -> left + "\n" + right)
                        .orElse(""));
            } else if (message instanceof AssistantMessage assistant) {
                node.put("stopReason", stopReason(assistant.stopReason()));
                ArrayNode content = node.putArray("content");
                assistant.content().forEach(block -> {
                    if (block instanceof TextContent text) {
                        content.addObject().put("type", "text").put("text", text.text());
                    }
                });
            }
        }
        return normalized;
    }

    private static String role(AgentMessage message) {
        if (message instanceof UserMessage) return "user";
        if (message instanceof AssistantMessage) return "assistant";
        return "toolResult";
    }

    private static String stopReason(StopReason reason) {
        return switch (reason) {
            case TOOL_USE -> "toolUse";
            default -> reason.name().toLowerCase();
        };
    }

    private static JsonNode fixture() throws IOException {
        Path root = Path.of(System.getProperty("pi.compatFixtures", "compat-fixtures"));
        return MAPPER.readTree(Files.readString(root.resolve("agent-core-0.84.2.json")));
    }

    private static final class EventCollector implements Flow.Subscriber<AgentEvent> {
        private final List<AgentEvent> events = new ArrayList<>();
        private final CompletableFuture<Void> completed = new CompletableFuture<>();

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(AgentEvent item) {
            events.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            completed.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            completed.complete(null);
        }
    }
}
