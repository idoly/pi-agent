package io.github.idoly.pi.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentKind;
import io.github.idoly.pi.ai.ImageContent;
import io.github.idoly.pi.ai.Message;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedModelStream;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Model MODEL = new Model(
            "fixture-model", "Fixture Model", "openai-responses", "fixture",
            "https://example.invalid", false, List.of("text"), 8_192, 2_048
    );

    @Test
    void fixtureTargetsThePinnedUpstreamVersion() throws Exception {
        assertEquals("@earendil-works/pi-agent-core", fixture().path("upstream").path("package").asText());
        assertEquals("0.84.2", fixture().path("upstream").path("version").asText());
    }

    @Test
    void streamingTextMatchesTheInstalledTypeScriptUpstream() throws Exception {
        JsonNode expected = fixture().path("scenarios").path("streamingText");
        AssistantMessage empty = assistant(List.of(), StopReason.PENDING);
        AssistantMessage partial = assistant(List.of(new TextContent("hello")), StopReason.PENDING);
        AssistantMessage complete = assistant(List.of(new TextContent("hello")), StopReason.STOP);
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Start(empty),
                new AssistantStreamEvent.ContentStart(ContentKind.TEXT, 0, empty),
                new AssistantStreamEvent.ContentDelta(ContentKind.TEXT, 0, "hello", partial),
                new AssistantStreamEvent.ContentEnd(ContentKind.TEXT, 0, partial),
                new AssistantStreamEvent.Done(complete)
        ));
        Agent agent = new Agent(new AgentOptions("", MODEL, stream));
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("hi").toCompletableFuture().join();

        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
    }

    @Test
    void parallelToolsMatchTheInstalledTypeScriptUpstream() throws Exception {
        ControlledTool first = new ControlledTool("first");
        ControlledTool second = new ControlledTool("second");
        AssistantMessage calls = assistant(List.of(
                new ToolCallContent("call-a", "first", Map.of()),
                new ToolCallContent("call-b", "second", Map.of())
        ), StopReason.TOOL_USE);
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(calls)),
                List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("finished")), StopReason.STOP
                )))
        ));
        Agent agent = new Agent(new AgentOptions("", MODEL, stream, List.of(first, second)));
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> run = agent.prompt("tools").toCompletableFuture();
        second.complete("second result");
        first.complete("first result");
        run.join();

        JsonNode expected = fixture().path("scenarios").path("parallelTools");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
    }

    @Test
    void blockedToolMatchesTheInstalledTypeScriptUpstream() throws Exception {
        ControlledTool tool = new ControlledTool("dangerous");
        AssistantMessage call = assistant(List.of(
                new ToolCallContent("call-blocked", "dangerous", Map.of())
        ), StopReason.TOOL_USE);
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(call)),
                List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("continued")), StopReason.STOP
                )))
        ));
        AgentOptions options = new AgentOptions(
                "", MODEL, "off", null, stream,
                null, null, null, List.of(tool), ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                (toolCall, arguments, context, cancellation) ->
                        CompletableFuture.completedFuture(
                                BeforeToolCallResult.block("Blocked by policy")
                        ),
                null, null, null
        );
        Agent agent = new Agent(options);
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("blocked").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("blockedTool");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
        assertEquals(false, tool.result.isDone());
    }

    @Test
    void lengthTruncatedToolMatchesTheInstalledTypeScriptUpstream() throws Exception {
        java.util.concurrent.atomic.AtomicInteger executions =
                new java.util.concurrent.atomic.AtomicInteger();
        AgentTool tool = new AgentTool() {
            private final ToolDefinition definition = new ToolDefinition(
                    "echo", "echo",
                    Map.of(
                            "type", "object",
                            "properties", Map.of("value", Map.of("type", "string")),
                            "required", List.of("value"),
                            "additionalProperties", false
                    )
            );

            @Override
            public ToolDefinition definition() {
                return definition;
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("must not execute")), Map.of()
                ));
            }
        };
        AssistantMessage truncated = assistant(List.of(
                new ToolCallContent("call-length", "echo", Map.of("value", "hel"))
        ), StopReason.LENGTH);
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(truncated)),
                List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("recovered")), StopReason.STOP
                )))
        ));
        Agent agent = new Agent(new AgentOptions("", MODEL, stream, List.of(tool)));
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("truncate").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("lengthTruncatedTool");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
        assertEquals(expected.path("executions").asInt(), executions.get());
    }

    @Test
    void prepareNextTurnSnapshotMatchesTheInstalledTypeScriptUpstream() throws Exception {
        Model nextModel = new Model(
                "next-model", "Next Model", MODEL.api(), MODEL.provider(), MODEL.baseUrl(),
                true, MODEL.input(), MODEL.contextWindow(), MODEL.maxTokens()
        );
        AgentTool tool = new AgentTool() {
            private final ToolDefinition definition = new ToolDefinition(
                    "prepare", "prepare",
                    Map.of("type", "object", "properties", Map.of(), "additionalProperties", false)
            );

            @Override
            public ToolDefinition definition() {
                return definition;
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("prepared tool")), Map.of()
                ));
            }
        };
        AssistantMessage call = assistant(List.of(
                new ToolCallContent("call-prepare", "prepare", Map.of())
        ), StopReason.TOOL_USE);
        AssistantMessage prepared = new AssistantMessage(
                List.of(new TextContent("prepared")), nextModel.api(), nextModel.provider(),
                nextModel.id(), Usage.ZERO, StopReason.STOP, null, 1L
        );
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(call)),
                List.of(new AssistantStreamEvent.Done(prepared))
        ));
        java.util.concurrent.atomic.AtomicBoolean invoked =
                new java.util.concurrent.atomic.AtomicBoolean();
        AgentOptions options = new AgentOptions(
                "first prompt", MODEL, "off", null, stream,
                null, null, null, List.of(tool), ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                null, null,
                (context, cancellation) -> {
                    if (!invoked.compareAndSet(false, true)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    List<AgentMessage> replacement = new ArrayList<>(
                            context.context().messages()
                    );
                    replacement.add(UserMessage.text("prepared context", 4L));
                    return CompletableFuture.completedFuture(new NextTurnUpdate(
                            new AgentContext(
                                    "second prompt", replacement, context.context().tools()
                            ),
                            nextModel,
                            "high"
                    ));
                },
                null
        );
        Agent agent = new Agent(options);
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("start").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("prepareNextTurn");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
        ArrayNode providerCalls = MAPPER.createArrayNode();
        for (int index = 0; index < stream.contexts().size(); index++) {
            io.github.idoly.pi.ai.ModelContext context = stream.contexts().get(index);
            ObjectNode callNode = providerCalls.addObject();
            callNode.put("model", stream.models().get(index).id());
            callNode.put("systemPrompt", context.systemPrompt());
            callNode.put("thinkingLevel", stream.options().get(index).thinkingLevel());
            ArrayNode userTexts = callNode.putArray("userTexts");
            context.messages().stream()
                    .filter(UserMessage.class::isInstance)
                    .map(UserMessage.class::cast)
                    .map(user -> user.content().stream()
                            .filter(TextContent.class::isInstance)
                            .map(TextContent.class::cast)
                            .map(TextContent::text)
                            .reduce((left, right) -> left + "\n" + right)
                            .orElse(""))
                    .forEach(userTexts::add);
        }
        assertEquals(expected.path("providerCalls"), providerCalls);
    }

    @Test
    void followUpQueueModesMatchTheInstalledTypeScriptUpstream() throws Exception {
        for (QueueMode mode : QueueMode.values()) {
            int turns = mode == QueueMode.ALL ? 2 : 3;
            List<List<AssistantStreamEvent>> scripts = new ArrayList<>();
            for (int turn = 1; turn <= turns; turn++) {
                scripts.add(List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("turn " + turn)), StopReason.STOP
                ))));
            }
            ScriptedModelStream stream = ScriptedModelStream.turns(scripts);
            AgentOptions options = new AgentOptions(
                    "", MODEL, "off", null, stream,
                    null, null, null, List.of(), ToolExecutionMode.PARALLEL,
                    QueueMode.ONE_AT_A_TIME, mode,
                    null, null, null, null
            );
            Agent agent = new Agent(options);
            agent.followUp(UserMessage.text("follow one", 2L));
            agent.followUp(UserMessage.text("follow two", 3L));
            ArrayNode events = MAPPER.createArrayNode();
            agent.subscribe((event, cancellation) -> {
                events.add(normalizeEvent(event));
                return CompletableFuture.completedFuture(null);
            });

            agent.prompt("start").toCompletableFuture().join();

            String scenarioName = mode == QueueMode.ALL
                    ? "followUpAll"
                    : "followUpOneAtATime";
            JsonNode expected = fixture().path("scenarios").path(scenarioName);
            assertEquals(expected.path("events"), events, scenarioName);
            assertEquals(
                    expected.path("messages"),
                    normalizeMessages(agent.state().messages()),
                    scenarioName
            );
            ArrayNode contexts = MAPPER.createArrayNode();
            stream.contexts().forEach(context -> {
                ArrayNode users = contexts.addArray();
                context.messages().stream()
                        .filter(UserMessage.class::isInstance)
                        .map(UserMessage.class::cast)
                        .map(user -> user.content().stream()
                                .filter(TextContent.class::isInstance)
                                .map(TextContent.class::cast)
                                .map(TextContent::text)
                                .reduce((left, right) -> left + "\n" + right)
                                .orElse(""))
                        .forEach(users::add);
            });
            assertEquals(expected.path("invocationContexts"), contexts, scenarioName);
        }
    }

    @Test
    void afterToolTerminationMatchesTheInstalledTypeScriptUpstream() throws Exception {
        AgentTool tool = new AgentTool() {
            private final ToolDefinition definition = new ToolDefinition(
                    "terminator", "terminator",
                    Map.of("type", "object", "properties", Map.of(), "additionalProperties", false)
            );

            @Override
            public ToolDefinition definition() {
                return definition;
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("terminated")), Map.of()
                ));
            }
        };
        AssistantMessage call = assistant(List.of(
                new ToolCallContent("call-terminate", "terminator", Map.of())
        ), StopReason.TOOL_USE);
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(call)
        ));
        AgentOptions options = new AgentOptions(
                "", MODEL, "off", null, stream,
                null, null, null, List.of(tool), ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                null,
                (toolCall, arguments, result, error, context, cancellation) ->
                        CompletableFuture.completedFuture(new AfterToolCallResult(
                                null, null, null, null, true
                        )),
                null, null
        );
        Agent agent = new Agent(options);
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("terminate").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("afterToolTermination");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
        assertEquals(expected.path("invocations").asInt(), stream.invocationCount());
    }

    @Test
    void steeringMatchesTheInstalledTypeScriptUpstream() throws Exception {
        AtomicReference<Agent> agentReference = new AtomicReference<>();
        AgentTool tool = new AgentTool() {
            private final ToolDefinition definition = new ToolDefinition(
                    "steerer", "steerer",
                    Map.of("type", "object", "properties", Map.of(), "additionalProperties", false)
            );

            @Override
            public ToolDefinition definition() {
                return definition;
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                agentReference.get().steer(UserMessage.text("change direction", 2L));
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("tool result")), Map.of()
                ));
            }
        };
        AssistantMessage call = assistant(List.of(
                new ToolCallContent("call-steer", "steerer", Map.of())
        ), StopReason.TOOL_USE);
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(call)),
                List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("redirected")), StopReason.STOP
                )))
        ));
        Agent agent = new Agent(new AgentOptions("", MODEL, stream, List.of(tool)));
        agentReference.set(agent);
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("start").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("steering");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
    }

    @Test
    void abortMatchesTheInstalledTypeScriptUpstream() throws Exception {
        AssistantMessage partial = assistant(List.of(), StopReason.PENDING);
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Start(partial),
                new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("too late")), StopReason.STOP
                ))
        ));
        Agent agent = new Agent(new AgentOptions("", MODEL, stream));
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            if (event instanceof AgentEvent.MessageStart start
                    && start.message() instanceof AssistantMessage) {
                agent.abort();
            }
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("abort me").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("abort");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
    }

    @Test
    void providerErrorMatchesTheInstalledTypeScriptUpstream() throws Exception {
        AssistantMessage failure = new AssistantMessage(
                List.of(), MODEL.api(), MODEL.provider(), MODEL.id(), Usage.ZERO,
                StopReason.ERROR, "provider failed", 1L
        );
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Error(failure)
        ));
        Agent agent = new Agent(new AgentOptions("", MODEL, stream));
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("fail").toCompletableFuture().join();

        JsonNode expected = fixture().path("scenarios").path("providerError");
        assertEquals(expected.path("events"), events);
        assertEquals(expected.path("messages"), normalizeMessages(agent.state().messages()));
    }

    @Test
    void synchronousToolHookFailuresMatchTheInstalledTypeScriptUpstream() throws Exception {
        JsonNode scenarios = fixture().path("scenarios");
        assertEquals(scenarios.path("beforeHookFailure"), runHookFailure(true));
        assertEquals(scenarios.path("afterHookFailure"), runHookFailure(false));
    }

    private static JsonNode runHookFailure(boolean beforeFailure) {
        String kind = beforeFailure ? "before" : "after";
        AtomicInteger executions = new AtomicInteger();
        AgentTool tool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition(
                        "hooked", "hooked",
                        Map.of("type", "object", "properties", Map.of(),
                                "additionalProperties", false)
                );
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("executed")), Map.of()
                ));
            }
        };
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new ToolCallContent(
                                "call-" + kind, "hooked", Map.of()
                        )), StopReason.TOOL_USE
                ))),
                List.of(new AssistantStreamEvent.Done(assistant(
                        List.of(new TextContent("recovered")), StopReason.STOP
                )))
        ));
        BeforeToolCall before = beforeFailure
                ? (call, arguments, messages, cancellation) -> {
                    throw new IllegalStateException("before failed");
                }
                : null;
        AfterToolCall after = beforeFailure
                ? null
                : (call, arguments, result, error, messages, cancellation) -> {
                    throw new IllegalStateException("after failed");
                };
        Agent agent = new Agent(new AgentOptions(
                "", MODEL, "off", null, stream,
                null, null, null, List.of(tool), ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                before, after, null, null
        ));
        ArrayNode events = MAPPER.createArrayNode();
        agent.subscribe((event, cancellation) -> {
            events.add(normalizeEvent(event));
            return CompletableFuture.completedFuture(null);
        });
        agent.prompt(kind).toCompletableFuture().join();
        ToolResultMessage result = agent.state().messages().stream()
                .filter(ToolResultMessage.class::isInstance)
                .map(ToolResultMessage.class::cast)
                .findFirst().orElseThrow();

        ObjectNode actual = MAPPER.createObjectNode();
        actual.set("result", normalizeMessage(result));
        actual.put("executions", executions.get());
        actual.put("invocations", stream.invocationCount());
        actual.set("events", events);
        return actual;
    }

    private static JsonNode fixture() throws Exception {
        Path fixture = Path.of(
                System.getProperty("pi.compatFixtures"),
                "agent-core-0.84.2.json"
        );
        return MAPPER.readTree(fixture.toFile());
    }

    private static ObjectNode normalizeEvent(AgentEvent event) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", eventType(event));
        AgentMessage message = null;
        if (event instanceof AgentEvent.MessageStart start) message = start.message();
        if (event instanceof AgentEvent.MessageUpdate update) message = update.message();
        if (event instanceof AgentEvent.MessageEnd end) message = end.message();
        if (message != null) node.put("role", role(message));
        if (event instanceof AgentEvent.MessageUpdate update) {
            node.put("deltaType", deltaType(update.streamEvent()));
        }
        if (event instanceof AgentEvent.ToolExecutionStart start) {
            node.put("toolName", start.toolName());
            node.put("toolCallId", start.toolCallId());
        }
        if (event instanceof AgentEvent.ToolExecutionUpdate update) {
            node.put("toolName", update.toolName());
            node.put("toolCallId", update.toolCallId());
        }
        if (event instanceof AgentEvent.ToolExecutionEnd end) {
            node.put("toolName", end.toolName());
            node.put("toolCallId", end.toolCallId());
            node.put("isError", end.error());
        }
        if (event instanceof AgentEvent.TurnEnd end) {
            node.put("role", role(end.message()));
            ArrayNode results = node.putArray("toolResults");
            end.toolResults().forEach(result -> results.add(result.toolName()));
        }
        return node;
    }

    private static String eventType(AgentEvent event) {
        if (event instanceof AgentEvent.AgentStart) return "agent_start";
        if (event instanceof AgentEvent.AgentEnd) return "agent_end";
        if (event instanceof AgentEvent.TurnStart) return "turn_start";
        if (event instanceof AgentEvent.TurnEnd) return "turn_end";
        if (event instanceof AgentEvent.MessageStart) return "message_start";
        if (event instanceof AgentEvent.MessageUpdate) return "message_update";
        if (event instanceof AgentEvent.MessageEnd) return "message_end";
        if (event instanceof AgentEvent.ToolExecutionStart) return "tool_execution_start";
        if (event instanceof AgentEvent.ToolExecutionUpdate) return "tool_execution_update";
        if (event instanceof AgentEvent.ToolExecutionEnd) return "tool_execution_end";
        throw new IllegalArgumentException("Unknown event " + event);
    }

    private static String deltaType(AssistantStreamEvent event) {
        ContentKind kind;
        String suffix;
        if (event instanceof AssistantStreamEvent.ContentStart start) {
            kind = start.kind();
            suffix = "_start";
        } else if (event instanceof AssistantStreamEvent.ContentDelta delta) {
            kind = delta.kind();
            suffix = "_delta";
        } else if (event instanceof AssistantStreamEvent.ContentEnd end) {
            kind = end.kind();
            suffix = "_end";
        } else {
            throw new IllegalArgumentException("Not a delta event " + event);
        }
        String prefix = switch (kind) {
            case TEXT -> "text";
            case THINKING -> "thinking";
            case TOOL_CALL -> "toolcall";
        };
        return prefix + suffix;
    }

    private static ArrayNode normalizeMessages(List<AgentMessage> messages) {
        ArrayNode output = MAPPER.createArrayNode();
        messages.forEach(message -> output.add(normalizeMessage(message)));
        return output;
    }

    private static ObjectNode normalizeMessage(AgentMessage message) {
        ObjectNode node = MAPPER.createObjectNode();
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
                ObjectNode item = content.addObject();
                if (block instanceof TextContent text) {
                    item.put("type", "text");
                    item.put("text", text.text());
                } else if (block instanceof ThinkingContent thinking) {
                    item.put("type", "thinking");
                    item.put("thinking", thinking.thinking());
                } else if (block instanceof ToolCallContent toolCall) {
                    item.put("type", "toolCall");
                    item.put("id", toolCall.id());
                    item.put("name", toolCall.name());
                    item.set("arguments", MAPPER.valueToTree(toolCall.arguments()));
                } else if (block instanceof ImageContent) {
                    throw new IllegalArgumentException("Fixture does not normalize images");
                }
            });
        } else if (message instanceof ToolResultMessage result) {
            node.put("toolCallId", result.toolCallId());
            node.put("toolName", result.toolName());
            node.put("isError", result.error());
            node.put("text", result.content().stream()
                    .filter(TextContent.class::isInstance)
                    .map(TextContent.class::cast)
                    .map(TextContent::text)
                    .reduce((left, right) -> left + "\n" + right)
                    .orElse(""));
        }
        return node;
    }

    private static String role(AgentMessage message) {
        if (message instanceof UserMessage) return "user";
        if (message instanceof AssistantMessage) return "assistant";
        if (message instanceof ToolResultMessage) return "toolResult";
        return "custom";
    }

    private static String stopReason(StopReason reason) {
        return switch (reason) {
            case PENDING -> "pending";
            case STOP -> "stop";
            case LENGTH -> "length";
            case TOOL_USE -> "toolUse";
            case ERROR -> "error";
            case ABORTED -> "aborted";
        };
    }

    private static AssistantMessage assistant(
            List<? extends io.github.idoly.pi.ai.ContentBlock> content,
            StopReason reason
    ) {
        return new AssistantMessage(
                new ArrayList<>(content), MODEL.api(), MODEL.provider(), MODEL.id(),
                Usage.ZERO, reason, null, 1L
        );
    }

    private static final class ControlledTool implements AgentTool {
        private final ToolDefinition definition;
        private final CompletableFuture<AgentToolResult> result = new CompletableFuture<>();

        private ControlledTool(String name) {
            definition = new ToolDefinition(
                    name, name,
                    Map.of("type", "object", "properties", Map.of(), "additionalProperties", false)
            );
        }

        @Override
        public ToolDefinition definition() {
            return definition;
        }

        @Override
        public CompletableFuture<AgentToolResult> execute(
                String toolCallId,
                Map<String, Object> arguments,
                io.github.idoly.pi.ai.CancellationSignal cancellation,
                java.util.function.Consumer<AgentToolResult> onUpdate
        ) {
            return result;
        }

        private void complete(String text) {
            result.complete(new AgentToolResult(List.of(new TextContent(text)), Map.of()));
        }
    }
}
