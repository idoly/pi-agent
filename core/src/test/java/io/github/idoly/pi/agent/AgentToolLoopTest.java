package io.github.idoly.pi.agent;

import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentToolLoopTest {
    @Test
    void parallelToolsFinishByCompletionOrderButPersistBySourceOrder() {
        ControlledTool first = new ControlledTool("first", ToolExecutionMode.PARALLEL);
        ControlledTool second = new ControlledTool("second", ToolExecutionMode.PARALLEL);
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("call-1", "first", Map.of()),
                        new ToolCallContent("call-2", "second", Map.of())
                ))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("finished", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(), stream, List.of(first, second)
        ));
        List<String> toolEnds = new CopyOnWriteArrayList<>();
        agent.subscribe((event, cancellation) -> {
            if (event instanceof AgentEvent.ToolExecutionEnd end) {
                toolEnds.add(end.toolName());
            }
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> run = agent.prompt("run tools").toCompletableFuture();
        assertTrue(first.started.isDone());
        assertTrue(second.started.isDone());
        assertEquals(2, agent.state().pendingToolCalls().size());

        second.complete("second result");
        assertEquals(List.of("second"), toolEnds);
        first.complete("first result");
        run.join();

        assertEquals(List.of("second", "first"), toolEnds);
        List<ToolResultMessage> persisted = agent.state().messages().stream()
                .filter(ToolResultMessage.class::isInstance)
                .map(ToolResultMessage.class::cast)
                .toList();
        assertEquals(List.of("first", "second"), persisted.stream().map(ToolResultMessage::toolName).toList());
        assertEquals(4, stream.contexts().get(1).messages().size());
        assertFalse(agent.state().streaming());
    }

    @Test
    void persistsToolNamesAddedByExecutionResults() {
        AgentTool loader = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition(
                        "loader", "Loads tools", Map.of("type", "object")
                );
            }

            @Override
            public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("loaded")), Map.of(), null,
                        List.of("fetch", "read"), false
                ));
            }
        };
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("call", "loader", Map.of())
                ))),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("finished", StopReason.STOP)
                ))
        ));
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(), stream, List.of(loader)
        ));

        agent.prompt("load").toCompletableFuture().join();

        ToolResultMessage result = agent.state().messages().stream()
                .filter(ToolResultMessage.class::isInstance)
                .map(ToolResultMessage.class::cast)
                .findFirst().orElseThrow();
        assertEquals(List.of("fetch", "read"), result.addedToolNames());
    }

    @Test
    void sequentialToolForcesTheWholeBatchToRunOneAtATime() {
        SequentialControlledTool tool = new SequentialControlledTool();
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("call-1", "sequential", Map.of("index", 1)),
                        new ToolCallContent("call-2", "sequential", Map.of("index", 2))
                ))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("finished", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(), stream, List.of(tool)
        ));

        CompletableFuture<Void> run = agent.prompt("run sequentially").toCompletableFuture();
        assertEquals(1, tool.invocations.get());

        tool.results.get(0).complete(result("one"));
        assertEquals(2, tool.invocations.get());
        tool.results.get(1).complete(result("two"));
        run.join();
    }

    @Test
    void steeringIsInjectedBeforeTheNextModelTurn() {
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("first", StopReason.STOP))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("second", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream));
        AtomicBoolean queued = new AtomicBoolean();
        agent.subscribe((event, cancellation) -> {
            if (event instanceof AgentEvent.TurnEnd && queued.compareAndSet(false, true)) {
                agent.steer(UserMessage.text("change direction", 2L));
            }
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("start").toCompletableFuture().join();

        assertEquals(2, stream.contexts().size());
        List<io.github.idoly.pi.ai.Message> secondContext = stream.contexts().get(1).messages();
        UserMessage steering = assertInstanceOf(UserMessage.class, secondContext.get(2));
        assertEquals("change direction", ((TextContent) steering.content().get(0)).text());
    }

    @Test
    void followUpsDrainOneAtATimeAfterTheAgentWouldSettle() {
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("first", StopReason.STOP))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("second", StopReason.STOP))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("third", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream));
        AtomicBoolean queued = new AtomicBoolean();
        agent.subscribe((event, cancellation) -> {
            if (event instanceof AgentEvent.TurnEnd && queued.compareAndSet(false, true)) {
                agent.followUp(UserMessage.text("follow one", 2L));
                agent.followUp(UserMessage.text("follow two", 3L));
            }
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("start").toCompletableFuture().join();

        assertEquals(3, stream.contexts().size());
        assertFalse(agent.hasQueuedMessages());
        List<String> userTexts = agent.state().messages().stream()
                .filter(UserMessage.class::isInstance)
                .map(UserMessage.class::cast)
                .map(message -> ((TextContent) message.content().get(0)).text())
                .toList();
        assertEquals(List.of("start", "follow one", "follow two"), userTexts);
    }

    @Test
    void abortCancelsTheStreamAndPersistsAnAbortedAssistantMessage() {
        AssistantMessage partial = ScriptedMessages.assistant(null, StopReason.PENDING);
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Start(partial),
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("too late", StopReason.STOP))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream));
        agent.subscribe((event, cancellation) -> {
            if (event instanceof AgentEvent.MessageStart start
                    && start.message() instanceof AssistantMessage) {
                agent.abort();
            }
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("abort me").toCompletableFuture().join();

        AssistantMessage saved = assertInstanceOf(
                AssistantMessage.class,
                agent.state().messages().get(1)
        );
        assertEquals(StopReason.ABORTED, saved.stopReason());
        assertFalse(agent.state().streaming());
    }

    @Test
    void missingToolEndsDuringParallelPreflightBeforeTheNextToolStarts() {
        ControlledTool existing = new ControlledTool("existing", ToolExecutionMode.PARALLEL);
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("missing-id", "missing", Map.of()),
                        new ToolCallContent("existing-id", "existing", Map.of())
                ))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("finished", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(), stream, List.of(existing)
        ));
        List<String> lifecycle = new CopyOnWriteArrayList<>();
        agent.subscribe((event, cancellation) -> {
            if (event instanceof AgentEvent.ToolExecutionStart start) {
                lifecycle.add("start:" + start.toolName());
            } else if (event instanceof AgentEvent.ToolExecutionEnd end) {
                lifecycle.add("end:" + end.toolName());
            }
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> run = agent.prompt("run").toCompletableFuture();
        assertEquals(List.of("start:missing", "end:missing", "start:existing"), lifecycle);
        existing.complete("ok");
        run.join();
    }

    @Test
    void ignoresToolUpdatesAfterExecutionSettles() {
        LateUpdatingTool tool = new LateUpdatingTool();
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("late-id", "late", Map.of())
                ))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("finished", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream, List.of(tool)));
        AtomicInteger updates = new AtomicInteger();
        agent.subscribe((event, cancellation) -> {
            if (event instanceof AgentEvent.ToolExecutionUpdate) updates.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> run = agent.prompt("run").toCompletableFuture();
        tool.result.complete(result("done"));
        tool.sendLateUpdate();
        run.join();

        assertEquals(0, updates.get());
    }

    @Test
    void lengthStopNeverExecutesPotentiallyTruncatedToolArguments() {
        AtomicInteger executions = new AtomicInteger();
        AgentTool tool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("dangerous", "dangerous", Map.of());
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(result("should not run"));
            }
        };
        AssistantMessage truncated = new AssistantMessage(
                List.of(new ToolCallContent("danger-id", "dangerous", Map.of("path", "/"))),
                "test-api", "test-provider", "test-model", Usage.ZERO,
                StopReason.LENGTH, null, 1_700_000_000_000L
        );
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(truncated)),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("recovered", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream, List.of(tool)));

        agent.prompt("run").toCompletableFuture().join();

        assertEquals(0, executions.get());
        ToolResultMessage failed = agent.state().messages().stream()
                .filter(ToolResultMessage.class::isInstance)
                .map(ToolResultMessage.class::cast)
                .findFirst().orElseThrow();
        assertTrue(failed.error());
        assertTrue(((TextContent) failed.content().get(0)).text().contains("may be truncated"));
    }

    @Test
    void synchronousBeforeAndAfterHookFailuresBecomeToolErrors() {
        AtomicInteger executions = new AtomicInteger();
        AgentTool tool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("hooked", "hooked", Map.of());
            }

            @Override
            public CompletableFuture<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(result("executed"));
            }
        };

        ScriptedModelStream beforeStream = toolThenRecoveryStream();
        BeforeToolCall before = (call, arguments, messages, cancellation) -> {
            throw new IllegalStateException("before failed");
        };
        Agent beforeAgent = new Agent(hookOptions(beforeStream, tool, before, null));
        beforeAgent.prompt("before").toCompletableFuture().join();
        ToolResultMessage beforeFailure = firstToolResult(beforeAgent);
        assertTrue(beforeFailure.error());
        assertEquals("before failed", ((TextContent) beforeFailure.content().getFirst()).text());
        assertEquals(0, executions.get());

        ScriptedModelStream afterStream = toolThenRecoveryStream();
        AfterToolCall after = (call, arguments, result, error, messages, cancellation) -> {
            throw new IllegalStateException("after failed");
        };
        Agent afterAgent = new Agent(hookOptions(afterStream, tool, null, after));
        afterAgent.prompt("after").toCompletableFuture().join();
        ToolResultMessage afterFailure = firstToolResult(afterAgent);
        assertTrue(afterFailure.error());
        assertEquals("after failed", ((TextContent) afterFailure.content().getFirst()).text());
        assertEquals(1, executions.get());
    }

    @Test
    void validatesArgumentsAgainstTheAdvertisedJsonSchema() {
        AtomicInteger executions = new AtomicInteger();
        AgentTool typedTool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition(
                        "typed",
                        "typed",
                        Map.of(
                                "type", "object",
                                "properties", Map.of("path", Map.of("type", "string")),
                                "required", List.of("path"),
                                "additionalProperties", false
                        )
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
                return CompletableFuture.completedFuture(result("executed"));
            }
        };
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("typed-id", "typed", Map.of("path", 42))
                ))),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("recovered", StopReason.STOP)))
        ));
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(), stream, List.of(typedTool)
        ));

        agent.prompt("validate").toCompletableFuture().join();

        assertEquals(0, executions.get());
        ToolResultMessage failed = agent.state().messages().stream()
                .filter(ToolResultMessage.class::isInstance)
                .map(ToolResultMessage.class::cast)
                .findFirst().orElseThrow();
        assertTrue(failed.error());
        assertFalse(((TextContent) failed.content().getFirst()).text().isBlank());
    }

    private static ScriptedModelStream toolThenRecoveryStream() {
        return ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage(
                        new ToolCallContent("hook-id", "hooked", Map.of())
                ))),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("recovered", StopReason.STOP)
                ))
        ));
    }

    private static AgentOptions hookOptions(
            ScriptedModelStream stream,
            AgentTool tool,
            BeforeToolCall before,
            AfterToolCall after
    ) {
        return new AgentOptions(
                "", ScriptedMessages.model(), "off", null, stream,
                null, null, null, List.of(tool), ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                before, after, null, null
        );
    }

    private static ToolResultMessage firstToolResult(Agent agent) {
        return agent.state().messages().stream()
                .filter(ToolResultMessage.class::isInstance)
                .map(ToolResultMessage.class::cast)
                .findFirst().orElseThrow();
    }

    private static AssistantMessage toolCallingMessage(ToolCallContent... calls) {
        return new AssistantMessage(
                List.of(calls), "test-api", "test-provider", "test-model",
                Usage.ZERO, StopReason.TOOL_USE, null, 1_700_000_000_000L
        );
    }

    private static AgentToolResult result(String text) {
        return new AgentToolResult(List.of(new TextContent(text)), Map.of());
    }

    private static final class ControlledTool implements AgentTool {
        private final ToolDefinition definition;
        private final ToolExecutionMode mode;
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private final CompletableFuture<AgentToolResult> result = new CompletableFuture<>();

        private ControlledTool(String name, ToolExecutionMode mode) {
            definition = new ToolDefinition(name, name, Map.of());
            this.mode = mode;
        }

        @Override
        public ToolDefinition definition() {
            return definition;
        }

        @Override
        public ToolExecutionMode executionMode() {
            return mode;
        }

        @Override
        public CompletableFuture<AgentToolResult> execute(
                String toolCallId,
                Map<String, Object> arguments,
                io.github.idoly.pi.ai.CancellationSignal cancellation,
                java.util.function.Consumer<AgentToolResult> onUpdate
        ) {
            started.complete(null);
            return result;
        }

        private void complete(String text) {
            result.complete(result(text));
        }
    }

    private static final class LateUpdatingTool implements AgentTool {
        private final CompletableFuture<AgentToolResult> result = new CompletableFuture<>();
        private java.util.function.Consumer<AgentToolResult> onUpdate;

        @Override
        public ToolDefinition definition() {
            return new ToolDefinition("late", "late", Map.of());
        }

        @Override
        public CompletableFuture<AgentToolResult> execute(
                String toolCallId,
                Map<String, Object> arguments,
                io.github.idoly.pi.ai.CancellationSignal cancellation,
                java.util.function.Consumer<AgentToolResult> onUpdate
        ) {
            this.onUpdate = onUpdate;
            return result;
        }

        private void sendLateUpdate() {
            onUpdate.accept(result("late"));
        }
    }

    private static final class SequentialControlledTool implements AgentTool {
        private final AtomicInteger invocations = new AtomicInteger();
        private final List<CompletableFuture<AgentToolResult>> results = new ArrayList<>();

        @Override
        public ToolDefinition definition() {
            return new ToolDefinition("sequential", "sequential", Map.of());
        }

        @Override
        public ToolExecutionMode executionMode() {
            return ToolExecutionMode.SEQUENTIAL;
        }

        @Override
        public CompletableFuture<AgentToolResult> execute(
                String toolCallId,
                Map<String, Object> arguments,
                io.github.idoly.pi.ai.CancellationSignal cancellation,
                java.util.function.Consumer<AgentToolResult> onUpdate
        ) {
            invocations.incrementAndGet();
            CompletableFuture<AgentToolResult> future = new CompletableFuture<>();
            results.add(future);
            return future;
        }
    }
}
