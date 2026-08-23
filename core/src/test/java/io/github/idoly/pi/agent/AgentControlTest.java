package io.github.idoly.pi.agent;

import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentControlTest {
    @Test
    void continueRunUsesExistingUserContextWithoutAppendingAnotherPrompt() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("continued", StopReason.STOP))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream));
        agent.messages(List.of(UserMessage.text("existing", 1L)));

        agent.continueRun().toCompletableFuture().join();

        assertEquals(2, agent.state().messages().size());
        assertEquals(1, stream.contexts().getFirst().messages().size());
    }

    @Test
    void continueRunDrainsQueuedSteeringAfterAssistantContext() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("continued", StopReason.STOP))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream));
        agent.messages(List.of(ScriptedMessages.assistant("previous", StopReason.STOP)));
        agent.steer(UserMessage.text("steer now", 2L));

        agent.continueRun().toCompletableFuture().join();

        assertEquals(3, agent.state().messages().size());
        assertFalse(agent.hasQueuedMessages());
        assertEquals(2, stream.contexts().getFirst().messages().size());
    }

    @Test
    void resetRequiresIdleAndClearsRuntimeTranscript() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("done", StopReason.STOP))
        ));
        Agent agent = new Agent(new AgentOptions("", ScriptedMessages.model(), stream));
        CompletableFuture<Void> release = new CompletableFuture<>();
        agent.subscribe((event, cancellation) ->
                event instanceof AgentEvent.AgentEnd ? release : CompletableFuture.completedFuture(null)
        );

        CompletableFuture<Void> run = agent.prompt("work").toCompletableFuture();
        assertThrows(IllegalStateException.class, agent::reset);
        release.complete(null);
        run.join();

        agent.followUp(UserMessage.text("queued", 2L));
        agent.reset();
        assertTrue(agent.state().messages().isEmpty());
        assertFalse(agent.hasQueuedMessages());
    }

    @Test
    void contextTransformerChangesProviderContextWithoutChangingTranscript() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("done", StopReason.STOP))
        ));
        ContextTransformer transformer = (messages, cancellation) ->
                CompletableFuture.completedFuture(List.of(messages.getLast()));
        Agent agent = new Agent(options(stream, transformer, null, List.of(), null, null));
        agent.messages(List.of(UserMessage.text("old", 1L)));

        agent.prompt("new").toCompletableFuture().join();

        assertEquals(3, agent.state().messages().size());
        assertEquals(1, stream.contexts().getFirst().messages().size());
        UserMessage sent = (UserMessage) stream.contexts().getFirst().messages().getFirst();
        assertEquals("new", ((TextContent) sent.content().getFirst()).text());
    }

    @Test
    void resolvesApiKeyAsynchronouslyWithoutStartingProviderEarly() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("done", StopReason.STOP))
        ));
        CompletableFuture<String> key = new CompletableFuture<>();
        ApiKeyResolver resolver = provider -> key;
        Agent agent = new Agent(options(stream, null, resolver, List.of(), null, null));

        CompletableFuture<Void> run = agent.prompt("work").toCompletableFuture();
        assertEquals(0, stream.invocationCount());
        assertFalse(run.isDone());

        key.complete("secret");
        run.join();
        assertEquals("secret", stream.options().getFirst().apiKey());
    }

    @Test
    void prepareNextTurnSwitchesModelAndThinkingBeforeToolFollowUp() {
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCallingMessage())),
                List.of(new AssistantStreamEvent.Done(ScriptedMessages.assistant("done", StopReason.STOP)))
        ));
        Model nextModel = new Model(
                "next", "Next", "next-api", "next-provider", "http://localhost",
                true, List.of("text"), 64_000, 4_096
        );
        PrepareNextTurn prepare = (context, cancellation) ->
                CompletableFuture.completedFuture(new NextTurnUpdate(nextModel, "high"));
        Agent agent = new Agent(options(
                stream, null, null, List.of(immediateTool()), prepare, null
        ));

        agent.prompt("work").toCompletableFuture().join();

        assertEquals(List.of("test-model", "next"), stream.models().stream().map(Model::id).toList());
        assertEquals("high", stream.options().get(1).thinkingLevel());
        assertEquals("next", agent.state().model().id());
    }

    @Test
    void shouldStopAfterTurnPreventsAutomaticToolFollowUp() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(toolCallingMessage())
        ));
        ShouldStopAfterTurn stop = (context, cancellation) -> CompletableFuture.completedFuture(true);
        Agent agent = new Agent(options(
                stream, null, null, List.of(immediateTool()), null, stop
        ));

        agent.prompt("work").toCompletableFuture().join();

        assertEquals(1, stream.invocationCount());
        assertEquals(3, agent.state().messages().size());
    }

    private static AgentOptions options(
            ScriptedModelStream stream,
            ContextTransformer transformer,
            ApiKeyResolver resolver,
            List<AgentTool> tools,
            PrepareNextTurn prepare,
            ShouldStopAfterTurn stop
    ) {
        return new AgentOptions(
                "", ScriptedMessages.model(), "off", "session-1", stream,
                null, transformer, resolver, tools, ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                null, null, prepare, stop
        );
    }

    private static AssistantMessage toolCallingMessage() {
        return new AssistantMessage(
                List.of(new ToolCallContent("call-1", "tool", Map.of())),
                "test-api", "test-provider", "test-model",
                Usage.ZERO, StopReason.TOOL_USE, null, 1_700_000_000_000L
        );
    }

    private static AgentTool immediateTool() {
        return new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("tool", "tool", Map.of());
            }

            @Override
            public CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                return CompletableFuture.completedFuture(
                        new AgentToolResult(List.of(new TextContent("result")), Map.of())
                );
            }
        };
    }
}
