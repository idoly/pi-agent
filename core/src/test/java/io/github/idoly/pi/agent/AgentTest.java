package io.github.idoly.pi.agent;

import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentKind;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentTest {
    @Test
    void emitsUpstreamCompatibleSequenceForTextTurn() {
        AssistantMessage empty = ScriptedMessages.assistant(null, StopReason.PENDING);
        AssistantMessage partial = ScriptedMessages.assistant("Hello", StopReason.PENDING);
        AssistantMessage complete = ScriptedMessages.assistant("Hello", StopReason.STOP);
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Start(empty),
                new AssistantStreamEvent.ContentStart(ContentKind.TEXT, 0, empty),
                new AssistantStreamEvent.ContentDelta(ContentKind.TEXT, 0, "Hello", partial),
                new AssistantStreamEvent.ContentEnd(ContentKind.TEXT, 0, partial),
                new AssistantStreamEvent.Done(complete)
        ));
        Agent agent = new Agent(new AgentOptions("Be useful", ScriptedMessages.model(), stream));
        List<String> events = new ArrayList<>();
        agent.subscribe((event, cancellation) -> {
            events.add(eventName(event));
            return CompletableFuture.completedFuture(null);
        });

        agent.prompt("Hi").toCompletableFuture().join();

        assertEquals(List.of(
                "agent_start", "turn_start",
                "message_start:user", "message_end:user",
                "message_start:assistant",
                "message_update", "message_update", "message_update",
                "message_end:assistant", "turn_end", "agent_end"
        ), events);
        assertEquals(2, agent.state().messages().size());
        assertInstanceOf(UserMessage.class, agent.state().messages().get(0));
        AssistantMessage saved = assertInstanceOf(AssistantMessage.class, agent.state().messages().get(1));
        assertEquals("Hello", ((TextContent) saved.content().get(0)).text());
        assertEquals(1, stream.lastContext().messages().size());
        assertFalse(agent.state().streaming());
        assertNull(agent.state().streamingMessage());
    }

    @Test
    void agentEndListenerIsPartOfRunSettlement() {
        AssistantMessage complete = ScriptedMessages.assistant("done", StopReason.STOP);
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(),
                new ScriptedModelStream(List.of(new AssistantStreamEvent.Done(complete)))
        ));
        CompletableFuture<Void> release = new CompletableFuture<>();
        agent.subscribe((event, cancellation) ->
                event instanceof AgentEvent.AgentEnd ? release : CompletableFuture.completedFuture(null)
        );

        CompletableFuture<Void> prompt = agent.prompt("work").toCompletableFuture();

        assertFalse(prompt.isDone());
        assertTrue(agent.state().streaming());
        assertFalse(agent.waitForIdle().toCompletableFuture().isDone());

        release.complete(null);
        prompt.join();
        assertFalse(agent.state().streaming());
    }

    @Test
    void rejectsOverlappingPrompt() {
        AssistantMessage complete = ScriptedMessages.assistant("done", StopReason.STOP);
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(),
                new ScriptedModelStream(List.of(new AssistantStreamEvent.Done(complete)))
        ));
        CompletableFuture<Void> release = new CompletableFuture<>();
        agent.subscribe((event, cancellation) ->
                event instanceof AgentEvent.AgentEnd ? release : CompletableFuture.completedFuture(null)
        );

        CompletableFuture<Void> first = agent.prompt("first").toCompletableFuture();
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> agent.prompt("second").toCompletableFuture().join()
        );
        assertInstanceOf(IllegalStateException.class, failure.getCause());

        release.complete(null);
        first.join();
    }

    @Test
    void providerErrorIsACompletedAssistantTurn() {
        AssistantMessage failed = ScriptedMessages.error("quota exceeded");
        Agent agent = new Agent(new AgentOptions(
                "", ScriptedMessages.model(),
                new ScriptedModelStream(List.of(new AssistantStreamEvent.Error(failed)))
        ));

        agent.prompt("work").toCompletableFuture().join();

        AssistantMessage saved = assertInstanceOf(
                AssistantMessage.class,
                agent.state().messages().get(1)
        );
        assertEquals(StopReason.ERROR, saved.stopReason());
        assertEquals("quota exceeded", agent.state().errorMessage());
    }

    private static String eventName(AgentEvent event) {
        if (event instanceof AgentEvent.AgentStart) return "agent_start";
        if (event instanceof AgentEvent.AgentEnd) return "agent_end";
        if (event instanceof AgentEvent.TurnStart) return "turn_start";
        if (event instanceof AgentEvent.TurnEnd) return "turn_end";
        if (event instanceof AgentEvent.MessageUpdate) return "message_update";
        if (event instanceof AgentEvent.MessageStart start) {
            return "message_start:" + role(start.message());
        }
        if (event instanceof AgentEvent.MessageEnd end) {
            return "message_end:" + role(end.message());
        }
        throw new AssertionError("Unexpected event: " + event);
    }

    private static String role(Object message) {
        if (message instanceof UserMessage) return "user";
        if (message instanceof AssistantMessage) return "assistant";
        return "other";
    }
}
