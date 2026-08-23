package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentLoopTest {
    @Test
    void returnsOnlyInvocationMessagesAndReplaysEventsWithDemand() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("done", StopReason.STOP))
        ));
        List<AgentMessage> prompts = List.of(
                UserMessage.text("first", 1L), UserMessage.text("second", 2L)
        );
        AgentLoopRun run = AgentLoop.run(
                prompts,
                new AgentContext("system", List.of(), List.of()),
                new AgentLoopConfig(ScriptedMessages.model(), stream)
        );

        List<AgentMessage> result = run.result().toCompletableFuture().join();
        assertEquals(3, result.size());
        assertEquals(prompts, result.subList(0, 2));
        assertInstanceOf(AssistantMessage.class, result.getLast());
        assertEquals(2, stream.lastContext().messages().size());

        CollectingSubscriber subscriber = new CollectingSubscriber();
        run.subscribe(subscriber);
        subscriber.completed.join();
        assertEquals(List.of(
                "agent_start", "turn_start", "message_start:user", "message_end:user",
                "message_start:user", "message_end:user", "message_start:assistant",
                "message_end:assistant", "turn_end", "agent_end"
        ), subscriber.events.stream().map(AgentLoopTest::eventName).toList());
        assertEquals(subscriber.events.size() + 1, subscriber.requests.get());
    }

    @Test
    void continuationDoesNotReturnPreexistingContextMessages() {
        UserMessage existing = UserMessage.text("existing", 1L);
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("continued", StopReason.STOP))
        ));
        AgentContext context = new AgentContext("", List.of(existing), List.of());

        List<AgentMessage> result = AgentLoop.continueRun(
                context, new AgentLoopConfig(ScriptedMessages.model(), stream)
        ).result().toCompletableFuture().join();

        assertEquals(1, result.size());
        assertInstanceOf(AssistantMessage.class, result.getFirst());
        assertEquals(List.of(existing), context.messages());
        assertEquals(List.of(existing), stream.lastContext().messages());
        assertThrows(IllegalArgumentException.class, () -> AgentLoop.continueRun(
                new AgentContext("", List.of(), List.of()),
                new AgentLoopConfig(ScriptedMessages.model(), stream)
        ));
        assertThrows(IllegalArgumentException.class, () -> AgentLoop.continueRun(
                new AgentContext("", List.of(ScriptedMessages.assistant("old", StopReason.STOP)), List.of()),
                new AgentLoopConfig(ScriptedMessages.model(), stream)
        ));
    }

    @Test
    void pollsInitialSteeringAndSettledFollowUpsAtUpstreamDrainPoints() {
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("first answer", StopReason.STOP)
                )),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("second answer", StopReason.STOP)
                ))
        ));
        AtomicInteger steeringCalls = new AtomicInteger();
        AtomicInteger followUpCalls = new AtomicInteger();
        AgentMessageSupplier steering = () -> CompletableFuture.completedFuture(
                steeringCalls.getAndIncrement() == 0
                        ? List.of(UserMessage.text("initial steering", 2L))
                        : List.of()
        );
        AgentMessageSupplier followUps = () -> CompletableFuture.completedFuture(
                followUpCalls.getAndIncrement() == 0
                        ? List.of(UserMessage.text("follow up", 3L))
                        : List.of()
        );
        AgentLoopConfig config = new AgentLoopConfig(
                ScriptedMessages.model(), "off", null, stream, null, null, null,
                ToolExecutionMode.PARALLEL, null, null, null, null, steering, followUps
        );

        List<AgentMessage> result = AgentLoop.run(
                List.of(UserMessage.text("prompt", 1L)),
                new AgentContext("", List.of(), List.of()), config
        ).result().toCompletableFuture().join();

        assertEquals(5, result.size());
        assertEquals(3, steeringCalls.get());
        assertEquals(2, followUpCalls.get());
        assertEquals(2, stream.invocationCount());
        assertEquals(2, stream.contexts().getFirst().messages().size());
        assertEquals(4, stream.contexts().get(1).messages().size());
        UserMessage followUp = assertInstanceOf(
                UserMessage.class, stream.contexts().get(1).messages().getLast()
        );
        assertEquals("follow up", ((TextContent) followUp.content().getFirst()).text());
    }

    @Test
    void terminateStopsAutomaticToolTurnButStillDrainsFollowUps() {
        AssistantMessage toolCall = new AssistantMessage(
                List.of(new ToolCallContent("call-1", "tool", Map.of())),
                "test-api", "test-provider", "test-model", Usage.ZERO,
                StopReason.TOOL_USE, null, 1L
        );
        ScriptedModelStream stream = ScriptedModelStream.turns(List.of(
                List.of(new AssistantStreamEvent.Done(toolCall)),
                List.of(new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("after follow up", StopReason.STOP)
                ))
        ));
        AgentTool tool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("tool", "tool", Map.of());
            }

            @Override
            public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent("tool result")), Map.of()
                ));
            }
        };
        AtomicInteger followUpCalls = new AtomicInteger();
        AgentLoopConfig config = new AgentLoopConfig(
                ScriptedMessages.model(), "off", null, stream, null, null, null,
                ToolExecutionMode.PARALLEL, null,
                (call, arguments, result, error, messages, cancellation) ->
                        CompletableFuture.completedFuture(new AfterToolCallResult(
                                null, null, null, null, true
                        )),
                null, null, null,
                () -> CompletableFuture.completedFuture(
                        followUpCalls.getAndIncrement() == 0
                                ? List.of(UserMessage.text("follow after terminate", 2L))
                                : List.of()
                )
        );

        List<AgentMessage> result = AgentLoop.run(
                List.of(UserMessage.text("prompt", 1L)),
                new AgentContext("", List.of(), List.of(tool)), config
        ).result().toCompletableFuture().join();

        assertEquals(2, stream.invocationCount());
        assertEquals(2, followUpCalls.get());
        assertEquals(5, result.size());
    }

    @Test
    void cancellationCancelsProviderAndCompletesWithAbortedAssistant() {
        AtomicBoolean providerCancelled = new AtomicBoolean();
        ModelStream waiting = (model, context, options) -> subscriber ->
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long count) {
                    }

                    @Override
                    public void cancel() {
                        providerCancelled.set(true);
                    }
                });
        AgentLoopRun run = AgentLoop.run(
                List.of(UserMessage.text("prompt", 1L)),
                new AgentContext("", List.of(), List.of()),
                new AgentLoopConfig(ScriptedMessages.model(), waiting)
        );

        run.cancel();
        List<AgentMessage> result = run.result().toCompletableFuture().join();

        assertTrue(providerCancelled.get());
        AssistantMessage aborted = assertInstanceOf(AssistantMessage.class, result.getLast());
        assertEquals(StopReason.ABORTED, aborted.stopReason());
        CollectingSubscriber subscriber = new CollectingSubscriber();
        run.subscribe(subscriber);
        subscriber.completed.join();
        assertTrue(subscriber.events.getLast() instanceof AgentEvent.AgentEnd);
    }

    @Test
    void callbackFailureFailsResultAndEventPublisherWithoutAgentEnd() {
        ScriptedModelStream stream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(ScriptedMessages.assistant("done", StopReason.STOP))
        ));
        AgentLoopConfig config = new AgentLoopConfig(
                ScriptedMessages.model(), "off", null, stream, null, null, null,
                ToolExecutionMode.PARALLEL, null, null, null, null,
                () -> CompletableFuture.completedFuture(List.of()),
                () -> CompletableFuture.failedFuture(new IllegalStateException("queue failed"))
        );
        AgentLoopRun run = AgentLoop.run(
                List.of(UserMessage.text("prompt", 1L)),
                new AgentContext("", List.of(), List.of()), config
        );
        CollectingSubscriber subscriber = new CollectingSubscriber();
        run.subscribe(subscriber);

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> run.result().toCompletableFuture().join()
        );
        assertEquals("queue failed", failure.getCause().getMessage());
        CompletionException streamFailure = assertThrows(
                CompletionException.class, subscriber.completed::join
        );
        assertEquals("queue failed", streamFailure.getCause().getMessage());
        assertTrue(subscriber.events.stream().noneMatch(AgentEvent.AgentEnd.class::isInstance));
    }

    private static String eventName(AgentEvent event) {
        if (event instanceof AgentEvent.AgentStart) return "agent_start";
        if (event instanceof AgentEvent.AgentEnd) return "agent_end";
        if (event instanceof AgentEvent.TurnStart) return "turn_start";
        if (event instanceof AgentEvent.TurnEnd) return "turn_end";
        if (event instanceof AgentEvent.MessageStart start) {
            return "message_start:" + role(start.message());
        }
        if (event instanceof AgentEvent.MessageEnd end) {
            return "message_end:" + role(end.message());
        }
        if (event instanceof AgentEvent.MessageUpdate) return "message_update";
        throw new AssertionError("Unexpected event: " + event);
    }

    private static String role(AgentMessage message) {
        if (message instanceof UserMessage) return "user";
        if (message instanceof AssistantMessage) return "assistant";
        return "toolResult";
    }

    private static final class CollectingSubscriber implements Flow.Subscriber<AgentEvent> {
        private final List<AgentEvent> events = new ArrayList<>();
        private final CompletableFuture<Void> completed = new CompletableFuture<>();
        private final AtomicInteger requests = new AtomicInteger();
        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            requests.incrementAndGet();
            subscription.request(1);
        }

        @Override
        public void onNext(AgentEvent item) {
            events.add(item);
            requests.incrementAndGet();
            subscription.request(1);
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
