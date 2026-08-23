package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** A running low-level loop: event publisher, final invocation-local messages, and cancellation. */
public final class AgentLoopRun implements Flow.Publisher<AgentEvent> {
    private final Flow.Publisher<AgentEvent> events;
    private final CompletionStage<List<AgentMessage>> result;
    private final Runnable cancel;

    AgentLoopRun(
            Flow.Publisher<AgentEvent> events,
            CompletionStage<List<AgentMessage>> result,
            Runnable cancel
    ) {
        this.events = Objects.requireNonNull(events, "events");
        this.result = Objects.requireNonNull(result, "result");
        this.cancel = Objects.requireNonNull(cancel, "cancel");
    }

    @Override
    public void subscribe(Flow.Subscriber<? super AgentEvent> subscriber) {
        events.subscribe(subscriber);
    }

    public CompletionStage<List<AgentMessage>> result() {
        return result;
    }

    public void cancel() {
        cancel.run();
    }
}
