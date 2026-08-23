package io.github.idoly.pi.testkit;

import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.StreamOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/** Deterministic, demand-aware stream used by Agent compatibility tests. */
public final class ScriptedModelStream implements ModelStream {
    private final List<List<AssistantStreamEvent>> scripts;
    private final AtomicInteger invocation = new AtomicInteger();
    private final List<ModelContext> contexts = new ArrayList<>();
    private final List<Model> models = new ArrayList<>();
    private final List<StreamOptions> options = new ArrayList<>();

    public ScriptedModelStream(List<AssistantStreamEvent> events) {
        this(List.of(List.copyOf(events)), true);
    }

    private ScriptedModelStream(List<List<AssistantStreamEvent>> scripts, boolean ignored) {
        this.scripts = scripts.stream().map(List::copyOf).toList();
    }

    public static ScriptedModelStream turns(List<List<AssistantStreamEvent>> scripts) {
        return new ScriptedModelStream(scripts, true);
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        int index = invocation.getAndIncrement();
        synchronized (contexts) {
            contexts.add(context);
            models.add(model);
            this.options.add(options);
        }
        if (index >= scripts.size()) {
            throw new IllegalStateException("No scripted model turn at index " + index);
        }
        List<AssistantStreamEvent> events = scripts.get(index);
        return subscriber -> subscriber.onSubscribe(new ScriptedSubscription(subscriber, events));
    }

    public ModelContext lastContext() {
        synchronized (contexts) {
            return contexts.isEmpty() ? null : contexts.get(contexts.size() - 1);
        }
    }

    public List<ModelContext> contexts() {
        synchronized (contexts) {
            return List.copyOf(contexts);
        }
    }

    public List<Model> models() {
        synchronized (contexts) {
            return List.copyOf(models);
        }
    }

    public List<StreamOptions> options() {
        synchronized (contexts) {
            return List.copyOf(options);
        }
    }

    public int invocationCount() {
        return invocation.get();
    }

    private static final class ScriptedSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super AssistantStreamEvent> subscriber;
        private final List<AssistantStreamEvent> events;
        private int index;
        private boolean cancelled;
        private boolean completed;

        private ScriptedSubscription(
                Flow.Subscriber<? super AssistantStreamEvent> subscriber,
                List<AssistantStreamEvent> events
        ) {
            this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
            this.events = events;
        }

        @Override
        public synchronized void request(long count) {
            if (cancelled || completed) {
                return;
            }
            if (count <= 0) {
                cancelled = true;
                subscriber.onError(new IllegalArgumentException("Demand must be positive"));
                return;
            }
            long remaining = count;
            while (remaining-- > 0 && index < events.size() && !cancelled) {
                subscriber.onNext(events.get(index++));
            }
            if (index == events.size() && !cancelled && !completed) {
                completed = true;
                subscriber.onComplete();
            }
        }

        @Override
        public synchronized void cancel() {
            cancelled = true;
        }
    }
}
