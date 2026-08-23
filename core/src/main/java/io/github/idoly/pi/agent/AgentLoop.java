package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.CancellationSignal;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Stateless facade over the core runtime for one prompt or continuation invocation. */
public final class AgentLoop {
    private AgentLoop() {
    }

    public static AgentLoopRun run(
            List<AgentMessage> prompts,
            AgentContext context,
            AgentLoopConfig config
    ) {
        return run(prompts, context, config, null);
    }

    public static AgentLoopRun run(
            List<AgentMessage> prompts,
            AgentContext context,
            AgentLoopConfig config,
            CancellationSignal cancellation
    ) {
        Objects.requireNonNull(prompts, "prompts");
        List<AgentMessage> safePrompts = List.copyOf(prompts);
        safePrompts.forEach(message -> Objects.requireNonNull(message, "prompt message"));
        return start(safePrompts, context, config, cancellation, false);
    }

    public static AgentLoopRun continueRun(
            AgentContext context,
            AgentLoopConfig config
    ) {
        return continueRun(context, config, null);
    }

    public static AgentLoopRun continueRun(
            AgentContext context,
            AgentLoopConfig config,
            CancellationSignal cancellation
    ) {
        Objects.requireNonNull(context, "context");
        if (context.messages().isEmpty()) {
            throw new IllegalArgumentException("Cannot continue: no messages in context");
        }
        AgentMessage last = context.messages().getLast();
        if (last instanceof io.github.idoly.pi.ai.AssistantMessage) {
            throw new IllegalArgumentException("Cannot continue from message role: assistant");
        }
        return start(List.of(), context, config, cancellation, true);
    }

    private static AgentLoopRun start(
            List<AgentMessage> prompts,
            AgentContext context,
            AgentLoopConfig config,
            CancellationSignal cancellation,
            boolean continuation
    ) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(config, "config");
        EventPublisher publisher = new EventPublisher();
        CompletableFuture<List<AgentMessage>> result = new CompletableFuture<>();
        AtomicReference<Agent> active = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean(cancellation != null && cancellation.isCancelled());
        AutoCloseable cancellationRegistration = registerCancellation(
                cancellation,
                () -> {
                    cancelled.set(true);
                    Agent agent = active.get();
                    if (agent != null) {
                        agent.abort();
                    }
                },
                result
        );

        AgentOptions options = new AgentOptions(
                context.systemPrompt(), config.model(), config.thinkingLevel(), config.sessionId(),
                config.modelStream(), config.contextConverter(), config.contextTransformer(),
                config.apiKeyResolver(), context.tools(), config.toolExecution(),
                QueueMode.ALL, QueueMode.ALL, config.beforeToolCall(), config.afterToolCall(),
                config.prepareNextTurn(), config.shouldStopAfterTurn(),
                config.steeringMessages(), config.followUpMessages()
        );
        Agent agent = new Agent(options);
        active.set(agent);
        agent.messages(context.messages());
        agent.subscribe((event, signal) -> {
            publisher.emit(event);
            if (event instanceof AgentEvent.AgentEnd end) {
                result.complete(end.messages());
                publisher.complete();
            }
            return CompletableFuture.completedFuture(null);
        });

        CompletionStage<Void> running = continuation
                ? agent.continueRun()
                : agent.prompt(prompts);
        if (cancelled.get()) {
            agent.abort();
        }
        running.whenComplete((ignored, failure) -> {
            if (failure != null) {
                fail(failure, publisher, result, cancellationRegistration);
            } else {
                closeQuietly(cancellationRegistration);
            }
        });

        return new AgentLoopRun(publisher, result, () -> {
            cancelled.set(true);
            Agent runningAgent = active.get();
            if (runningAgent != null) {
                runningAgent.abort();
            }
        });
    }

    private static AutoCloseable registerCancellation(
            CancellationSignal cancellation,
            Runnable callback,
            CompletableFuture<?> result
    ) {
        if (cancellation == null) {
            return () -> { };
        }
        try {
            return cancellation.onCancel(callback);
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
            return () -> { };
        }
    }

    private static void fail(
            Throwable failure,
            EventPublisher publisher,
            CompletableFuture<List<AgentMessage>> result,
            AutoCloseable cancellationRegistration
    ) {
        Throwable unwrapped = unwrap(failure);
        result.completeExceptionally(unwrapped);
        publisher.fail(unwrapped);
        closeQuietly(cancellationRegistration);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof java.util.concurrent.CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Cancellation registrations are best-effort cleanup only.
        }
    }

    private static final class EventPublisher implements Flow.Publisher<AgentEvent> {
        private final ArrayDeque<AgentEvent> events = new ArrayDeque<>();
        private Flow.Subscriber<? super AgentEvent> subscriber;
        private Subscription subscription;
        private long demand;
        private boolean subscribed;
        private boolean draining;
        private boolean completed;
        private Throwable failure;

        @Override
        public void subscribe(Flow.Subscriber<? super AgentEvent> subscriber) {
            Objects.requireNonNull(subscriber, "subscriber");
            Subscription next;
            synchronized (this) {
                if (subscribed) {
                    subscriber.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long count) {
                        }

                        @Override
                        public void cancel() {
                        }
                    });
                    subscriber.onError(new IllegalStateException(
                            "AgentLoopRun supports one event subscriber"
                    ));
                    return;
                }
                subscribed = true;
                this.subscriber = subscriber;
                subscription = new Subscription();
                next = subscription;
            }
            subscriber.onSubscribe(next);
            drain();
        }

        private void emit(AgentEvent event) {
            synchronized (this) {
                if (completed || failure != null) {
                    return;
                }
                events.addLast(event);
            }
            drain();
        }

        private void complete() {
            synchronized (this) {
                if (failure == null) {
                    completed = true;
                }
            }
            drain();
        }

        private void fail(Throwable failure) {
            synchronized (this) {
                if (!completed && this.failure == null) {
                    this.failure = failure;
                }
            }
            drain();
        }

        private void failDemand(Throwable failure) {
            synchronized (this) {
                if (subscription.cancelled) {
                    return;
                }
                events.clear();
                completed = false;
                this.failure = failure;
            }
            drain();
        }

        private void drain() {
            synchronized (this) {
                if (draining || subscriber == null) {
                    return;
                }
                draining = true;
            }
            while (true) {
                AgentEvent event;
                Flow.Subscriber<? super AgentEvent> target;
                Throwable terminalFailure;
                boolean terminalComplete;
                synchronized (this) {
                    if (subscription.cancelled) {
                        events.clear();
                        draining = false;
                        return;
                    }
                    target = subscriber;
                    if (demand > 0 && !events.isEmpty()) {
                        demand--;
                        event = events.removeFirst();
                        terminalFailure = null;
                        terminalComplete = false;
                    } else {
                        event = null;
                        terminalFailure = events.isEmpty() ? failure : null;
                        terminalComplete = events.isEmpty() && failure == null && completed;
                        if (terminalFailure != null || terminalComplete) {
                            subscription.cancelled = true;
                        }
                        draining = false;
                    }
                }
                if (event != null) {
                    try {
                        target.onNext(event);
                    } catch (Throwable subscriberFailure) {
                        subscription.cancel();
                        return;
                    }
                    continue;
                }
                if (terminalFailure != null) {
                    target.onError(terminalFailure);
                } else if (terminalComplete) {
                    target.onComplete();
                }
                return;
            }
        }

        private final class Subscription implements Flow.Subscription {
            private boolean cancelled;

            @Override
            public void request(long count) {
                if (count <= 0) {
                    failDemand(new IllegalArgumentException("Demand must be positive"));
                    return;
                }
                synchronized (EventPublisher.this) {
                    if (cancelled) {
                        return;
                    }
                    long next = demand + count;
                    demand = next < 0 ? Long.MAX_VALUE : next;
                }
                drain();
            }

            @Override
            public void cancel() {
                synchronized (EventPublisher.this) {
                    cancelled = true;
                    events.clear();
                }
            }
        }
    }
}
