package io.github.idoly.pi.agent.extension;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** Ordered asynchronous event bus shared by extensions in one runtime. */
public final class ExtensionEventBus {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Listener>> listeners =
            new ConcurrentHashMap<>();

    public AutoCloseable on(String topic, Listener listener) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(listener, "listener");
        listeners.computeIfAbsent(topic, ignored -> new CopyOnWriteArrayList<>())
                .add(listener);
        return () -> listeners.getOrDefault(
                topic, new CopyOnWriteArrayList<>()
        ).remove(listener);
    }

    public CompletionStage<Void> emit(String topic, Object value) {
        List<Listener> snapshot = List.copyOf(listeners.getOrDefault(
                topic, new CopyOnWriteArrayList<>()
        ));
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (Listener listener : snapshot) {
            stage = stage.thenCompose(ignored -> listener.onEvent(topic, value));
        }
        return stage;
    }

    @FunctionalInterface
    public interface Listener {
        CompletionStage<Void> onEvent(String topic, Object value);
    }
}
