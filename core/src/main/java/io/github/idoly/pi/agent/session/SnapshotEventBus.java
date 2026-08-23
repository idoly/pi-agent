package io.github.idoly.pi.agent.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class SnapshotEventBus<E> {
    static final int DEFAULT_BUFFER_CAPACITY = 1024;
    private final CopyOnWriteArrayList<Consumer<E>> listeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<WatchState<E>> watchers =
            new CopyOnWriteArrayList<>();

    AutoCloseable onEvent(Consumer<E> listener) {
        Objects.requireNonNull(listener, "listener");
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    void emit(E event) {
        Objects.requireNonNull(event, "event");
        listeners.forEach(listener -> deliver(listener, event));
        watchers.forEach(watcher -> watcher.receive(event));
    }

    <T> WatchHandle<T, E> watch(Supplier<T> captureSnapshot) {
        return watch(captureSnapshot, DEFAULT_BUFFER_CAPACITY);
    }

    <T> WatchHandle<T, E> watch(
            Supplier<T> captureSnapshot,
            int maxBufferedEvents
    ) {
        Objects.requireNonNull(captureSnapshot, "captureSnapshot");
        if (maxBufferedEvents < 1) {
            throw new IllegalArgumentException(
                    "maxBufferedEvents must be positive"
            );
        }
        WatchState<E> state = new WatchState<>(maxBufferedEvents);
        watchers.add(state);
        T snapshot;
        try {
            snapshot = captureSnapshot.get();
        } catch (Throwable failure) {
            watchers.remove(state);
            throw failure;
        }
        return new WatchHandle<>(snapshot, state, () -> watchers.remove(state));
    }

    private static <E> void deliver(Consumer<E> listener, E event) {
        try {
            listener.accept(event);
        } catch (RuntimeException ignored) {
            // Observation is passive and cannot alter durable outcomes.
        }
    }

    static final class WatchHandle<T, E> implements AutoCloseable {
        private final T snapshot;
        private final WatchState<E> state;
        private final Runnable remove;

        private WatchHandle(T snapshot, WatchState<E> state, Runnable remove) {
            this.snapshot = snapshot;
            this.state = state;
            this.remove = remove;
        }

        T snapshot() {
            return snapshot;
        }

        void start(Consumer<E> listener) {
            try {
                state.start(listener);
            } catch (SessionWatchOverflowException failure) {
                remove.run();
                throw failure;
            }
        }

        int bufferedEvents() {
            return state.bufferedEvents();
        }

        boolean overflowed() {
            return state.overflowed();
        }

        long droppedEvents() {
            return state.droppedEvents();
        }

        @Override
        public void close() {
            remove.run();
            state.unsubscribe();
        }
    }

    private static final class WatchState<E> {
        private final ArrayList<E> buffered = new ArrayList<>();
        private final int capacity;
        private Consumer<E> listener;
        private boolean closed;
        private boolean overflowed;
        private long droppedEvents;

        private WatchState(int capacity) {
            this.capacity = capacity;
        }

        void receive(E event) {
            Consumer<E> current;
            synchronized (this) {
                if (closed) return;
                current = listener;
                if (current == null) {
                    if (overflowed || buffered.size() == capacity) {
                        overflowed = true;
                        droppedEvents++;
                        buffered.clear();
                    } else {
                        buffered.add(event);
                    }
                    return;
                }
            }
            deliver(current, event);
        }

        void start(Consumer<E> next) {
            Objects.requireNonNull(next, "listener");
            while (true) {
                List<E> pending;
                synchronized (this) {
                    if (closed) return;
                    if (overflowed) {
                        closed = true;
                        throw new SessionWatchOverflowException(
                                capacity, droppedEvents
                        );
                    }
                    if (listener != null) {
                        throw new IllegalStateException("Watch listener already started");
                    }
                    if (buffered.isEmpty()) {
                        listener = next;
                        return;
                    }
                    pending = List.copyOf(buffered);
                    buffered.clear();
                }
                pending.forEach(event -> deliver(next, event));
            }
        }

        synchronized int bufferedEvents() {
            return buffered.size();
        }

        synchronized boolean overflowed() {
            return overflowed;
        }

        synchronized long droppedEvents() {
            return droppedEvents;
        }

        synchronized void unsubscribe() {
            closed = true;
            buffered.clear();
            listener = null;
        }
    }
}
