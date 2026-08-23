package io.github.idoly.pi.agent.harness;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class HarnessEventBus {
    private final CopyOnWriteArrayList<Consumer<HarnessEvent.RunStart>> starts =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<HarnessEvent.RunEnd>> ends =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<WatchState<?>> watchers =
            new CopyOnWriteArrayList<>();

    public AutoCloseable onRunStart(Consumer<HarnessEvent.RunStart> listener) {
        Objects.requireNonNull(listener, "listener");
        starts.add(listener);
        return () -> starts.remove(listener);
    }

    public AutoCloseable onRunEnd(Consumer<HarnessEvent.RunEnd> listener) {
        Objects.requireNonNull(listener, "listener");
        ends.add(listener);
        return () -> ends.remove(listener);
    }

    public void emit(HarnessEvent event) {
        Objects.requireNonNull(event, "event");
        if (event instanceof HarnessEvent.RunStart start) {
            starts.forEach(listener -> listener.accept(start));
        } else if (event instanceof HarnessEvent.RunEnd end) {
            ends.forEach(listener -> listener.accept(end));
        }
        watchers.forEach(watcher -> watcher.receive(event));
    }

    public <T> WatchHandle<T> watch(Supplier<T> captureSnapshot) {
        Objects.requireNonNull(captureSnapshot, "captureSnapshot");
        WatchState<T> state = new WatchState<>();
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

    public static final class WatchHandle<T> implements AutoCloseable {
        private final T snapshot;
        private final WatchState<T> state;
        private final Runnable remove;

        private WatchHandle(T snapshot, WatchState<T> state, Runnable remove) {
            this.snapshot = snapshot;
            this.state = state;
            this.remove = remove;
        }

        public T snapshot() { return snapshot; }

        public void start(Consumer<HarnessEvent> listener) {
            state.start(listener);
        }

        @Override
        public void close() {
            remove.run();
            state.unsubscribe();
        }
    }

    private static final class WatchState<T> {
        private final ArrayList<HarnessEvent> buffered = new ArrayList<>();
        private Consumer<HarnessEvent> listener;
        private boolean closed;

        void receive(HarnessEvent event) {
            Consumer<HarnessEvent> current;
            synchronized (this) {
                if (closed) return;
                current = listener;
                if (current == null) {
                    buffered.add(event);
                    return;
                }
            }
            current.accept(event);
        }

        void start(Consumer<HarnessEvent> next) {
            Objects.requireNonNull(next, "listener");
            while (true) {
                List<HarnessEvent> pending;
                synchronized (this) {
                    if (closed) return;
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
                pending.forEach(next);
            }
        }

        synchronized void unsubscribe() {
            closed = true;
            buffered.clear();
            listener = null;
        }
    }
}
