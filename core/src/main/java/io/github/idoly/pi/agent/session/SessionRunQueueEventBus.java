package io.github.idoly.pi.agent.session;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** Session-shared passive observation of committed durable queue changes. */
public final class SessionRunQueueEventBus {
    private final SnapshotEventBus<SessionRunQueueEvent> delegate =
            new SnapshotEventBus<>();

    public AutoCloseable onEvent(Consumer<SessionRunQueueEvent> listener) {
        return delegate.onEvent(listener);
    }

    public <T> WatchHandle<T> watch(Supplier<T> captureSnapshot) {
        return new WatchHandle<>(delegate.watch(captureSnapshot));
    }

    public <T> WatchHandle<T> watch(
            Supplier<T> captureSnapshot,
            int maxBufferedEvents
    ) {
        return new WatchHandle<>(delegate.watch(
                captureSnapshot, maxBufferedEvents
        ));
    }

    void emit(SessionRunQueueEvent event) {
        delegate.emit(event);
    }

    public static final class WatchHandle<T> implements AutoCloseable {
        private final SnapshotEventBus.WatchHandle<T, SessionRunQueueEvent> delegate;

        private WatchHandle(
                SnapshotEventBus.WatchHandle<T, SessionRunQueueEvent> delegate
        ) {
            this.delegate = delegate;
        }

        public T snapshot() {
            return delegate.snapshot();
        }

        public void start(Consumer<SessionRunQueueEvent> listener) {
            delegate.start(listener);
        }

        public int bufferedEvents() {
            return delegate.bufferedEvents();
        }

        public boolean overflowed() {
            return delegate.overflowed();
        }

        public long droppedEvents() {
            return delegate.droppedEvents();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
