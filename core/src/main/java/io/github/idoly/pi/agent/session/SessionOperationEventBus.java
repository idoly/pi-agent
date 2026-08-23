package io.github.idoly.pi.agent.session;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** Passive in-process observation of session-level durable operations. */
public final class SessionOperationEventBus {
    private final SnapshotEventBus<SessionOperationEvent> delegate =
            new SnapshotEventBus<>();

    public AutoCloseable onEvent(Consumer<SessionOperationEvent> listener) {
        return delegate.onEvent(listener);
    }

    public void emit(SessionOperationEvent event) {
        delegate.emit(event);
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

    public static final class WatchHandle<T> implements AutoCloseable {
        private final SnapshotEventBus.WatchHandle<T, SessionOperationEvent> delegate;

        private WatchHandle(
                SnapshotEventBus.WatchHandle<T, SessionOperationEvent> delegate
        ) {
            this.delegate = delegate;
        }

        public T snapshot() {
            return delegate.snapshot();
        }

        public void start(Consumer<SessionOperationEvent> listener) {
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
