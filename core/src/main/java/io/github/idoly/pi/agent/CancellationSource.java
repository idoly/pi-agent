package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.CancellationSignal;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CancellationSource implements CancellationSignal {
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CopyOnWriteArrayList<Runnable> callbacks = new CopyOnWriteArrayList<>();

    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) {
            return;
        }
        for (Runnable callback : callbacks) {
            callback.run();
        }
        callbacks.clear();
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public void throwIfCancelled() {
        if (isCancelled()) {
            throw new CancellationException("Operation cancelled");
        }
    }

    @Override
    public AutoCloseable onCancel(Runnable callback) {
        if (isCancelled()) {
            callback.run();
            return () -> { };
        }
        callbacks.add(callback);
        if (isCancelled() && callbacks.remove(callback)) {
            callback.run();
        }
        return () -> callbacks.remove(callback);
    }
}
