package io.github.idoly.pi.ai;

public interface CancellationSignal {
    boolean isCancelled();

    void throwIfCancelled();

    AutoCloseable onCancel(Runnable callback);
}
