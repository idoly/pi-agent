package io.github.idoly.pi.ai;

public interface CancellationSignal {
    CancellationSignal NONE = new CancellationSignal() {
        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void throwIfCancelled() {
        }

        @Override
        public AutoCloseable onCancel(Runnable callback) {
            java.util.Objects.requireNonNull(callback, "callback");
            return () -> { };
        }
    };

    boolean isCancelled();

    void throwIfCancelled();

    AutoCloseable onCancel(Runnable callback);
}
