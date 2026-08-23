package io.github.idoly.pi.agent.session;

import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

/** Optional advisory transport for cross-process JSONL operation aborts. */
@ExperimentalSessionApi
public interface JsonlOperationAbortNotifier {
    JsonlOperationAbortNotifier NONE = new JsonlOperationAbortNotifier() {
        @Override
        public void publish(Notification notification) {
        }

        @Override
        public AutoCloseable observe(Key key, Runnable cancellation) {
            return () -> { };
        }
    };

    void publish(Notification notification);

    AutoCloseable observe(Key key, Runnable cancellation);

    /** Optional sequence-preserving observer extension. */
    interface Sequenced extends JsonlOperationAbortNotifier {
        AutoCloseable observeNotifications(
                Key key,
                Consumer<Notification> observer
        );

        @Override
        default AutoCloseable observe(Key key, Runnable cancellation) {
            Objects.requireNonNull(cancellation, "cancellation");
            return observeNotifications(key, ignored -> cancellation.run());
        }
    }

    record Key(Path generationPath, String lane, String runId) {
        public Key {
            generationPath = Objects.requireNonNull(
                    generationPath, "generationPath"
            ).toAbsolutePath().normalize();
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(runId, "runId");
        }
    }

    record Notification(Key key, long sequence) {
        public Notification {
            Objects.requireNonNull(key, "key");
            if (sequence <= 0) {
                throw new IllegalArgumentException("sequence must be positive");
            }
        }
    }
}
