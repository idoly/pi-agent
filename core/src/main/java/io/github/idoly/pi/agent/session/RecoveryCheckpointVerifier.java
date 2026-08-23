package io.github.idoly.pi.agent.session;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/** Orchestrates scan and detail cursors for bounded checkpoint verification. */
@ExperimentalSessionApi
public final class RecoveryCheckpointVerifier {
    private RecoveryCheckpointVerifier() {
    }

    public static CompletionStage<Result> verify(
            JsonlSessionRepository repository,
            JsonlSessionRepository.RecoveryCheckpoint checkpoint,
            Options options,
            Consumer<JsonlSessionRepository.RecoveryCheckpointDetail> details,
            Consumer<State> progress
    ) {
        return resume(
                repository, checkpoint, options, State.INITIAL, details, progress
        );
    }

    public static CompletionStage<Result> resume(
            JsonlSessionRepository repository,
            JsonlSessionRepository.RecoveryCheckpoint checkpoint,
            Options options,
            State state,
            Consumer<JsonlSessionRepository.RecoveryCheckpointDetail> details,
            Consumer<State> progress
    ) {
        Objects.requireNonNull(repository, "repository");
        Objects.requireNonNull(checkpoint, "checkpoint");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(details, "details");
        Objects.requireNonNull(progress, "progress");
        return CompletableFuture.supplyAsync(
                () -> verifyNow(
                        repository, checkpoint, options, state, details, progress
                ),
                command -> Thread.ofVirtual()
                        .name("pi-checkpoint-verifier")
                        .start(command)
        );
    }

    private static Result verifyNow(
            JsonlSessionRepository repository,
            JsonlSessionRepository.RecoveryCheckpoint checkpoint,
            Options options,
            State initial,
            Consumer<JsonlSessionRepository.RecoveryCheckpointDetail> details,
            Consumer<State> progress
    ) {
        State state = initial;
        if (state.complete()) {
            return new Result(state.counts(), state.generationsInspected());
        }
        while (true) {
            JsonlSessionRepository.RecoveryCheckpointQuery detailQuery =
                    new JsonlSessionRepository.RecoveryCheckpointQuery(
                            options.maxDetailsPerPage(), state.detailAfter()
                    );
            JsonlSessionRepository.RecoveryCheckpointBatchReport batch = repository
                    .verifyRecoveryCheckpointBatch(
                            checkpoint,
                            new JsonlSessionRepository.RecoveryCheckpointScanQuery(
                                    detailQuery, state.scanAfter(),
                                    options.maxGenerationsPerBatch(),
                                    options.maxDurationPerBatch()
                            )
                    ).toCompletableFuture().join();

            EnumMap<JsonlSessionRepository.CheckpointStatus, Long> counts =
                    mutableCounts(state.counts());
            long inspected = state.generationsInspected();
            boolean counted = state.currentBatchCounted();
            if (!counted) {
                for (JsonlSessionRepository.CheckpointStatus status
                        : JsonlSessionRepository.CheckpointStatus.values()) {
                    counts.merge(
                            status, batch.verification().count(status), Long::sum
                    );
                }
                inspected = Math.addExact(
                        inspected, batch.generationsInspected()
                );
                counted = true;
            }
            for (JsonlSessionRepository.RecoveryCheckpointDetail detail
                    : batch.verification().details()) {
                details.accept(detail);
            }

            if (batch.verification().truncated()) {
                state = new State(
                        state.scanAfter(), batch.verification().nextCursor(),
                        true, false, counts, inspected
                );
                progress.accept(state);
                continue;
            }
            if (batch.scanComplete()) {
                State completed = new State(
                        null, null, false, true, counts, inspected
                );
                progress.accept(completed);
                return new Result(counts, inspected);
            }
            state = new State(
                    batch.nextScanCursor(), null, false, false, counts, inspected
            );
            progress.accept(state);
        }
    }

    private static EnumMap<JsonlSessionRepository.CheckpointStatus, Long>
    mutableCounts(Map<JsonlSessionRepository.CheckpointStatus, Long> source) {
        EnumMap<JsonlSessionRepository.CheckpointStatus, Long> result =
                new EnumMap<>(JsonlSessionRepository.CheckpointStatus.class);
        for (JsonlSessionRepository.CheckpointStatus status
                : JsonlSessionRepository.CheckpointStatus.values()) {
            result.put(status, source.getOrDefault(status, 0L));
        }
        return result;
    }

    public record Options(
            int maxGenerationsPerBatch,
            Integer maxDetailsPerPage,
            Duration maxDurationPerBatch
    ) {
        public Options(int maxGenerationsPerBatch, Integer maxDetailsPerPage) {
            this(maxGenerationsPerBatch, maxDetailsPerPage, null);
        }

        public Options {
            if (maxGenerationsPerBatch < 1) {
                throw new IllegalArgumentException(
                        "maxGenerationsPerBatch must be positive"
                );
            }
            if (maxDetailsPerPage != null && maxDetailsPerPage < 1) {
                throw new IllegalArgumentException(
                        "maxDetailsPerPage must be positive"
                );
            }
            if (maxDurationPerBatch != null
                    && maxDurationPerBatch.isNegative()) {
                throw new IllegalArgumentException(
                        "maxDurationPerBatch must be non-negative"
                );
            }
        }
    }

    public record State(
            JsonlSessionRepository.RecoveryCheckpointScanCursor scanAfter,
            JsonlSessionRepository.RecoveryCheckpointCursor detailAfter,
            boolean currentBatchCounted,
            boolean complete,
            Map<JsonlSessionRepository.CheckpointStatus, Long> counts,
            long generationsInspected
    ) {
        public static final State INITIAL = new State(
                null, null, false, false, Map.of(), 0
        );

        public State {
            counts = Map.copyOf(Objects.requireNonNull(counts, "counts"));
            if (detailAfter != null && !currentBatchCounted) {
                throw new IllegalArgumentException(
                        "A detail cursor requires counted batch totals"
                );
            }
            if (complete && (scanAfter != null || detailAfter != null
                    || currentBatchCounted)) {
                throw new IllegalArgumentException(
                        "Completed verification cannot retain active cursors"
                );
            }
            if (generationsInspected < 0) {
                throw new IllegalArgumentException(
                        "generationsInspected must not be negative"
                );
            }
            for (Map.Entry<JsonlSessionRepository.CheckpointStatus, Long> entry
                    : counts.entrySet()) {
                Objects.requireNonNull(entry.getKey(), "count status");
                if (entry.getValue() == null || entry.getValue() < 0) {
                    throw new IllegalArgumentException(
                            "Checkpoint counts must not be negative"
                    );
                }
            }
        }
    }

    public record Result(
            Map<JsonlSessionRepository.CheckpointStatus, Long> counts,
            long generationsInspected
    ) {
        public Result {
            counts = Map.copyOf(Objects.requireNonNull(counts, "counts"));
            if (generationsInspected < 0) {
                throw new IllegalArgumentException(
                        "generationsInspected must not be negative"
                );
            }
        }

        public long count(JsonlSessionRepository.CheckpointStatus status) {
            return counts.getOrDefault(status, 0L);
        }
    }
}
