package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.harness.SuspendedOperation;

import java.util.Objects;

public sealed interface SessionOperationEvent permits
        SessionOperationEvent.Started,
        SessionOperationEvent.AttemptStarted,
        SessionOperationEvent.Finished {
    String lane();

    String runId();

    record Started(
            String lane,
            String runId,
            SuspendedOperation.Kind kind,
            String sourceLeafId
    ) implements SessionOperationEvent {
        public Started {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(kind, "kind");
        }
    }

    record AttemptStarted(
            String lane,
            String runId,
            SessionRecordDraft.Step step,
            int attempt,
            SessionRecordDraft.CompactionReason compactionReason
    ) implements SessionOperationEvent {
        public AttemptStarted {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(step, "step");
            if (attempt < 1) throw new IllegalArgumentException("attempt must be positive");
            if ((step == SessionRecordDraft.Step.COMPACTION)
                    != (compactionReason != null)) {
                throw new IllegalArgumentException(
                        "compactionReason is required exactly for compaction attempts"
                );
            }
        }
    }

    record Finished(
            String lane,
            String runId,
            SuspendedOperation.Kind kind,
            Outcome outcome,
            String leafId,
            String resultEntryId,
            SessionRecordDraft.OperationError error
    ) implements SessionOperationEvent {
        public Finished {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(outcome, "outcome");
            if ((outcome == Outcome.FAILED) != (error != null)) {
                throw new IllegalArgumentException(
                        "error is required exactly for failed outcomes"
                );
            }
        }
    }

    enum Outcome { COMPLETED, ABORTED, FAILED }
}
