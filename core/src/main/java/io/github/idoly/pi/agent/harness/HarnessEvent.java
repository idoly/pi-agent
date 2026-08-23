package io.github.idoly.pi.agent.harness;

import java.util.Objects;

public sealed interface HarnessEvent permits HarnessEvent.RunStart, HarnessEvent.RunEnd {
    String lane();

    String runId();

    record RunStart(String lane, String runId) implements HarnessEvent {
        public RunStart {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(runId, "runId");
        }
    }

    record RunEnd(
            String lane,
            String runId,
            Outcome outcome,
            String leafId
    ) implements HarnessEvent {
        public RunEnd {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(leafId, "leafId");
        }
    }

    enum Outcome { COMPLETED, ABORTED, FAILED }
}
