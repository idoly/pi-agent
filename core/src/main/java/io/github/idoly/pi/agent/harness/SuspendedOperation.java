package io.github.idoly.pi.agent.harness;

import java.util.List;

public record SuspendedOperation(
        String lane,
        Kind kind,
        String id,
        long startedAt,
        Reason reason,
        List<String> missingTools,
        List<String> missingModels
) {
    public SuspendedOperation {
        missingTools = missingTools == null ? List.of() : List.copyOf(missingTools);
        missingModels = missingModels == null ? List.of() : List.copyOf(missingModels);
    }

    public enum Kind { RUN, COMPACTION, NAVIGATION }

    public enum Reason { CRASH, DEFERRED }
}
