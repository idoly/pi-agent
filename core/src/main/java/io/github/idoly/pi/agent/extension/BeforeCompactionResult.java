package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.compaction.CompactionResult;

/** Cancel compaction or provide a complete replacement result. */
public record BeforeCompactionResult(boolean cancel, CompactionResult replacement) {
    public BeforeCompactionResult {
        if (cancel && replacement != null) {
            throw new IllegalArgumentException(
                    "cancel and replacement are mutually exclusive"
            );
        }
    }
    public static BeforeCompactionResult proceed() {
        return new BeforeCompactionResult(false, null);
    }

    public static BeforeCompactionResult cancelled() {
        return new BeforeCompactionResult(true, null);
    }

    public static BeforeCompactionResult replace(CompactionResult result) {
        return new BeforeCompactionResult(false, result);
    }
}
