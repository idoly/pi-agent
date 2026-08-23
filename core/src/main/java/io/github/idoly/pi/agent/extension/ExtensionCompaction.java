package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.compaction.CompactionPreparation;
import io.github.idoly.pi.agent.compaction.CompactionResult;
import io.github.idoly.pi.agent.session.SessionRecordDraft;

import java.util.Objects;

/**
 * Headless compaction event shared with an embedding host. Preparation may be
 * omitted from an after-compaction notification; result may be omitted before.
 */
public record ExtensionCompaction(
        CompactionPreparation preparation,
        SessionRecordDraft.CompactionReason reason,
        boolean willRetry,
        CompactionResult result
) {
    public ExtensionCompaction {
        Objects.requireNonNull(reason, "reason");
    }
}
