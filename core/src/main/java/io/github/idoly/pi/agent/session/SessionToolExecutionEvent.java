package io.github.idoly.pi.agent.session;

import java.util.Objects;

public sealed interface SessionToolExecutionEvent permits
        SessionToolExecutionEvent.EffectStarted,
        SessionToolExecutionEvent.EffectFinished,
        SessionToolExecutionEvent.ResultPublished {
    String lane();

    String runId();

    String assistantEntryId();

    int toolIndex();

    String toolCallId();

    String toolName();

    record EffectStarted(
            String lane,
            String runId,
            String assistantEntryId,
            int toolIndex,
            String toolCallId,
            String toolName,
            SessionRecordDraft.Replay replay,
            boolean recovery
    ) implements SessionToolExecutionEvent {
        public EffectStarted {
            common(lane, runId, assistantEntryId, toolIndex, toolCallId, toolName);
            Objects.requireNonNull(replay, "replay");
        }
    }

    record EffectFinished(
            String lane,
            String runId,
            String assistantEntryId,
            int toolIndex,
            String toolCallId,
            String toolName,
            boolean error
    ) implements SessionToolExecutionEvent {
        public EffectFinished {
            common(lane, runId, assistantEntryId, toolIndex, toolCallId, toolName);
        }
    }

    record ResultPublished(
            String lane,
            String runId,
            String assistantEntryId,
            int toolIndex,
            String toolCallId,
            String toolName,
            String resultEntryId
    ) implements SessionToolExecutionEvent {
        public ResultPublished {
            common(lane, runId, assistantEntryId, toolIndex, toolCallId, toolName);
            Objects.requireNonNull(resultEntryId, "resultEntryId");
        }
    }

    private static void common(
            String lane,
            String runId,
            String assistantEntryId,
            int toolIndex,
            String toolCallId,
            String toolName
    ) {
        Objects.requireNonNull(lane, "lane");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(assistantEntryId, "assistantEntryId");
        if (toolIndex < 0) throw new IllegalArgumentException("toolIndex must be non-negative");
        Objects.requireNonNull(toolCallId, "toolCallId");
        Objects.requireNonNull(toolName, "toolName");
    }
}
