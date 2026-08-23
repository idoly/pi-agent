package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Objects;

public sealed interface SessionRecordDraft {
    String id();

    String lane();

    Type type();

    enum Type {
        OPERATION_STARTED,
        ABORT_REQUESTED,
        OPERATION_FINISHED,
        STEP_ATTEMPT,
        TOOL_STARTED,
        QUEUE_ENQUEUED,
        QUEUE_CANCELLED,
        WRITE_DEFERRED,
        USAGE
    }

    enum OperationKind { RUN, COMPACTION, NAVIGATION }

    enum OperationOutcome { COMPLETED, ABORTED, FAILED, DECLINED }

    enum Step { ASSISTANT, COMPACTION, BRANCH_SUMMARY }

    enum CompactionReason { MANUAL, THRESHOLD, OVERFLOW }

    enum Replay { NEVER, SAFE }

    enum Queue { STEER, FOLLOW_UP, NEXT_RUN }

    sealed interface OperationIntent {
        OperationKind kind();

        record Run(
                List<AgentMessage> originalPrompt,
                List<SessionEntryDraft> initialMessages,
                String systemPromptOverride,
                JsonNode resumeData
        ) implements OperationIntent {
            public Run {
                originalPrompt = SessionCopies.messages(originalPrompt);
                initialMessages = List.copyOf(initialMessages);
                resumeData = SessionJson.copy(resumeData, "operation resume data");
            }

            @Override
            public JsonNode resumeData() {
                return SessionJson.copy(resumeData, "operation resume data");
            }

            @Override
            public OperationKind kind() { return OperationKind.RUN; }
        }

        record Compaction(String customInstructions, String resultEntryId)
                implements OperationIntent {
            public Compaction {
                Objects.requireNonNull(resultEntryId, "resultEntryId");
            }

            @Override
            public OperationKind kind() { return OperationKind.COMPACTION; }
        }

        record Navigation(
                String targetId,
                boolean summarize,
                String customInstructions,
                String label,
                String summaryEntryId
        ) implements OperationIntent {
            @Override
            public OperationKind kind() { return OperationKind.NAVIGATION; }
        }
    }

    record OperationStarted(
            String id,
            String lane,
            String sourceLeafId,
            OperationIntent intent
    ) implements SessionRecordDraft {
        public OperationStarted {
            require(id, lane);
            Objects.requireNonNull(intent, "intent");
        }

        @Override public Type type() { return Type.OPERATION_STARTED; }
    }

    record AbortRequested(String id, String lane, String runId) implements SessionRecordDraft {
        public AbortRequested { require(id, lane); Objects.requireNonNull(runId, "runId"); }
        @Override public Type type() { return Type.ABORT_REQUESTED; }
    }

    record OperationError(String code, String message) {
        public OperationError { Objects.requireNonNull(code, "code"); Objects.requireNonNull(message, "message"); }
    }

    record OperationFinished(
            String id,
            String lane,
            String runId,
            OperationOutcome outcome,
            OperationError error
    ) implements SessionRecordDraft {
        public OperationFinished {
            require(id, lane);
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(outcome, "outcome");
        }
        @Override public Type type() { return Type.OPERATION_FINISHED; }
    }

    record StepAttempt(
            String id,
            String lane,
            String runId,
            Step step,
            int attempt,
            String resultEntryId,
            CompactionReason compactionReason
    ) implements SessionRecordDraft {
        public StepAttempt {
            require(id, lane);
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(step, "step");
            Objects.requireNonNull(resultEntryId, "resultEntryId");
            if (attempt < 0) throw invalid("attempt must be non-negative");
            if ((step == Step.COMPACTION) != (compactionReason != null)) {
                throw invalid("compactionReason is required exactly for compaction attempts");
            }
        }
        @Override public Type type() { return Type.STEP_ATTEMPT; }
    }

    record ToolStarted(
            String id,
            String lane,
            String runId,
            String assistantEntryId,
            int toolIndex,
            String toolCallId,
            String toolName,
            JsonNode effectiveArgs,
            String resultEntryId,
            Replay replay
    ) implements SessionRecordDraft {
        public ToolStarted {
            require(id, lane);
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(assistantEntryId, "assistantEntryId");
            if (toolIndex < 0) throw invalid("toolIndex must be non-negative");
            Objects.requireNonNull(toolCallId, "toolCallId");
            Objects.requireNonNull(toolName, "toolName");
            effectiveArgs = SessionJson.copy(effectiveArgs, "effective tool arguments");
            if (effectiveArgs == null || !effectiveArgs.isObject()) throw invalid("effectiveArgs must be an object");
            Objects.requireNonNull(resultEntryId, "resultEntryId");
            Objects.requireNonNull(replay, "replay");
        }
        @Override
        public JsonNode effectiveArgs() {
            return SessionJson.copy(effectiveArgs, "effective tool arguments");
        }
        @Override public Type type() { return Type.TOOL_STARTED; }
    }

    record QueueEnqueued(
            String id,
            String lane,
            Queue queue,
            String runId,
            SessionEntryDraft target
    ) implements SessionRecordDraft {
        public QueueEnqueued {
            require(id, lane);
            Objects.requireNonNull(queue, "queue");
            Objects.requireNonNull(target, "target");
            if ((queue == Queue.NEXT_RUN) == (runId != null)) {
                throw invalid("runId is required for steer/follow-up and forbidden for nextRun");
            }
        }
        @Override public Type type() { return Type.QUEUE_ENQUEUED; }
    }

    record QueueCancelled(String id, String lane, String runId, String entryId)
            implements SessionRecordDraft {
        public QueueCancelled { require(id, lane); Objects.requireNonNull(entryId, "entryId"); }
        @Override public Type type() { return Type.QUEUE_CANCELLED; }
    }

    record WriteDeferred(String id, String lane, String runId, SessionEntryDraft target)
            implements SessionRecordDraft {
        public WriteDeferred {
            require(id, lane);
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(target, "target");
        }
        @Override public Type type() { return Type.WRITE_DEFERRED; }
    }

    record UsageRecord(
            String id,
            String lane,
            String cause,
            Usage usage,
            String runId,
            String entryId,
            Integer attempt,
            String stopReason,
            String toolCallId,
            JsonNode details
    ) implements SessionRecordDraft {
        public UsageRecord {
            require(id, lane);
            Objects.requireNonNull(cause, "cause");
            Objects.requireNonNull(usage, "usage");
            if (attempt != null && attempt < 0) throw invalid("attempt must be non-negative");
            details = SessionJson.copy(details, "usage details");
        }
        @Override
        public JsonNode details() {
            return SessionJson.copy(details, "usage details");
        }
        @Override public Type type() { return Type.USAGE; }
    }

    private static void require(String id, String lane) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(lane, "lane");
        if (id.isBlank()) throw invalid("id must not be blank");
        if (lane.isBlank()) throw invalid("lane must not be blank");
    }

    private static SessionError invalid(String message) {
        return new SessionError(SessionError.Code.INVALID_ENTRY, message);
    }
}
