package io.github.idoly.pi.agent.session;

final class SessionRecords {
    private SessionRecords() {
    }

    static SessionRecordDraft copy(SessionRecordDraft value) {
        return switch (value) {
            case SessionRecordDraft.OperationStarted record ->
                    new SessionRecordDraft.OperationStarted(
                            record.id(), record.lane(), record.sourceLeafId(),
                            copyIntent(record.intent())
                    );
            case SessionRecordDraft.AbortRequested record ->
                    new SessionRecordDraft.AbortRequested(
                            record.id(), record.lane(), record.runId()
                    );
            case SessionRecordDraft.OperationFinished record ->
                    new SessionRecordDraft.OperationFinished(
                            record.id(), record.lane(), record.runId(), record.outcome(),
                            record.error() == null ? null
                                    : new SessionRecordDraft.OperationError(
                                            record.error().code(), record.error().message()
                                    )
                    );
            case SessionRecordDraft.StepAttempt record ->
                    new SessionRecordDraft.StepAttempt(
                            record.id(), record.lane(), record.runId(), record.step(),
                            record.attempt(), record.resultEntryId(), record.compactionReason()
                    );
            case SessionRecordDraft.ToolStarted record ->
                    new SessionRecordDraft.ToolStarted(
                            record.id(), record.lane(), record.runId(),
                            record.assistantEntryId(), record.toolIndex(), record.toolCallId(),
                            record.toolName(), record.effectiveArgs(), record.resultEntryId(),
                            record.replay()
                    );
            case SessionRecordDraft.QueueEnqueued record ->
                    new SessionRecordDraft.QueueEnqueued(
                            record.id(), record.lane(), record.queue(), record.runId(),
                            SessionCopies.draft(record.target())
                    );
            case SessionRecordDraft.QueueCancelled record ->
                    new SessionRecordDraft.QueueCancelled(
                            record.id(), record.lane(), record.runId(), record.entryId()
                    );
            case SessionRecordDraft.WriteDeferred record ->
                    new SessionRecordDraft.WriteDeferred(
                            record.id(), record.lane(), record.runId(),
                            SessionCopies.draft(record.target())
                    );
            case SessionRecordDraft.UsageRecord record ->
                    new SessionRecordDraft.UsageRecord(
                            record.id(), record.lane(), record.cause(), record.usage(),
                            record.runId(), record.entryId(), record.attempt(),
                            record.stopReason(), record.toolCallId(), record.details()
                    );
        };
    }

    private static SessionRecordDraft.OperationIntent copyIntent(
            SessionRecordDraft.OperationIntent intent
    ) {
        return switch (intent) {
            case SessionRecordDraft.OperationIntent.Run run ->
                    new SessionRecordDraft.OperationIntent.Run(
                            run.originalPrompt(), run.initialMessages().stream()
                                    .map(SessionCopies::draft).toList(),
                            run.systemPromptOverride(), run.resumeData()
                    );
            case SessionRecordDraft.OperationIntent.Compaction compaction ->
                    new SessionRecordDraft.OperationIntent.Compaction(
                            compaction.customInstructions(), compaction.resultEntryId()
                    );
            case SessionRecordDraft.OperationIntent.Navigation navigation ->
                    new SessionRecordDraft.OperationIntent.Navigation(
                            navigation.targetId(), navigation.summarize(),
                            navigation.customInstructions(), navigation.label(),
                            navigation.summaryEntryId()
                    );
        };
    }
}
