package io.github.idoly.pi.agent.compaction;

import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.SessionBranchQuery;
import io.github.idoly.pi.agent.session.SessionEntry;
import io.github.idoly.pi.agent.session.SessionEntryQuery;

import java.util.concurrent.CompletionStage;

public final class SessionCompaction {
    private SessionCompaction() {
    }

    public static CompletionStage<SessionEntry.Compaction> compact(
            AgentSession session,
            CompactionSettings settings,
            CompactionSummarizer summarizer,
            String customInstructions
    ) {
        return session.findEntriesOnBranch(new SessionBranchQuery(
                null, null, null,
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
        )).thenCompose(entries -> {
            CompactionPreparation preparation = ContextCompaction.prepare(entries, settings);
            if (preparation == null) return java.util.concurrent.CompletableFuture.completedFuture(null);
            return ContextCompaction.compact(
                    preparation, summarizer, customInstructions
            ).thenCompose(result -> session.appendCompaction(
                    result.summary(), result.retainedTail(), result.tokensBefore(),
                    result.details(), result.usage()
            ));
        });
    }
}
