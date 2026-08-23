package io.github.idoly.pi.agent.session;

import java.util.List;
import java.util.Objects;

/** Passive notification emitted after a durable queue mutation commits. */
public sealed interface SessionRunQueueEvent {
    SessionRecordDraft.Queue queue();

    String runId();

    record Enqueued(SessionRunQueue.Pending pending) implements SessionRunQueueEvent {
        public Enqueued {
            Objects.requireNonNull(pending, "pending");
        }

        @Override
        public SessionRecordDraft.Queue queue() {
            return pending.queue();
        }

        @Override
        public String runId() {
            return pending.runId();
        }
    }

    record Cancelled(
            SessionRecordDraft.Queue queue,
            String runId,
            String entryId
    ) implements SessionRunQueueEvent {
        public Cancelled {
            Objects.requireNonNull(queue, "queue");
            Objects.requireNonNull(entryId, "entryId");
        }
    }

    record Consumed(
            SessionRecordDraft.Queue queue,
            String runId,
            List<String> entryIds
    ) implements SessionRunQueueEvent {
        public Consumed {
            Objects.requireNonNull(queue, "queue");
            entryIds = List.copyOf(entryIds);
            if (entryIds.isEmpty()) {
                throw new IllegalArgumentException("entryIds must not be empty");
            }
        }
    }
}
