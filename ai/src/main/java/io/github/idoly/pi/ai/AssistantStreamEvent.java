package io.github.idoly.pi.ai;

import java.util.Objects;

public sealed interface AssistantStreamEvent {
    record Start(AssistantMessage partial) implements AssistantStreamEvent {
        public Start {
            Objects.requireNonNull(partial, "partial");
        }
    }

    record ContentStart(ContentKind kind, int contentIndex, AssistantMessage partial)
            implements AssistantStreamEvent {
        public ContentStart {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(partial, "partial");
        }
    }

    record ContentDelta(ContentKind kind, int contentIndex, String delta, AssistantMessage partial)
            implements AssistantStreamEvent {
        public ContentDelta {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(delta, "delta");
            Objects.requireNonNull(partial, "partial");
        }
    }

    record ContentEnd(ContentKind kind, int contentIndex, AssistantMessage partial)
            implements AssistantStreamEvent {
        public ContentEnd {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(partial, "partial");
        }
    }

    record Done(AssistantMessage message) implements AssistantStreamEvent {
        public Done {
            Objects.requireNonNull(message, "message");
        }
    }

    record Error(AssistantMessage message) implements AssistantStreamEvent {
        public Error {
            Objects.requireNonNull(message, "message");
        }
    }
}
