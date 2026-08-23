package io.github.idoly.pi.agent.session;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface SessionRepository {
    CompletionStage<AgentSession> create(CreateOptions options);

    CompletionStage<AgentSession> open(SessionMetadata metadata);

    CompletionStage<List<SessionMetadata>> list();

    CompletionStage<Void> delete(SessionMetadata metadata);

    CompletionStage<AgentSession> fork(SessionMetadata source, SessionForkOptions options);

    CompletionStage<AgentSession> copyRetained(
            SessionMetadata source,
            SessionRetainedCopyOptions options
    );

    record CreateOptions(String id, String parentSessionId) {
        public static final CreateOptions DEFAULT = new CreateOptions(null, null);
    }
}
