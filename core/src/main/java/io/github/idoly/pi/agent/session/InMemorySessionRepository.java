package io.github.idoly.pi.agent.session;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Process-local repository implementing the same tree/query/fork contract as durable backends. */
public final class InMemorySessionRepository implements SessionRepository {
    private final Object lock = new Object();
    private final LinkedHashMap<String, InMemorySessionState> sessions = new LinkedHashMap<>();
    private final Clock clock;
    private final SessionIdGenerator idGenerator;

    public InMemorySessionRepository() {
        this(Clock.systemUTC(), SessionIdGenerator.uuidV7());
    }

    public InMemorySessionRepository(Clock clock, SessionIdGenerator idGenerator) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.idGenerator = Objects.requireNonNull(idGenerator, "idGenerator");
    }

    @Override
    public CompletionStage<AgentSession> create(CreateOptions options) {
        CreateOptions effective = options == null ? CreateOptions.DEFAULT : options;
        return stage(() -> {
            String id = effective.id() == null ? idGenerator.next() : effective.id();
            synchronized (lock) {
                if (sessions.containsKey(id)) {
                    throw error(SessionError.Code.ALREADY_EXISTS, "Session already exists: " + id);
                }
                SessionMetadata metadata = new SessionMetadata(
                        id, clock.millis(), SessionMetadata.CURRENT_STORAGE_VERSION,
                        effective.parentSessionId()
                );
                InMemorySessionState state = new InMemorySessionState(metadata, clock);
                sessions.put(id, state);
                return new AgentSession(state, idGenerator, "main");
            }
        });
    }

    @Override
    public CompletionStage<AgentSession> open(SessionMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        return stage(() -> {
            if (metadata.storageVersion() != SessionMetadata.CURRENT_STORAGE_VERSION) {
                throw error(
                        SessionError.Code.STORAGE,
                        "Unsupported session storage version: " + metadata.storageVersion()
                );
            }
            synchronized (lock) {
                InMemorySessionState state = sessions.get(metadata.id());
                if (state == null) {
                    throw error(SessionError.Code.NOT_FOUND, "Session not found: " + metadata.id());
                }
                return new AgentSession(state, idGenerator, "main");
            }
        });
    }

    @Override
    public CompletionStage<List<SessionMetadata>> list() {
        return stage(() -> {
            synchronized (lock) {
                return sessions.values().stream().map(InMemorySessionState::metadata).toList();
            }
        });
    }

    @Override
    public CompletionStage<Void> delete(SessionMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        return stage(() -> {
            synchronized (lock) {
                sessions.remove(metadata.id());
            }
            return null;
        });
    }

    @Override
    public CompletionStage<AgentSession> fork(
            SessionMetadata source,
            SessionForkOptions options
    ) {
        Objects.requireNonNull(source, "source");
        SessionForkOptions effective = options == null
                ? new SessionForkOptions(null, null, null, null, null)
                : options;
        return stage(() -> {
            synchronized (lock) {
                InMemorySessionState sourceState = sessions.get(source.id());
                if (sourceState == null) {
                    throw error(SessionError.Code.NOT_FOUND, "Session not found: " + source.id());
                }
                String id = effective.id() == null ? idGenerator.next() : effective.id();
                if (sessions.containsKey(id)) {
                    throw error(SessionError.Code.ALREADY_EXISTS, "Session already exists: " + id);
                }
                SessionMetadata metadata = new SessionMetadata(
                        id, clock.millis(), SessionMetadata.CURRENT_STORAGE_VERSION,
                        effective.parentSessionId() == null ? source.id() : effective.parentSessionId()
                );
                InMemorySessionState state = sourceState.fork(metadata, effective);
                sessions.put(id, state);
                return new AgentSession(state, idGenerator, "main");
            }
        });
    }

    @Override
    public CompletionStage<AgentSession> copyRetained(
            SessionMetadata source,
            SessionRetainedCopyOptions options
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");
        return stage(() -> {
            synchronized (lock) {
                InMemorySessionState sourceState = sessions.get(source.id());
                if (sourceState == null) {
                    throw error(SessionError.Code.NOT_FOUND,
                            "Session not found: " + source.id());
                }
                String id = options.id() == null ? idGenerator.next() : options.id();
                if (sessions.containsKey(id)) {
                    throw error(SessionError.Code.ALREADY_EXISTS,
                            "Session already exists: " + id);
                }
                SessionMetadata metadata = new SessionMetadata(
                        id, clock.millis(), SessionMetadata.CURRENT_STORAGE_VERSION,
                        options.parentSessionId() == null
                                ? source.id() : options.parentSessionId()
                );
                InMemorySessionState state = sourceState.retainedCopy(metadata, options);
                sessions.put(id, state);
                return new AgentSession(state, idGenerator, "main");
            }
        });
    }

    private static <T> CompletionStage<T> stage(Supplier<T> operation) {
        try {
            return CompletableFuture.completedFuture(operation.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static SessionError error(SessionError.Code code, String message) {
        return new SessionError(code, message);
    }
}
