package io.github.idoly.pi.agent.extension;

import java.nio.file.Path;
import java.util.Objects;

/** A host-orchestrated session replacement or fork before it commits. */
public record SessionTransition(
        Reason reason,
        Path targetSession,
        String entryId
) {
    public SessionTransition {
        Objects.requireNonNull(reason, "reason");
        targetSession = targetSession == null
                ? null : targetSession.toAbsolutePath().normalize();
    }

    public enum Reason {
        NEW,
        RESUME,
        FORK,
        CLONE
    }
}
