package io.github.idoly.pi.agent.session;

import java.util.Objects;

public record SessionMetadata(
        String id,
        long createdAt,
        int storageVersion,
        String parentSessionId
) {
    public static final int CURRENT_STORAGE_VERSION = 1;

    public SessionMetadata {
        Objects.requireNonNull(id, "id");
        if (id.isBlank()) throw new IllegalArgumentException("id must not be blank");
        if (createdAt < 0) throw new IllegalArgumentException("createdAt must be non-negative");
        if (storageVersion < 1) throw new IllegalArgumentException("storageVersion must be positive");
    }

    public SessionMetadata(String id, long createdAt) {
        this(id, createdAt, CURRENT_STORAGE_VERSION, null);
    }
}
