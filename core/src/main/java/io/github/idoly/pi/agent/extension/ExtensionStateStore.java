package io.github.idoly.pi.agent.extension;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.SessionEntry;
import io.github.idoly.pi.agent.session.SessionEntryQuery;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Append-only durable extension state backed by session custom entries. */
public final class ExtensionStateStore {
    private final String extensionId;
    private final AgentSession session;

    ExtensionStateStore(String extensionId, AgentSession session) {
        this.extensionId = Objects.requireNonNull(extensionId, "extensionId");
        this.session = session;
    }

    public CompletionStage<String> put(String key, JsonNode value) {
        requireKey(key);
        Objects.requireNonNull(value, "value");
        if (session == null) return noSession();
        return session.appendCustom(customType(key), value);
    }

    public CompletionStage<Optional<JsonNode>> get(String key) {
        requireKey(key);
        if (session == null) return noSession();
        return session.findEntries(new SessionEntryQuery(
                SessionEntry.Type.CUSTOM, customType(key),
                SessionEntryQuery.Order.NEWEST_FIRST, 1, null
        )).thenApply(entries -> entries.isEmpty()
                ? Optional.empty()
                : Optional.of(((SessionEntry.Custom) entries.getFirst()).data()));
    }

    public CompletionStage<List<JsonNode>> history(String key, int limit) {
        requireKey(key);
        if (limit < 1) throw new IllegalArgumentException("limit must be positive");
        if (session == null) return noSession();
        return session.findEntries(new SessionEntryQuery(
                SessionEntry.Type.CUSTOM, customType(key),
                SessionEntryQuery.Order.NEWEST_FIRST, limit, null
        )).thenApply(entries -> entries.stream()
                .map(SessionEntry.Custom.class::cast)
                .map(SessionEntry.Custom::data).toList());
    }

    private String customType(String key) {
        return "extension-state:" + extensionId.length() + ':'
                + extensionId + ':' + key;
    }

    private static void requireKey(String key) {
        if (key == null || key.isBlank() || key.length() > 128) {
            throw new IllegalArgumentException(
                    "state key must contain 1 to 128 characters"
            );
        }
    }

    private static <T> CompletionStage<T> noSession() {
        return CompletableFuture.failedFuture(new IllegalStateException(
                "Durable extension state requires an AgentSession"
        ));
    }
}
