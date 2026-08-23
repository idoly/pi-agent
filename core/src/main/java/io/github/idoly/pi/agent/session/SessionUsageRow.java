package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.ai.Usage;

import java.util.Objects;

public record SessionUsageRow(
        String id,
        long sequence,
        long timestamp,
        Usage usage,
        String entryId,
        boolean adjustment,
        JsonNode details
) {
    public SessionUsageRow {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(usage, "usage");
        if (sequence <= 0) throw new IllegalArgumentException("sequence must be positive");
        if (timestamp < 0) throw new IllegalArgumentException("timestamp must be non-negative");
        details = SessionJson.copy(details, "usage details");
    }

    public JsonNode details() {
        return SessionJson.copy(details, "usage details");
    }
}
