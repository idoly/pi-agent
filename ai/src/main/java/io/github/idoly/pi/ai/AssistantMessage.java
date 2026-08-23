package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Objects;

public record AssistantMessage(
        List<ContentBlock> content,
        String api,
        String provider,
        String model,
        Usage usage,
        StopReason stopReason,
        String errorMessage,
        long timestamp,
        String responseId,
        String rawStopReason
) implements Message {
    public AssistantMessage(
            List<ContentBlock> content,
            String api,
            String provider,
            String model,
            Usage usage,
            StopReason stopReason,
            String errorMessage,
            long timestamp
    ) {
        this(content, api, provider, model, usage, stopReason, errorMessage, timestamp, null, null);
    }

    public AssistantMessage {
        content = List.copyOf(Objects.requireNonNull(content, "content"));
        Objects.requireNonNull(api, "api");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(model, "model");
        Objects.requireNonNull(usage, "usage");
        Objects.requireNonNull(stopReason, "stopReason");
    }
}
