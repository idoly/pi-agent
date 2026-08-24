package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ToolResultMessage(
        String toolCallId,
        String toolName,
        List<ContentBlock> content,
        Map<String, Object> details,
        Usage usage,
        List<String> addedToolNames,
        boolean error,
        long timestamp
) implements Message {
    public ToolResultMessage(
            String toolCallId,
            String toolName,
            List<ContentBlock> content,
            Map<String, Object> details,
            Usage usage,
            boolean error,
            long timestamp
    ) {
        this(
                toolCallId, toolName, content, details, usage, List.of(),
                error, timestamp
        );
    }

    public ToolResultMessage {
        Objects.requireNonNull(toolCallId, "toolCallId");
        Objects.requireNonNull(toolName, "toolName");
        content = List.copyOf(Objects.requireNonNull(content, "content"));
        details = details == null ? Map.of() : Map.copyOf(details);
        addedToolNames = addedToolNames == null
                ? List.of() : List.copyOf(addedToolNames);
    }
}
