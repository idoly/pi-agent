package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record AgentToolResult(
        List<ContentBlock> content,
        Map<String, Object> details,
        Usage usage,
        List<String> addedToolNames,
        boolean terminate
) {
    public AgentToolResult(
            List<ContentBlock> content,
            Map<String, Object> details,
            Usage usage,
            boolean terminate
    ) {
        this(content, details, usage, List.of(), terminate);
    }

    public AgentToolResult {
        content = List.copyOf(Objects.requireNonNull(content, "content"));
        details = details == null ? Map.of() : Map.copyOf(details);
        addedToolNames = addedToolNames == null
                ? List.of() : List.copyOf(addedToolNames);
    }

    public AgentToolResult(List<ContentBlock> content, Map<String, Object> details) {
        this(content, details, null, List.of(), false);
    }
}
