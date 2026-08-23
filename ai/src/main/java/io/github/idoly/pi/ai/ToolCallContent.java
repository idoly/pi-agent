package io.github.idoly.pi.ai;

import java.util.Map;
import java.util.Objects;

public record ToolCallContent(
        String id,
        String name,
        Map<String, Object> arguments,
        String signature
) implements ContentBlock {
    public ToolCallContent(
            String id,
            String name,
            Map<String, Object> arguments
    ) {
        this(id, name, arguments, null);
    }

    public ToolCallContent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        arguments = Map.copyOf(Objects.requireNonNull(arguments, "arguments"));
    }
}
