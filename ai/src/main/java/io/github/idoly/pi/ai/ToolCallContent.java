package io.github.idoly.pi.ai;

import java.util.Map;
import java.util.Objects;

public record ToolCallContent(String id, String name, Map<String, Object> arguments) implements ContentBlock {
    public ToolCallContent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        arguments = Map.copyOf(Objects.requireNonNull(arguments, "arguments"));
    }
}
