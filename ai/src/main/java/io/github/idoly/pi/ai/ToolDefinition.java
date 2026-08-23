package io.github.idoly.pi.ai;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record ToolDefinition(
        String name,
        String description,
        Map<String, Object> parameters,
        ConstrainedSampling constrainedSampling
) {
    public ToolDefinition(String name, String description, Map<String, Object> parameters) {
        this(name, description, parameters, null);
    }

    public ToolDefinition {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(description, "description");
        parameters = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(parameters, "parameters")
        ));
    }
}
