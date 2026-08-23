package io.github.idoly.pi.agent.skill;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Validated Agent Skills resource with provenance and deferred instructions. */
public record AgentSkill(
        String name,
        String description,
        String license,
        String compatibility,
        Map<String, Object> metadata,
        List<String> allowedTools,
        boolean disableModelInvocation,
        Path source,
        Path directory,
        String instructions,
        Scope scope,
        List<String> warnings
) {
    public AgentSkill {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(description, "description");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        allowedTools = allowedTools == null ? List.of() : List.copyOf(allowedTools);
        source = Objects.requireNonNull(source, "source")
                .toAbsolutePath().normalize();
        directory = Objects.requireNonNull(directory, "directory")
                .toAbsolutePath().normalize();
        Objects.requireNonNull(instructions, "instructions");
        Objects.requireNonNull(scope, "scope");
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }

    public enum Scope {
        GLOBAL,
        PROJECT,
        EXPLICIT,
        PACKAGE
    }
}
