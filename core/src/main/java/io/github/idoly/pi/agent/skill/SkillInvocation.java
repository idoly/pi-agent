package io.github.idoly.pi.agent.skill;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** Prepared manual skill invocation with provenance and requested tool policy. */
public record SkillInvocation(
        String command,
        String name,
        String arguments,
        String prompt,
        List<String> allowedTools,
        Path source,
        AgentSkill.Scope scope
) {
    public SkillInvocation {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(name, "name");
        arguments = arguments == null ? "" : arguments;
        Objects.requireNonNull(prompt, "prompt");
        allowedTools = List.copyOf(Objects.requireNonNull(
                allowedTools, "allowedTools"
        ));
        source = Objects.requireNonNull(source, "source")
                .toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
    }
}
