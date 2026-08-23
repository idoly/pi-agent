package io.github.idoly.pi.agent.skill;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Parser and metadata view for the headless {@code /skill:name} convention. */
public final class SkillCommandDispatcher {
    private static final String PREFIX = "/skill:";
    private final SkillRegistry registry;

    public SkillCommandDispatcher(SkillRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /** Returns empty when input is not a skill command. */
    public Optional<SkillInvocation> dispatch(String input) {
        Objects.requireNonNull(input, "input");
        String normalized = input.strip();
        if (!normalized.startsWith(PREFIX)) return Optional.empty();
        int separator = firstWhitespace(normalized);
        String command = separator < 0
                ? normalized : normalized.substring(0, separator);
        String name = command.substring(PREFIX.length());
        if (name.isBlank()) {
            throw new IllegalArgumentException("Skill command name is missing");
        }
        String arguments = separator < 0
                ? "" : normalized.substring(separator).strip();
        AgentSkill skill = registry.find(name).orElseThrow(() ->
                new IllegalArgumentException("Unknown skill: " + name)
        );
        return Optional.of(new SkillInvocation(
                command, name, arguments,
                registry.invoke(name, arguments), skill.allowedTools(),
                skill.source(), skill.scope()
        ));
    }

    public List<Command> commands() {
        return registry.skills().stream().map(skill -> new Command(
                PREFIX + skill.name(), skill.name(), skill.description(),
                skill.allowedTools(), skill.source(), skill.scope()
        )).toList();
    }

    private static int firstWhitespace(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (Character.isWhitespace(value.charAt(index))) return index;
        }
        return -1;
    }

    /** Command-palette metadata; authorization remains a host decision. */
    public record Command(
            String command,
            String name,
            String description,
            List<String> allowedTools,
            Path source,
            AgentSkill.Scope scope
    ) {
        public Command {
            Objects.requireNonNull(command, "command");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(description, "description");
            allowedTools = List.copyOf(Objects.requireNonNull(
                    allowedTools, "allowedTools"
            ));
            source = Objects.requireNonNull(source, "source")
                    .toAbsolutePath().normalize();
            Objects.requireNonNull(scope, "scope");
        }
    }
}
