package io.github.idoly.pi.agent.harness;

import java.util.Objects;

public record Skill(
        String name,
        String description,
        String content,
        String filePath,
        boolean disableModelInvocation
) {
    public Skill {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(filePath, "filePath");
    }

    public Skill(String name, String description, String content, String filePath) {
        this(name, description, content, filePath, false);
    }
}
