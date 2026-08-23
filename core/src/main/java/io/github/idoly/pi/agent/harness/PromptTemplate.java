package io.github.idoly.pi.agent.harness;

import java.util.Objects;

public record PromptTemplate(String name, String description, String content) {
    public PromptTemplate {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(content, "content");
    }

    public PromptTemplate(String name, String content) {
        this(name, null, content);
    }
}
