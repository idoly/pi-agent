package io.github.idoly.pi.agent.harness;

import java.util.List;

public record HarnessResources(List<Skill> skills, List<PromptTemplate> promptTemplates) {
    public static final HarnessResources EMPTY = new HarnessResources(List.of(), List.of());

    public HarnessResources {
        skills = skills == null ? List.of() : List.copyOf(skills);
        promptTemplates = promptTemplates == null ? List.of() : List.copyOf(promptTemplates);
    }

    public HarnessResources copy() {
        return new HarnessResources(skills, promptTemplates);
    }
}
