package io.github.idoly.pi.agent.skill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SkillRegistryTest {
    @TempDir
    Path temporary;

    @Test
    void discoversTrustedScopesAndRendersProgressiveDisclosure() throws Exception {
        Path home = Files.createDirectories(temporary.resolve("home"));
        Path project = Files.createDirectories(temporary.resolve("repo/sub/project"));
        Files.createDirectory(temporary.resolve("repo/.git"));
        write(home.resolve(".pi/agent/skills/root.md"),
                "global-root", "Global <root>", false);
        write(home.resolve(".agents/skills/ignored.md"),
                "ignored-root", "ignored", false);
        write(home.resolve(".agents/skills/nested/SKILL.md"),
                "global-nested", "Nested & global", false);
        write(temporary.resolve("repo/.agents/skills/ancestor/SKILL.md"),
                "ancestor", "Ancestor", false);
        write(project.resolve(".pi/skills/private/SKILL.md"),
                "private", "Manual only", true);

        SkillRegistry untrusted = SkillRegistry.discover(new SkillDiscoveryOptions(
                home, project, false, true, List.of(), List.of()
        ));
        assertEquals(List.of("global-nested", "global-root"),
                untrusted.skills().stream().map(AgentSkill::name).sorted().toList());

        SkillRegistry trusted = SkillRegistry.discover(new SkillDiscoveryOptions(
                home, project, true, true, List.of(), List.of()
        ));
        assertEquals(List.of("ancestor", "global-nested", "global-root", "private"),
                trusted.skills().stream().map(AgentSkill::name).sorted().toList());
        String prompt = trusted.systemPromptXml();
        assertTrue(prompt.contains("Global &lt;root&gt;"));
        assertTrue(prompt.contains("Nested &amp; global"));
        assertFalse(prompt.contains("Manual only"));
        assertEquals("# global-root\n\nBody\n\nUser: run now",
                trusted.invoke("global-root", " run now "));
    }

    @Test
    void validationIsLenientExceptDescriptionAndCollisionsKeepFirst()
            throws Exception {
        Path home = Files.createDirectories(temporary.resolve("home"));
        Path first = write(home.resolve(".pi/agent/skills/first/SKILL.md"),
                "Bad_Name", "first", false);
        write(home.resolve(".pi/agent/skills/second/SKILL.md"),
                "Bad_Name", "second", false);
        Path missing = home.resolve(".pi/agent/skills/missing/SKILL.md");
        Files.createDirectories(missing.getParent());
        Files.writeString(missing, "---\nname: missing\n---\nbody");

        SkillRegistry registry = SkillRegistry.discover(new SkillDiscoveryOptions(
                home, temporary, false, true, List.of(), List.of()
        ));
        assertEquals(1, registry.skills().size());
        assertEquals(first.toAbsolutePath().normalize(),
                registry.skills().getFirst().source());
        assertTrue(registry.warnings().stream().anyMatch(
                value -> value.contains("lowercase letters")
        ));
        assertTrue(registry.warnings().stream().anyMatch(
                value -> value.contains("Duplicate skill")
        ));
        assertTrue(registry.warnings().stream().anyMatch(
                value -> value.contains("description is missing")
        ));
    }

    private static Path write(
            Path path,
            String name,
            String description,
            boolean disabled
    ) throws Exception {
        Files.createDirectories(path.getParent());
        Files.writeString(path, "---\nname: " + name
                + "\ndescription: " + description
                + "\nlicense: MIT\ncompatibility: JDK 25\n"
                + "metadata:\n  owner: test\n"
                + "allowed-tools: read bash\n"
                + "disable-model-invocation: " + disabled
                + "\n---\n# " + name + "\n\nBody");
        return path;
    }
}
