package io.github.idoly.pi.agent.skill;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** Skill roots and trust policy for one host application context. */
public record SkillDiscoveryOptions(
        Path home,
        Path cwd,
        boolean projectTrusted,
        boolean discoverDefaults,
        List<Path> explicitPaths,
        List<Path> packagePaths
) {
    public SkillDiscoveryOptions {
        home = Objects.requireNonNull(home, "home")
                .toAbsolutePath().normalize();
        cwd = Objects.requireNonNull(cwd, "cwd")
                .toAbsolutePath().normalize();
        explicitPaths = normalize(explicitPaths);
        packagePaths = normalize(packagePaths);
    }

    public static SkillDiscoveryOptions defaults(Path cwd) {
        return new SkillDiscoveryOptions(
                Path.of(System.getProperty("user.home")), cwd,
                false, true, List.of(), List.of()
        );
    }

    private static List<Path> normalize(List<Path> paths) {
        if (paths == null) return List.of();
        return paths.stream()
                .map(path -> Objects.requireNonNull(path, "skill path")
                        .toAbsolutePath().normalize())
                .toList();
    }
}
