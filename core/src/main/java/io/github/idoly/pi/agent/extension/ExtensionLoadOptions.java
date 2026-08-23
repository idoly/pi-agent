package io.github.idoly.pi.agent.extension;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** Native Java extension JAR discovery roots and project trust. */
public record ExtensionLoadOptions(
        Path home,
        Path cwd,
        boolean projectTrusted,
        boolean discoverDefaults,
        List<Path> explicitPaths
) {
    public ExtensionLoadOptions {
        home = Objects.requireNonNull(home, "home")
                .toAbsolutePath().normalize();
        cwd = Objects.requireNonNull(cwd, "cwd")
                .toAbsolutePath().normalize();
        explicitPaths = explicitPaths == null ? List.of()
                : explicitPaths.stream().map(path -> path
                .toAbsolutePath().normalize()).toList();
    }

    public static ExtensionLoadOptions defaults(Path cwd) {
        return new ExtensionLoadOptions(
                Path.of(System.getProperty("user.home")), cwd,
                false, true, List.of()
        );
    }
}
