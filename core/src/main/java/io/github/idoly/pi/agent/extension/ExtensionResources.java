package io.github.idoly.pi.agent.extension;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Headless resources contributed by extensions during discovery. */
public record ExtensionResources(List<Path> skillPaths) {
    public static final ExtensionResources EMPTY = new ExtensionResources(List.of());

    public ExtensionResources {
        skillPaths = skillPaths == null ? List.of() : skillPaths.stream()
                .map(path -> path.toAbsolutePath().normalize()).toList();
    }

    public ExtensionResources merge(ExtensionResources other) {
        if (other == null || other.skillPaths().isEmpty()) return this;
        ArrayList<Path> merged = new ArrayList<>(skillPaths);
        for (Path path : other.skillPaths()) {
            if (!merged.contains(path)) merged.add(path);
        }
        return new ExtensionResources(merged);
    }

    public enum Reason {
        STARTUP,
        RELOAD
    }
}
