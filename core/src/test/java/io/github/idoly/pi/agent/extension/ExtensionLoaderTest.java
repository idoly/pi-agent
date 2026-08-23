package io.github.idoly.pi.agent.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExtensionLoaderTest {
    @TempDir
    Path temporary;

    @Test
    void discoversGlobalExplicitAndOnlyTrustedProjectJars() throws Exception {
        Path home = Files.createDirectories(temporary.resolve("home"));
        Path project = Files.createDirectories(temporary.resolve("project"));
        Path global = jar(home.resolve(".pi/agent/extensions/global.jar"));
        Path nested = jar(home.resolve(".pi/agent/extensions/group/nested.jar"));
        Path local = jar(project.resolve(".pi/extensions/local.jar"));
        Path explicit = jar(temporary.resolve("explicit.jar"));

        ExtensionLoadOptions untrusted = new ExtensionLoadOptions(
                home, project, false, true, List.of(explicit)
        );
        assertEquals(List.of(
                global.toRealPath(), nested.toRealPath(), explicit.toRealPath()
        ), ExtensionLoader.discover(untrusted));

        ExtensionLoadOptions trusted = new ExtensionLoadOptions(
                home, project, true, true, List.of(explicit)
        );
        assertEquals(List.of(
                global.toRealPath(), nested.toRealPath(), local.toRealPath(),
                explicit.toRealPath()
        ), ExtensionLoader.discover(trusted));
    }

    private static Path jar(Path path) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[]{0});
        return path;
    }
}
