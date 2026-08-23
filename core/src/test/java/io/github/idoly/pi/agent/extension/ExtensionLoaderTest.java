package io.github.idoly.pi.agent.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.ToolProvider;

import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.ProviderRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void loadsServiceJarAndReloadsLifecycleExactlyOnce() throws Exception {
        Path extensionJar = compileExtensionJar(temporary.resolve("compiled"));
        ExtensionContext context = new ExtensionContext(
                temporary, null, new ProviderRegistry(),
                CancellationSignal.NONE, Map.of()
        );
        ExtensionLoadOptions options = new ExtensionLoadOptions(
                temporary, temporary, false, false, List.of(extensionJar)
        );
        ExtensionLoader.LoadedRuntime loaded = ExtensionLoader.load(
                context, options
        ).toCompletableFuture().join();
        loaded.runtime().startSession().toCompletableFuture().join();
        ExtensionLoader.LoadedRuntime reloaded = loaded.reload(context)
                .toCompletableFuture().join();
        reloaded.runtime().startSession().toCompletableFuture().join();
        reloaded.close();

        assertEquals("xx", Files.readString(temporary.resolve("starts")));
        assertEquals("xx", Files.readString(temporary.resolve("stops")));
    }

    private static Path compileExtensionJar(Path root) throws Exception {
        Path source = root.resolve("src/example/ReloadExtension.java");
        Path classes = root.resolve("classes");
        Files.createDirectories(source.getParent());
        Files.createDirectories(classes);
        Files.writeString(source, """
                package example;
                import io.github.idoly.pi.agent.extension.*;
                import java.nio.file.*;
                import java.util.concurrent.*;
                public final class ReloadExtension implements AgentExtension {
                    public String id() { return "reload-test"; }
                    public void configure(ExtensionApi api) {
                        api.onSessionStart(context -> append(
                            context.cwd().resolve("starts")));
                        api.onSessionShutdown(context -> append(
                            context.cwd().resolve("stops")));
                    }
                    private static CompletionStage<Void> append(Path path) {
                        try {
                            Files.writeString(path, "x",
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND);
                            return CompletableFuture.completedFuture(null);
                        } catch (Exception failure) {
                            return CompletableFuture.failedFuture(failure);
                        }
                    }
                }
                """);
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertTrue(compiler != null, "JDK compiler is required");
        int result = compiler.run(
                null, null, null,
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()
        );
        assertEquals(0, result);
        Path jar = root.resolve("reload-extension.jar");
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(jar)
        )) {
            try (var files = Files.walk(classes)) {
                for (Path file : files.filter(Files::isRegularFile).toList()) {
                    String name = classes.relativize(file).toString()
                            .replace('\\', '/');
                    output.putNextEntry(new JarEntry(name));
                    output.write(Files.readAllBytes(file));
                    output.closeEntry();
                }
            }
            output.putNextEntry(new JarEntry(
                    "META-INF/services/"
                            + AgentExtension.class.getName()
            ));
            output.write("example.ReloadExtension\n".getBytes(
                    java.nio.charset.StandardCharsets.UTF_8
            ));
            output.closeEntry();
        }
        return jar;
    }

    private static Path jar(Path path) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[]{0});
        return path;
    }
}
