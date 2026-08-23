package io.github.idoly.pi.agent.extension;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

/** Discovers trusted Java extension JARs and owns their reloadable classloader. */
public final class ExtensionLoader {
    private ExtensionLoader() {
    }

    public static CompletionStage<LoadedRuntime> load(
            ExtensionContext context,
            ExtensionLoadOptions options
    ) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(options, "options");
        List<Path> jars;
        try {
            jars = discover(options);
        } catch (IOException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        URLClassLoader loader = new URLClassLoader(
                jars.stream().map(ExtensionLoader::url).toArray(URL[]::new),
                AgentExtension.class.getClassLoader()
        );
        return ExtensionRuntime.loadServices(context, loader)
                .handle((runtime, failure) -> {
                    if (failure != null) {
                        try {
                            loader.close();
                        } catch (IOException closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                    return new LoadedRuntime(runtime, loader, options);
                });
    }

    public static List<Path> discover(ExtensionLoadOptions options)
            throws IOException {
        LinkedHashSet<Path> values = new LinkedHashSet<>();
        if (options.discoverDefaults()) {
            scan(options.home().resolve(".pi/agent/extensions"), values);
            if (options.projectTrusted()) {
                scan(options.cwd().resolve(".pi/extensions"), values);
            }
        }
        for (Path path : options.explicitPaths()) {
            if (Files.isRegularFile(path) && isJar(path)) {
                values.add(path.toRealPath());
            } else {
                scan(path, values);
            }
        }
        return List.copyOf(values);
    }

    private static void scan(Path root, LinkedHashSet<Path> values)
            throws IOException {
        if (!Files.exists(root)) return;
        if (Files.isRegularFile(root)) {
            if (isJar(root)) values.add(root.toRealPath());
            return;
        }
        try (Stream<Path> paths = Files.walk(root, 2)) {
            paths.filter(Files::isRegularFile)
                    .filter(ExtensionLoader::isJar)
                    .sorted()
                    .forEach(path -> {
                        try {
                            values.add(path.toRealPath());
                        } catch (IOException failure) {
                            throw new ScanFailure(failure);
                        }
                    });
        } catch (ScanFailure failure) {
            throw failure.cause;
        } catch (java.io.UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    private static boolean isJar(Path path) {
        return path.getFileName().toString().toLowerCase(java.util.Locale.ROOT)
                .endsWith(".jar");
    }

    private static URL url(Path path) {
        try {
            return path.toUri().toURL();
        } catch (java.net.MalformedURLException failure) {
            throw new IllegalArgumentException("Invalid extension path " + path, failure);
        }
    }

    public static final class LoadedRuntime implements AutoCloseable {
        private final ExtensionRuntime runtime;
        private final URLClassLoader loader;
        private final ExtensionLoadOptions options;
        private boolean closed;

        private LoadedRuntime(
                ExtensionRuntime runtime,
                URLClassLoader loader,
                ExtensionLoadOptions options
        ) {
            this.runtime = runtime;
            this.loader = loader;
            this.options = options;
        }

        public ExtensionRuntime runtime() {
            if (closed) throw new IllegalStateException("Extensions are closed");
            return runtime;
        }

        public CompletionStage<LoadedRuntime> reload(ExtensionContext context) {
            close();
            return ExtensionLoader.load(context, options);
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            RuntimeException failure = null;
            try {
                runtime.close();
            } catch (RuntimeException error) {
                failure = error;
            }
            try {
                loader.close();
            } catch (IOException error) {
                if (failure == null) failure = new IllegalStateException(
                        "Failed to close extension classloader", error
                );
                else failure.addSuppressed(error);
            }
            if (failure != null) throw failure;
        }
    }

    private static final class ScanFailure extends RuntimeException {
        private final IOException cause;
        private ScanFailure(IOException cause) {
            this.cause = cause;
        }
    }
}
