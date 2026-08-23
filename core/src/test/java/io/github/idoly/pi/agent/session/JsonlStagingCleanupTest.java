package io.github.idoly.pi.agent.session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlStagingCleanupTest {
    @TempDir
    Path temporary;

    @Test
    void removesOnlyRecognizedStagingFilesAndIsIdempotent() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("cleanup", null)
        ));
        join(repository.open(join(session.metadata())));
        Path target = onlySessionFile();
        Path repair = Path.of(target + ".tmp");
        Path transaction = Path.of(target + "123456789.txn.tmp");
        Path unknown = target.getParent().resolve("notes.tmp");
        Path malformedTransaction = Path.of(target + "writer.txn.tmp");
        Files.writeString(repair, "repair");
        Files.writeString(transaction, "transaction");
        Files.writeString(unknown, "unknown");
        Files.writeString(malformedTransaction, "unknown");
        assertTrue(Files.exists(Path.of(target + ".lock")),
                "session creation/open should establish a stable lock file");

        JsonlSessionRepository.CleanupResult first =
                join(repository.cleanupOrphanedStaging());
        assertEquals(new JsonlSessionRepository.CleanupResult(2, 2), first);
        assertFalse(Files.exists(repair));
        assertFalse(Files.exists(transaction));
        assertTrue(Files.exists(target));
        assertTrue(Files.exists(Path.of(target + ".lock")));
        assertTrue(Files.exists(unknown));
        assertTrue(Files.exists(malformedTransaction));
        assertEquals(List.of("cleanup"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());
        assertFalse(session.isClosed());

        assertEquals(new JsonlSessionRepository.CleanupResult(0, 0),
                join(repository.cleanupOrphanedStaging()));
    }

    @Test
    void retentionPolicyDeletesOnlyStagingOldEnoughAtLeaseTime() throws Exception {
        JsonlSessionRepository repository = repository();
        join(repository.create(new SessionRepository.CreateOptions("retention", null)));
        Path target = onlySessionFile();
        Path old = Path.of(target + ".tmp");
        Path fresh = Path.of(target + "123.txn.tmp");
        Files.writeString(old, "old");
        Files.writeString(fresh, "fresh");
        Files.setLastModifiedTime(old, FileTime.from(
                Instant.now().minus(Duration.ofHours(2))
        ));

        JsonlSessionRepository.CleanupResult result = join(
                repository.cleanupOrphanedStaging(
                        new JsonlSessionRepository.StagingCleanupPolicy(
                                Duration.ofHours(1)
                        )
                )
        );
        assertEquals(new JsonlSessionRepository.CleanupResult(2, 1), result);
        assertFalse(Files.exists(old));
        assertTrue(Files.exists(fresh));
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.StagingCleanupPolicy(Duration.ofSeconds(-1)));
    }

    @Test
    void emptyRepositoryCleanupIsANoOp() {
        assertEquals(new JsonlSessionRepository.CleanupResult(0, 0),
                join(repository().cleanupOrphanedStaging()));
    }

    @Test
    @Timeout(30)
    void cleanupWaitsForTargetLeaseHeldByAnotherJvm() throws Exception {
        JsonlSessionRepository repository = repository();
        join(repository.create(new SessionRepository.CreateOptions("leased", null)));
        Path target = onlySessionFile();
        Path staged = Path.of(target + ".tmp");
        Files.writeString(staged, "staged");
        Path control = Files.createDirectories(temporary.resolve("control"));
        Process child = start("hold", target.toString(), control.toString());
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var cleanup = executor.submit(() ->
                        join(repository.cleanupOrphanedStaging()));
                Thread.sleep(250);
                assertFalse(cleanup.isDone(),
                        "cleanup deleted staging while another JVM held target lease");
                assertTrue(Files.exists(staged));
                Files.writeString(control.resolve("release"), "release");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS));
                assertEquals(0, child.exitValue());
                assertEquals(new JsonlSessionRepository.CleanupResult(1, 1),
                        cleanup.get(10, TimeUnit.SECONDS));
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        assertFalse(Files.exists(staged));
    }

    @Test
    @Timeout(30)
    void retentionAgeIsRecheckedAfterWaitingForTargetLease() throws Exception {
        JsonlSessionRepository repository = repository();
        join(repository.create(new SessionRepository.CreateOptions("refreshed", null)));
        Path target = onlySessionFile();
        Path staged = Path.of(target + ".tmp");
        Files.writeString(staged, "staged");
        Files.setLastModifiedTime(staged, FileTime.from(
                Instant.now().minus(Duration.ofHours(2))
        ));
        Path control = Files.createDirectories(temporary.resolve("refresh-control"));
        Process child = start("hold", target.toString(), control.toString());
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var cleanup = executor.submit(() -> join(
                        repository.cleanupOrphanedStaging(
                                new JsonlSessionRepository.StagingCleanupPolicy(
                                        Duration.ofHours(1)
                                )
                        )
                ));
                Thread.sleep(250);
                assertFalse(cleanup.isDone());
                Files.setLastModifiedTime(staged, FileTime.from(Instant.now()));
                Files.writeString(control.resolve("release"), "release");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS));
                assertEquals(0, child.exitValue());
                assertEquals(new JsonlSessionRepository.CleanupResult(1, 0),
                        cleanup.get(10, TimeUnit.SECONDS));
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        assertTrue(Files.exists(staged));
    }

    private Process start(String... arguments) throws Exception {
        String java = Path.of(
                System.getProperty("java.home"), "bin", "java"
        ).toString();
        String classPath = System.getProperty(
                "surefire.test.class.path", System.getProperty("java.class.path")
        );
        java.util.ArrayList<String> command = new java.util.ArrayList<>();
        command.add(java);
        command.add("-cp");
        command.add(classPath);
        command.add(JsonlLeaseProcess.class.getName());
        command.addAll(List.of(arguments));
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(temporary.resolve("cleanup-process.log").toFile())
                .start();
    }

    private static void await(Path signal) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!Files.exists(signal)) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Timed out waiting for " + signal);
            }
            Thread.sleep(10);
        }
    }

    private Path onlySessionFile() throws Exception {
        try (var paths = Files.walk(temporary)) {
            return paths.filter(path -> path.toString().endsWith(".jsonl"))
                    .findFirst().orElseThrow();
        }
    }

    private JsonlSessionRepository repository() {
        return new JsonlSessionRepository(temporary.resolve("sessions"), temporary);
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
