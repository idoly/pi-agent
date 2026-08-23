package io.github.idoly.pi.agent.session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlRecoveryCheckpointTest {
    @TempDir
    Path temporary;

    @Test
    void durableInventoryDetectsChangedMissingAndAddedGenerations() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession idle = create(repository, "checkpoint-idle");
        SessionMetadata idleMetadata = join(idle.metadata());
        AgentSession active = create(repository, "checkpoint-active");
        join(active.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        assertTrue(active.rawState().claimOperationExecution("main", "run"));
        Path malformed = temporary.resolve("sessions/checkpoint-malformed.jsonl");
        Files.createDirectories(malformed.getParent());
        Files.writeString(malformed, "not-json\n{", StandardCharsets.UTF_8);
        Path destination = temporary.resolve("audit/recovery-checkpoint.json");

        JsonlSessionRepository.RecoveryCheckpoint checkpoint;
        try {
            checkpoint = join(repository.createRecoveryCheckpoint(destination));
        } finally {
            active.rawState().releaseOperationExecution("main", "run");
        }
        assertEquals(1, checkpoint.version());
        assertEquals(3, checkpoint.generations().size());
        assertEquals(temporary.resolve("sessions").toAbsolutePath().normalize(),
                checkpoint.repositoryRoot());
        assertEquals(java.util.Arrays.asList(
                "checkpoint-active", "checkpoint-idle", null
        ), checkpoint.generations().stream()
                .map(JsonlSessionRepository.RecoveryGenerationFingerprint::sessionId)
                .sorted(java.util.Comparator.nullsLast(String::compareTo)).toList());
        JsonlSessionRepository.RecoveryGenerationFingerprint malformedFingerprint =
                checkpoint.generations().stream()
                        .filter(value -> value.relativePath()
                                .equals("checkpoint-malformed.jsonl"))
                        .findFirst().orElseThrow();
        assertEquals(null, malformedFingerprint.sessionId());
        assertEquals(null, malformedFingerprint.tailSequence());
        assertEquals(64, malformedFingerprint.sha256().length());
        assertTrue(Files.exists(Path.of(destination + ".lock")));
        assertFalse(Files.exists(Path.of(destination + ".tmp")));
        byte[] manifest = Files.readAllBytes(destination);
        assertEquals(checkpoint, join(repository.readRecoveryCheckpoint(destination)));

        JsonlSessionRepository.RecoveryCheckpointReport unchanged = join(
                repository.verifyRecoveryCheckpoint(checkpoint, null)
        );
        assertEquals(3, unchanged.count(
                JsonlSessionRepository.CheckpointStatus.UNCHANGED
        ));
        assertEquals(0, unchanged.matched());
        assertTrue(unchanged.details().isEmpty());
        assertFalse(unchanged.truncated());

        join(active.name("changed"));
        join(repository.delete(idleMetadata));
        create(repository, "checkpoint-added");
        JsonlSessionRepository.RecoveryCheckpointReport bounded = join(
                repository.verifyRecoveryCheckpoint(checkpoint, 2)
        );
        assertEquals(1, bounded.count(
                JsonlSessionRepository.CheckpointStatus.UNCHANGED
        ));
        assertEquals(1, bounded.count(
                JsonlSessionRepository.CheckpointStatus.CHANGED
        ));
        assertEquals(1, bounded.count(
                JsonlSessionRepository.CheckpointStatus.MISSING
        ));
        assertEquals(1, bounded.count(
                JsonlSessionRepository.CheckpointStatus.ADDED
        ));
        assertEquals(3, bounded.matched());
        assertEquals(2, bounded.details().size());
        assertTrue(bounded.truncated());

        JsonlSessionRepository.RecoveryCheckpointReport complete = join(
                repository.verifyRecoveryCheckpoint(
                        join(repository.readRecoveryCheckpoint(destination)), null
                )
        );
        assertEquals(List.of(
                JsonlSessionRepository.CheckpointStatus.CHANGED,
                JsonlSessionRepository.CheckpointStatus.MISSING,
                JsonlSessionRepository.CheckpointStatus.ADDED
        ), complete.details().stream()
                .map(JsonlSessionRepository.RecoveryCheckpointDetail::status)
                .sorted().toList());
        JsonlSessionRepository.RecoveryCheckpointDetail changed =
                complete.details().stream()
                        .filter(value -> value.status()
                                == JsonlSessionRepository.CheckpointStatus.CHANGED)
                        .findFirst().orElseThrow();
        assertTrue(changed.current().tailSequence()
                > changed.expected().tailSequence());
        assertTrue(java.util.Arrays.equals(manifest, Files.readAllBytes(destination)),
                "verification modified the durable checkpoint");

        JsonlSessionRepository.RecoveryCheckpointReport firstPage = join(
                repository.verifyRecoveryCheckpointPage(
                        checkpoint,
                        new JsonlSessionRepository.RecoveryCheckpointQuery(2, null)
                )
        );
        assertEquals(3, firstPage.matched());
        assertEquals(2, firstPage.details().size());
        assertTrue(firstPage.truncated());
        JsonlSessionRepository.RecoveryCheckpointDetail last =
                firstPage.details().getLast();
        assertEquals(new JsonlSessionRepository.RecoveryCheckpointCursor(
                last.relativePath(), last.status()
        ), firstPage.nextCursor());
        JsonlSessionRepository.RecoveryCheckpointReport secondPage = join(
                repository.verifyRecoveryCheckpointPage(
                        checkpoint,
                        new JsonlSessionRepository.RecoveryCheckpointQuery(
                                2, firstPage.nextCursor()
                        )
                )
        );
        assertEquals(1, secondPage.matched());
        assertEquals(1, secondPage.details().size());
        assertFalse(secondPage.truncated());
        assertEquals(null, secondPage.nextCursor());
        assertEquals(complete.details(), java.util.stream.Stream.concat(
                firstPage.details().stream(), secondPage.details().stream()
        ).toList());
        for (JsonlSessionRepository.CheckpointStatus status
                : JsonlSessionRepository.CheckpointStatus.values()) {
            assertEquals(complete.count(status), secondPage.count(status));
        }

        JsonlSessionRepository.RecoveryCheckpointScanCursor scanCursor = null;
        java.util.ArrayList<JsonlSessionRepository.RecoveryCheckpointDetail>
                batchDetails = new java.util.ArrayList<>();
        java.util.EnumMap<JsonlSessionRepository.CheckpointStatus, Long> batchCounts =
                new java.util.EnumMap<>(JsonlSessionRepository.CheckpointStatus.class);
        for (JsonlSessionRepository.CheckpointStatus status
                : JsonlSessionRepository.CheckpointStatus.values()) {
            batchCounts.put(status, 0L);
        }
        int inspected = 0;
        int batches = 0;
        while (true) {
            JsonlSessionRepository.RecoveryCheckpointBatchReport batch = join(
                    repository.verifyRecoveryCheckpointBatch(
                            checkpoint,
                            new JsonlSessionRepository.RecoveryCheckpointScanQuery(
                                    JsonlSessionRepository.RecoveryCheckpointQuery.ALL,
                                    scanCursor, 2, null
                            )
                    )
            );
            batches++;
            inspected += batch.generationsInspected();
            batchDetails.addAll(batch.verification().details());
            for (JsonlSessionRepository.CheckpointStatus status
                    : JsonlSessionRepository.CheckpointStatus.values()) {
                batchCounts.compute(status, (ignored, count) -> count
                        + batch.verification().count(status));
            }
            if (batch.scanComplete()) {
                assertEquals(null, batch.nextScanCursor());
                break;
            }
            assertTrue(batch.nextScanCursor() != null);
            if (scanCursor != null) {
                assertTrue(batch.nextScanCursor().relativePath()
                        .compareTo(scanCursor.relativePath()) > 0);
            }
            scanCursor = batch.nextScanCursor();
        }
        assertEquals(2, batches);
        assertEquals(4, inspected);
        assertEquals(complete.details(), batchDetails);
        for (JsonlSessionRepository.CheckpointStatus status
                : JsonlSessionRepository.CheckpointStatus.values()) {
            assertEquals(complete.count(status), batchCounts.get(status));
        }

        JsonlSessionRepository.RecoveryCheckpointBatchReport zeroDuration = join(
                repository.verifyRecoveryCheckpointBatch(
                        checkpoint,
                        new JsonlSessionRepository.RecoveryCheckpointScanQuery(
                                JsonlSessionRepository.RecoveryCheckpointQuery.ALL,
                                null, 4, java.time.Duration.ZERO
                        )
                )
        );
        assertEquals(1, zeroDuration.generationsInspected());
        assertFalse(zeroDuration.scanComplete());
        assertTrue(zeroDuration.nextScanCursor() != null);

        java.util.ArrayList<JsonlSessionRepository.RecoveryCheckpointDetail>
                orchestrated = new java.util.ArrayList<>();
        java.util.concurrent.atomic.AtomicReference<RecoveryCheckpointVerifier.State>
                durableState = new java.util.concurrent.atomic.AtomicReference<>();
        CompletionException interrupted = assertThrows(
                CompletionException.class,
                () -> RecoveryCheckpointVerifier.verify(
                        repository, checkpoint,
                        new RecoveryCheckpointVerifier.Options(4, 1),
                        orchestrated::add,
                        state -> {
                            durableState.set(state);
                            throw new IllegalStateException("simulated stop");
                        }
                ).toCompletableFuture().join()
        );
        assertInstanceOf(IllegalStateException.class, interrupted.getCause());
        assertEquals(1, orchestrated.size());
        assertTrue(durableState.get().currentBatchCounted());
        assertTrue(durableState.get().detailAfter() != null);

        RecoveryCheckpointVerifier.Result resumed = join(
                RecoveryCheckpointVerifier.resume(
                        repository, checkpoint,
                        new RecoveryCheckpointVerifier.Options(4, 1),
                        durableState.get(), orchestrated::add, durableState::set
                )
        );
        assertEquals(complete.details(), orchestrated);
        assertEquals(4, resumed.generationsInspected());
        for (JsonlSessionRepository.CheckpointStatus status
                : JsonlSessionRepository.CheckpointStatus.values()) {
            assertEquals(complete.count(status), resumed.count(status));
        }
        assertTrue(durableState.get().complete());
        RecoveryCheckpointVerifier.Result alreadyComplete = join(
                RecoveryCheckpointVerifier.resume(
                        repository, checkpoint,
                        new RecoveryCheckpointVerifier.Options(4, 1),
                        durableState.get(),
                        ignored -> { throw new AssertionError("detail redelivered"); },
                        ignored -> { throw new AssertionError("progress repeated"); }
                )
        );
        assertEquals(resumed, alreadyComplete);
    }

    @Test
    @Timeout(45)
    void boundedVerifierCoversManyGenerationsAcrossScanAndDetailPages()
            throws Exception {
        JsonlSessionRepository repository = repository();
        java.util.ArrayList<AgentSession> sessions = new java.util.ArrayList<>();
        for (int index = 0; index < 128; index++) {
            sessions.add(create(repository, "checkpoint-scale-" + index));
        }
        JsonlSessionRepository.RecoveryCheckpoint checkpoint = join(
                repository.createRecoveryCheckpoint(
                        temporary.resolve("audit/checkpoint-scale.json")
                )
        );
        for (int index = 0; index < sessions.size(); index += 16) {
            join(sessions.get(index).name("changed-" + index));
        }

        java.util.ArrayList<JsonlSessionRepository.RecoveryCheckpointDetail> details =
                new java.util.ArrayList<>();
        RecoveryCheckpointVerifier.Result result = join(
                RecoveryCheckpointVerifier.verify(
                        repository, checkpoint,
                        new RecoveryCheckpointVerifier.Options(17, 3),
                        details::add, ignored -> { }
                )
        );
        assertEquals(128, result.generationsInspected());
        assertEquals(120, result.count(
                JsonlSessionRepository.CheckpointStatus.UNCHANGED
        ));
        assertEquals(8, result.count(
                JsonlSessionRepository.CheckpointStatus.CHANGED
        ));
        assertEquals(8, details.size());
        assertTrue(details.stream().allMatch(detail -> detail.status()
                == JsonlSessionRepository.CheckpointStatus.CHANGED));
    }

    @Test
    @Timeout(60)
    void checkpointCaptureRemainsValidDuringCrossJvmMutation() throws Exception {
        JsonlSessionRepository repository = repository();
        int rounds = 8;
        for (int round = 0; round < rounds; round++) {
            create(repository, "checkpoint-append-" + round);
            create(repository, "checkpoint-delete-" + round);
        }
        Path control = Files.createDirectories(temporary.resolve("checkpoint-control"));
        Process child = start(
                "checkpoint-mutate-loop",
                temporary.resolve("sessions").toString(),
                temporary.toString(), Integer.toString(rounds), control.toString()
        );
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int round = 0; round < rounds; round++) {
                    int currentRound = round;
                    Path destination = temporary.resolve(
                            "audit/checkpoint-race-" + round + ".json"
                    );
                    var capture = executor.submit(() -> join(
                            repository.createRecoveryCheckpoint(destination)
                    ));
                    Files.writeString(control.resolve("go-" + round), "go");
                    await(control.resolve("done-" + round));
                    JsonlSessionRepository.RecoveryCheckpoint checkpoint =
                            capture.get(10, TimeUnit.SECONDS);
                    assertEquals(checkpoint,
                            join(repository.readRecoveryCheckpoint(destination)));
                    assertEquals(checkpoint.generations().size(), checkpoint.generations()
                            .stream().map(JsonlSessionRepository
                                    .RecoveryGenerationFingerprint::relativePath)
                            .distinct().count());
                    checkpoint.generations().forEach(fingerprint -> {
                        assertEquals(64, fingerprint.sha256().length());
                        assertTrue(fingerprint.size() > 0);
                    });

                    JsonlSessionRepository.RecoveryCheckpointReport report = join(
                            repository.verifyRecoveryCheckpoint(checkpoint, null)
                    );
                    report.details().forEach(detail -> {
                        String expectedId = detail.expected() == null
                                ? null : detail.expected().sessionId();
                        String currentId = detail.current() == null
                                ? null : detail.current().sessionId();
                        switch (detail.status()) {
                            case CHANGED -> assertEquals(
                                    "checkpoint-append-" + currentRound, expectedId
                            );
                            case MISSING -> assertEquals(
                                    "checkpoint-delete-" + currentRound, expectedId
                            );
                            case ADDED -> assertEquals(
                                    "checkpoint-added-" + currentRound, currentId
                            );
                            case UNCHANGED -> throw new AssertionError(
                                    "unchanged generations have no drift detail"
                            );
                        }
                    });
                    assertTrue(report.count(
                            JsonlSessionRepository.CheckpointStatus.CHANGED
                    ) <= 1);
                    assertTrue(report.count(
                            JsonlSessionRepository.CheckpointStatus.MISSING
                    ) <= 1);
                    assertTrue(report.count(
                            JsonlSessionRepository.CheckpointStatus.ADDED
                    ) <= 1);
                }
            }
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void checkpointRejectsWrongRepositoryUnsafeDestinationAndInvalidLimit() {
        JsonlSessionRepository repository = repository();
        create(repository, "checkpoint-validation");
        assertFailure(IllegalArgumentException.class,
                repository.createRecoveryCheckpoint(
                        temporary.resolve("sessions/inside.json")
                ));
        JsonlSessionRepository.RecoveryCheckpoint checkpoint = join(
                repository.createRecoveryCheckpoint(
                        temporary.resolve("outside/checkpoint.json")
                )
        );
        JsonlSessionRepository other = new JsonlSessionRepository(
                temporary.resolve("other-sessions"), temporary
        );
        assertFailure(IllegalArgumentException.class,
                other.verifyRecoveryCheckpoint(checkpoint, null));
        assertFailure(IllegalArgumentException.class,
                repository.verifyRecoveryCheckpoint(checkpoint, 0));
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.RecoveryCheckpointScanQuery(
                        null, null, 0, null
                ));
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.RecoveryCheckpointScanQuery(
                        null, null, 1, java.time.Duration.ofNanos(-1)
                ));
    }

    private static void assertFailure(
            Class<? extends Throwable> type,
            java.util.concurrent.CompletionStage<?> stage
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class, () -> stage.toCompletableFuture().join()
        );
        assertInstanceOf(type, failure.getCause());
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
        return new ProcessBuilder(command).redirectErrorStream(true).start();
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

    private static String processOutput(Process process) {
        try {
            return new String(
                    process.getInputStream().readAllBytes(), StandardCharsets.UTF_8
            );
        } catch (Exception ignored) {
            return "<process output unavailable>";
        }
    }

    private AgentSession create(JsonlSessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private JsonlSessionRepository repository() {
        return new JsonlSessionRepository(temporary.resolve("sessions"), temporary);
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
