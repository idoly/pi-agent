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

class JsonlOperationSignalCleanupTest {
    @TempDir
    Path temporary;

    @Test
    void deletesOnlyOldUnassociatedSignalsAndRetainsEveryLock() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = openRun(repository, "signal-cleanup", "run");
        SessionMetadata metadata = join(session.metadata());
        assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
        JsonlSessionRepository.MaintenanceArtifact signal = signal(repository);
        Path marker = signal.path();
        Path operationLock = Path.of(marker.toString()
                .substring(0, marker.toString().length() - ".abort".length())
                + ".lock");
        Path generationLock = Path.of(signal.target() + ".lock");

        assertEquals(
                new JsonlSessionRepository.OperationSignalCleanupResult(1, 0, 1, 0),
                join(repository.cleanupUnassociatedOperationSignals())
        );
        assertTrue(Files.exists(marker));

        join(repository.delete(metadata));
        assertEquals(
                new JsonlSessionRepository.OperationSignalCleanupResult(1, 0, 0, 1),
                join(repository.cleanupUnassociatedOperationSignals(
                        new JsonlSessionRepository.OperationSignalCleanupPolicy(
                                Duration.ofDays(1)
                        )
                ))
        );
        Files.setLastModifiedTime(marker, FileTime.from(Instant.EPOCH));
        Path malformed = marker.getParent().resolve("not-a-digest.abort");
        Files.writeString(malformed, "unknown");

        assertEquals(
                new JsonlSessionRepository.OperationSignalCleanupResult(1, 1, 0, 0),
                join(repository.cleanupUnassociatedOperationSignals(
                        new JsonlSessionRepository.OperationSignalCleanupPolicy(
                                Duration.ofDays(1)
                        )
                ))
        );
        assertFalse(Files.exists(marker));
        assertTrue(Files.exists(operationLock));
        assertTrue(Files.exists(generationLock));
        assertTrue(Files.exists(malformed));
        assertEquals(0, join(repository.inspectMaintenance()).count(
                JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL
        ));
        assertTrue(join(repository.inspectMaintenance()).count(
                JsonlSessionRepository.ArtifactKind.OPERATION_LOCK
        ) >= 1);
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.OperationSignalCleanupPolicy(
                        Duration.ofSeconds(-1)
                ));
    }

    @Test
    @Timeout(20)
    void leaseAcquisitionRechecksMarkerAgeAfterOwnerRefresh() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = openRun(repository, "signal-refresh", "run");
        SessionMetadata metadata = join(session.metadata());
        assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
        Path marker = signal(repository).path();
        join(repository.delete(metadata));
        Files.setLastModifiedTime(marker, FileTime.from(Instant.EPOCH));
        Path operationBase = Path.of(marker.toString().substring(
                0, marker.toString().length() - ".abort".length()
        ));

        JsonlSessionRepository.OperationSignalCleanupResult result;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            java.util.concurrent.Future<
                    JsonlSessionRepository.OperationSignalCleanupResult
                    > cleanup;
            try (JsonlWriterLease ignored = JsonlWriterLease.acquire(operationBase)) {
                cleanup = executor.submit(() -> join(
                        repository.cleanupUnassociatedOperationSignals(
                                new JsonlSessionRepository.OperationSignalCleanupPolicy(
                                        Duration.ofHours(1)
                                )
                        )
                ));
                Thread.sleep(200);
                assertFalse(cleanup.isDone());
                Files.writeString(marker, "refreshed");
            }
            result = cleanup.get(10, TimeUnit.SECONDS);
        }
        assertEquals(new JsonlSessionRepository.OperationSignalCleanupResult(
                1, 0, 0, 1
        ), result);
        assertTrue(Files.exists(marker));
    }

    @Test
    void emptyRepositoryProducesEmptyResult() {
        assertEquals(new JsonlSessionRepository.OperationSignalCleanupResult(
                0, 0, 0, 0
        ), join(repository().cleanupUnassociatedOperationSignals()));
    }

    private static AgentSession openRun(
            JsonlSessionRepository repository,
            String id,
            String runId
    ) {
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions(id, null)
        ));
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                runId, "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        return session;
    }

    private static JsonlSessionRepository.MaintenanceArtifact signal(
            JsonlSessionRepository repository
    ) {
        return join(repository.inspectMaintenance()).artifacts().stream()
                .filter(value -> value.kind()
                        == JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL)
                .findFirst().orElseThrow();
    }

    private JsonlSessionRepository repository() {
        return new JsonlSessionRepository(temporary.resolve("sessions"), temporary);
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
