package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlMaintenanceReportTest {
    @TempDir
    Path temporary;

    @Test
    void reportsDataStagingAndStableLocksWithoutMutatingRepository() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("reported", null)
        ));
        join(session.append(new SessionEntryDraft.Message(
                "entry", UserMessage.text("entry", 1)
        )));
        Path target = findFile("reported");
        Path repair = Path.of(target + ".tmp");
        Path transaction = Path.of(target + "12345.txn.tmp");
        Files.writeString(repair, "repair");
        Files.writeString(transaction, "transaction");

        Path root = temporary.resolve("sessions").toAbsolutePath().normalize();
        Path unmatchedSessionLock = root.resolve("ghost.jsonl.lock");
        Files.writeString(unmatchedSessionLock, "");
        Path unmatchedIdLock = root.resolve(".writer-leases/orphan.lock");
        Files.createDirectories(unmatchedIdLock.getParent());
        Files.writeString(unmatchedIdLock, "");
        Path malformed = root.resolve("malformed.jsonl");
        Files.writeString(malformed, "not-json\n");
        Path ignored = root.resolve("notes.lock");
        Files.writeString(ignored, "ignored");

        List<Path> before = allFiles(root);
        JsonlSessionRepository.MaintenanceReport report =
                join(repository.inspectMaintenance());
        assertEquals(before, allFiles(root));
        assertEquals(8, report.artifacts().size());
        assertEquals(2, report.artifacts(
                JsonlSessionRepository.ArtifactKind.SESSION
        ).size());
        assertEquals(2, report.artifacts(
                JsonlSessionRepository.ArtifactKind.STAGING
        ).size());
        assertEquals(2, report.artifacts(
                JsonlSessionRepository.ArtifactKind.SESSION_LOCK
        ).size());
        assertEquals(2, report.artifacts(
                JsonlSessionRepository.ArtifactKind.SESSION_ID_LOCK
        ).size());
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.SESSION));
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.STAGING));
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.SESSION_LOCK));
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.SESSION_ID_LOCK));
        assertEquals(8, report.matched());
        assertFalse(report.truncated());

        var persisted = artifact(report, target);
        assertEquals(JsonlSessionRepository.ArtifactKind.SESSION, persisted.kind());
        assertEquals("reported", persisted.sessionId());
        assertEquals(target.toAbsolutePath().normalize(), persisted.target());
        assertTrue(persisted.associatedDataPresent());
        assertTrue(persisted.size() > 0);

        var malformedArtifact = artifact(report, malformed);
        assertNull(malformedArtifact.sessionId());
        assertTrue(malformedArtifact.associatedDataPresent());

        assertTrue(artifact(report, repair).associatedDataPresent());
        assertTrue(artifact(report, transaction).associatedDataPresent());
        assertTrue(artifact(report, Path.of(target + ".lock"))
                .associatedDataPresent());
        assertFalse(artifact(report, unmatchedSessionLock)
                .associatedDataPresent());

        var reportedIdLock = artifact(
                report, root.resolve(".writer-leases/reported.lock")
        );
        assertEquals("reported", reportedIdLock.sessionId());
        assertTrue(reportedIdLock.associatedDataPresent());
        assertEquals("orphan", artifact(report, unmatchedIdLock).sessionId());
        assertFalse(artifact(report, unmatchedIdLock).associatedDataPresent());
        assertTrue(report.artifacts().stream().noneMatch(value ->
                value.path().equals(ignored.toAbsolutePath().normalize())));
        assertThrows(UnsupportedOperationException.class, () ->
                report.artifacts().add(persisted));
    }

    @Test
    void boundedQueryFiltersDetailsButKeepsCompleteKindCounts() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession first = join(repository.create(
                new SessionRepository.CreateOptions("first", null)
        ));
        join(repository.create(new SessionRepository.CreateOptions("second", null)));
        join(repository.open(join(first.metadata())));

        JsonlSessionRepository.MaintenanceReport report = join(
                repository.inspectMaintenance(
                        new JsonlSessionRepository.MaintenanceQuery(
                                java.util.Set.of(
                                        JsonlSessionRepository.ArtifactKind.SESSION_LOCK,
                                        JsonlSessionRepository.ArtifactKind.SESSION_ID_LOCK
                                ),
                                1
                        )
                )
        );
        assertEquals(1, report.artifacts().size());
        assertEquals(4, report.matched());
        assertTrue(report.truncated());
        assertTrue(report.artifacts().getFirst().kind()
                == JsonlSessionRepository.ArtifactKind.SESSION_LOCK
                || report.artifacts().getFirst().kind()
                == JsonlSessionRepository.ArtifactKind.SESSION_ID_LOCK);
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.SESSION));
        assertEquals(0, report.count(JsonlSessionRepository.ArtifactKind.STAGING));
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.SESSION_LOCK));
        assertEquals(2, report.count(JsonlSessionRepository.ArtifactKind.SESSION_ID_LOCK));
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.MaintenanceQuery(null, 0));
    }

    @Test
    void inspectionDoesNotProbeOrWaitForWriterLease() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("leased", null)
        ));
        Path target = findFile("leased");
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(target)) {
            JsonlSessionRepository.MaintenanceReport report =
                    join(repository.inspectMaintenance());
            var lock = artifact(report, Path.of(target + ".lock"));
            assertTrue(lock.associatedDataPresent());
        }
        assertFalse(session.isClosed());
    }

    @Test
    void emptyRepositoryProducesEmptyReport() {
        JsonlSessionRepository.MaintenanceReport report =
                join(repository().inspectMaintenance());
        assertTrue(report.artifacts().isEmpty());
        assertEquals(0, report.matched());
        assertFalse(report.truncated());
        for (JsonlSessionRepository.ArtifactKind kind
                : JsonlSessionRepository.ArtifactKind.values()) {
            assertEquals(0, report.count(kind));
        }
    }

    private static JsonlSessionRepository.MaintenanceArtifact artifact(
            JsonlSessionRepository.MaintenanceReport report,
            Path path
    ) {
        Path expected = path.toAbsolutePath().normalize();
        return report.artifacts().stream()
                .filter(value -> value.path().equals(expected))
                .findFirst().orElseThrow();
    }

    private static List<Path> allFiles(Path root) throws Exception {
        try (var paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile)
                    .map(path -> path.toAbsolutePath().normalize())
                    .sorted().toList();
        }
    }

    private Path findFile(String id) throws Exception {
        try (var paths = Files.walk(temporary)) {
            return paths.filter(path -> path.getFileName().toString()
                            .endsWith("_" + id + ".jsonl"))
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
