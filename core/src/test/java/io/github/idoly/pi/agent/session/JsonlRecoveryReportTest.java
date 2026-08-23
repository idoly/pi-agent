package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.Usage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlRecoveryReportTest {
    @TempDir
    Path temporary;

    @Test
    void aggregatesOpenOperationsAndCorruptionWithBoundedDetails() throws Exception {
        JsonlSessionRepository repository = repository();
        create(repository, "idle");
        openToolRun(repository, "tools", List.of(
                new StartedTool(0, "safe", SessionRecordDraft.Replay.SAFE),
                new StartedTool(1, "never", SessionRecordDraft.Replay.NEVER)
        ));
        AgentSession aborting = create(repository, "aborting");
        join(aborting.appendRecord(new SessionRecordDraft.OperationStarted(
                "aborting-run", "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        join(SessionRunOperation.requestAbort(aborting, "aborting-run"));

        Path corrupt = temporary.resolve("sessions/corrupt.jsonl");
        Files.createDirectories(corrupt.getParent());
        Files.writeString(corrupt, "{\"kind\":\"header\",\"version\":4,"
                + "\"id\":\"corrupt\",\"createdAt\":1,\"cwd\":\"/\"}\n{", StandardCharsets.UTF_8);
        byte[] before = Files.readAllBytes(corrupt);

        JsonlSessionRepository.RecoveryReport report = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        null, 1
                ))
        );
        assertEquals(4, report.sessionsScanned());
        assertEquals(2, report.count(
                JsonlSessionRepository.RecoveryKind.OPEN_OPERATION
        ));
        assertEquals(1, report.count(
                JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION
        ));
        assertEquals(1, report.unresolvedSafe());
        assertEquals(1, report.unresolvedNever());
        assertEquals(3, report.matched());
        assertEquals(1, report.details().size());
        assertTrue(report.truncated());
        assertEquals(List.of("aborting", "tools"), join(
                repository.inspectRecovery()
        ).operations().stream().map(value -> value.session().id()).sorted().toList());
        assertEquals(1, join(repository.inspectRecovery()).failures().size());
        assertTrue(java.util.Arrays.equals(before, Files.readAllBytes(corrupt)),
                "read-only recovery inspection repaired a corrupt tail");
    }

    @Test
    void scanBatchesBoundSessionReplayAndResumeByGenerationPath() throws Exception {
        JsonlSessionRepository repository = repository();
        create(repository, "batch-idle");
        for (String id : List.of("batch-a", "batch-b", "batch-c", "batch-d")) {
            openToolRun(repository, id, List.of(
                    new StartedTool(
                            0, id,
                            id.endsWith("a") || id.endsWith("c")
                                    ? SessionRecordDraft.Replay.NEVER
                                    : SessionRecordDraft.Replay.SAFE
                    )
            ));
        }
        Path corrupt = temporary.resolve("sessions/batch-corrupt.jsonl");
        Files.createDirectories(corrupt.getParent());
        Files.writeString(corrupt, "not-json\n", StandardCharsets.UTF_8);
        JsonlSessionRepository.RecoveryReport complete =
                join(repository.inspectRecovery());

        JsonlSessionRepository.RecoveryScanCursor cursor = null;
        ArrayList<JsonlSessionRepository.RecoveryDetail> details = new ArrayList<>();
        long scanned = 0;
        long operations = 0;
        long failures = 0;
        long safe = 0;
        long never = 0;
        int batches = 0;
        while (true) {
            JsonlSessionRepository.RecoveryBatchReport batch = join(
                    repository.inspectRecoveryBatch(
                            new JsonlSessionRepository.RecoveryScanQuery(
                                    JsonlSessionRepository.RecoveryQuery.ALL,
                                    cursor, 2, null
                            )
                    )
            );
            batches++;
            assertTrue(batch.recovery().sessionsScanned() <= 2);
            details.addAll(batch.recovery().details());
            scanned += batch.recovery().sessionsScanned();
            operations += batch.recovery().count(
                    JsonlSessionRepository.RecoveryKind.OPEN_OPERATION
            );
            failures += batch.recovery().count(
                    JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION
            );
            safe += batch.recovery().unresolvedSafe();
            never += batch.recovery().unresolvedNever();
            if (batch.scanComplete()) {
                assertEquals(null, batch.nextScanCursor());
                break;
            }
            assertTrue(batch.nextScanCursor() != null);
            if (cursor != null) {
                assertTrue(batch.nextScanCursor().generationPath().toString()
                        .compareTo(cursor.generationPath().toString()) > 0);
            }
            cursor = batch.nextScanCursor();
        }
        assertEquals(3, batches);
        assertEquals(complete.sessionsScanned(), scanned);
        assertEquals(complete.count(
                JsonlSessionRepository.RecoveryKind.OPEN_OPERATION), operations);
        assertEquals(complete.count(
                JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION), failures);
        assertEquals(complete.unresolvedSafe(), safe);
        assertEquals(complete.unresolvedNever(), never);
        assertEquals(complete.details().stream().map(JsonlRecoveryReportTest::key).toList(),
                details.stream().sorted(java.util.Comparator.comparing(
                        JsonlRecoveryReportTest::key
                )).map(JsonlRecoveryReportTest::key).toList());
    }

    @Test
    void zeroDurationInspectsOneGenerationAndDetailLimitRemainsIndependent() {
        JsonlSessionRepository repository = repository();
        openToolRun(repository, "budget-a", List.of(
                new StartedTool(0, "a", SessionRecordDraft.Replay.SAFE)
        ));
        openToolRun(repository, "budget-b", List.of(
                new StartedTool(0, "b", SessionRecordDraft.Replay.NEVER)
        ));
        openToolRun(repository, "budget-c", List.of(
                new StartedTool(0, "c", SessionRecordDraft.Replay.SAFE)
        ));

        JsonlSessionRepository.RecoveryBatchReport timed = join(
                repository.inspectRecoveryBatch(
                        new JsonlSessionRepository.RecoveryScanQuery(
                                new JsonlSessionRepository.RecoveryQuery(null, 1),
                                null, 3, java.time.Duration.ZERO
                        )
                )
        );
        assertEquals(1, timed.recovery().sessionsScanned());
        assertFalse(timed.scanComplete());
        assertTrue(timed.nextScanCursor() != null);
        assertEquals(1, timed.recovery().details().size());
        assertFalse(timed.recovery().truncated(),
                "one inspected generation had only one recovery detail");

        JsonlSessionRepository.RecoveryBatchReport remaining = join(
                repository.inspectRecoveryBatch(
                        new JsonlSessionRepository.RecoveryScanQuery(
                                new JsonlSessionRepository.RecoveryQuery(null, 1),
                                timed.nextScanCursor(), 3, null
                        )
                )
        );
        assertEquals(2, remaining.recovery().sessionsScanned());
        assertTrue(remaining.scanComplete());
        assertEquals(2, remaining.recovery().matched());
        assertEquals(1, remaining.recovery().details().size());
        assertTrue(remaining.recovery().truncated());
        assertTrue(remaining.recovery().nextCursor() != null);
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.RecoveryScanQuery(
                        null, null, 0, null
                ));
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.RecoveryScanQuery(
                        null, null, 1, java.time.Duration.ofNanos(-1)
                ));
    }

    @Test
    void cursorPagesFilteredDetailsWhileRepeatingCompleteCounts() throws Exception {
        JsonlSessionRepository repository = repository();
        openToolRun(repository, "page-a", List.of(
                new StartedTool(0, "never", SessionRecordDraft.Replay.NEVER)
        ));
        openToolRun(repository, "page-b", List.of(
                new StartedTool(0, "safe", SessionRecordDraft.Replay.SAFE)
        ));
        openToolRun(repository, "page-c", List.of(
                new StartedTool(0, "never", SessionRecordDraft.Replay.NEVER)
        ));
        Path malformed = temporary.resolve("sessions/page-corrupt.jsonl");
        Files.createDirectories(malformed.getParent());
        Files.writeString(malformed, "not-json\n", StandardCharsets.UTF_8);

        JsonlSessionRepository.RecoveryReport complete =
                join(repository.inspectRecovery());
        JsonlSessionRepository.RecoveryReport first = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        null, 2, null, null
                ))
        );
        assertEquals(4, first.matched());
        assertEquals(2, first.details().size());
        assertTrue(first.truncated());
        assertTrue(first.nextCursor() != null);
        assertEquals(cursor(first.details().getLast()), first.nextCursor());

        JsonlSessionRepository.RecoveryReport second = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        null, 2, null, first.nextCursor()
                ))
        );
        assertEquals(2, second.matched());
        assertEquals(2, second.details().size());
        assertFalse(second.truncated());
        assertEquals(null, second.nextCursor());
        assertEquals(complete.details().stream().map(JsonlRecoveryReportTest::key).toList(),
                java.util.stream.Stream.concat(
                        first.details().stream(), second.details().stream()
                ).map(JsonlRecoveryReportTest::key).toList());
        assertEquals(complete.count(
                        JsonlSessionRepository.RecoveryKind.OPEN_OPERATION),
                second.count(JsonlSessionRepository.RecoveryKind.OPEN_OPERATION));
        assertEquals(complete.count(
                        JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION),
                second.count(JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION));
        assertEquals(complete.unresolvedSafe(), second.unresolvedSafe());
        assertEquals(complete.unresolvedNever(), second.unresolvedNever());

        JsonlSessionRepository.RecoveryReport firstNever = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        Set.of(JsonlSessionRepository.RecoveryKind.OPEN_OPERATION),
                        1,
                        Set.of(SessionOperationInspector.ToolRecovery
                                .ADMINISTRATIVE_RESULT_REQUIRED),
                        null
                ))
        );
        assertEquals(2, firstNever.matched());
        assertTrue(firstNever.truncated());
        JsonlSessionRepository.RecoveryReport secondNever = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        Set.of(JsonlSessionRepository.RecoveryKind.OPEN_OPERATION),
                        1,
                        Set.of(SessionOperationInspector.ToolRecovery
                                .ADMINISTRATIVE_RESULT_REQUIRED),
                        firstNever.nextCursor()
                ))
        );
        assertEquals(1, secondNever.matched());
        assertEquals(1, secondNever.operations().size());
        assertFalse(secondNever.truncated());
    }

    @Test
    void inspectionDoesNotRepairValidMissingNewlineOrProbeOperationLease()
            throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession open = create(repository, "leased-operation");
        join(open.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        assertTrue(open.rawState().claimOperationExecution("main", "run"));
        Path sessionFile;
        try (var paths = Files.walk(temporary.resolve("sessions"))) {
            sessionFile = paths.filter(path -> path.getFileName().toString()
                            .endsWith("_leased-operation.jsonl"))
                    .findFirst().orElseThrow();
        }
        String content = Files.readString(sessionFile, StandardCharsets.UTF_8);
        Files.writeString(
                sessionFile, content.substring(0, content.length() - 1),
                StandardCharsets.UTF_8
        );
        byte[] before = Files.readAllBytes(sessionFile);
        try {
            JsonlSessionRepository.RecoveryReport report =
                    join(repository.inspectRecovery());
            assertEquals(1, report.operations().size());
            assertEquals("run", report.operations().getFirst().operation().id());
            assertTrue(java.util.Arrays.equals(before, Files.readAllBytes(sessionFile)));
        } finally {
            open.rawState().releaseOperationExecution("main", "run");
        }
    }

    @Test
    void kindFilterBoundsReturnedDetailsButKeepsCompleteCounts() throws Exception {
        JsonlSessionRepository repository = repository();
        openToolRun(repository, "never-only", List.of(
                new StartedTool(0, "never", SessionRecordDraft.Replay.NEVER)
        ));
        Path malformed = temporary.resolve("sessions/malformed.jsonl");
        Files.createDirectories(malformed.getParent());
        Files.writeString(malformed, "not-json\n", StandardCharsets.UTF_8);

        JsonlSessionRepository.RecoveryReport failures = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        Set.of(JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION),
                        1
                ))
        );
        assertEquals(1, failures.details().size());
        assertEquals(1, failures.failures().size());
        assertTrue(failures.operations().isEmpty());
        assertEquals(1, failures.matched());
        assertFalse(failures.truncated());
        assertEquals(1, failures.count(
                JsonlSessionRepository.RecoveryKind.OPEN_OPERATION
        ));
        assertEquals(1, failures.count(
                JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION
        ));
        assertEquals(1, failures.unresolvedNever());
        assertEquals("malformed.jsonl",
                failures.failures().getFirst().path().getFileName().toString());
        assertEquals(null, failures.failures().getFirst().sessionId());

        JsonlSessionRepository.RecoveryReport administrative = join(
                repository.inspectRecovery(new JsonlSessionRepository.RecoveryQuery(
                        Set.of(JsonlSessionRepository.RecoveryKind.OPEN_OPERATION),
                        1,
                        Set.of(SessionOperationInspector.ToolRecovery
                                .ADMINISTRATIVE_RESULT_REQUIRED)
                ))
        );
        assertEquals(1, administrative.operations().size());
        assertEquals("never-only",
                administrative.operations().getFirst().session().id());
        assertEquals(1, administrative.matched());
        assertFalse(administrative.truncated());
        assertEquals(1, administrative.count(
                JsonlSessionRepository.RecoveryKind.CORRUPT_SESSION
        ));
        assertThrows(IllegalArgumentException.class, () ->
                new JsonlSessionRepository.RecoveryQuery(null, 0));
    }

    @Test
    void emptyRepositoryProducesEmptyRecoveryReport() {
        JsonlSessionRepository.RecoveryReport report =
                join(repository().inspectRecovery());
        assertTrue(report.details().isEmpty());
        assertEquals(0, report.sessionsScanned());
        assertEquals(0, report.unresolvedSafe());
        assertEquals(0, report.unresolvedNever());
        assertEquals(0, report.matched());
        assertFalse(report.truncated());
        for (JsonlSessionRepository.RecoveryKind kind
                : JsonlSessionRepository.RecoveryKind.values()) {
            assertEquals(0, report.count(kind));
        }
    }

    private static JsonlSessionRepository.RecoveryCursor cursor(
            JsonlSessionRepository.RecoveryDetail detail
    ) {
        return new JsonlSessionRepository.RecoveryCursor(
                detail.path(), detail.kind(),
                detail instanceof JsonlSessionRepository.RecoveryOperation operation
                        ? operation.operation().lane() : ""
        );
    }

    private static String key(JsonlSessionRepository.RecoveryDetail detail) {
        JsonlSessionRepository.RecoveryCursor cursor = cursor(detail);
        return cursor.path() + "|" + cursor.kind() + "|" + cursor.lane();
    }

    private AgentSession openToolRun(
            JsonlSessionRepository repository,
            String id,
            List<StartedTool> tools
    ) {
        AgentSession session = create(repository, id);
        AssistantMessage assistant = new AssistantMessage(
                tools.stream().<ContentBlock>map(tool -> new ToolCallContent(
                        tool.name() + "-call", tool.name(), Map.of()
                )).toList(),
                "api", "provider", "model", Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        join(session.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(), List.of(), "system", null
                    )
            ));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "attempt", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1, "assistant", null
            ));
            transaction.append(new SessionEntryDraft.Message("assistant", assistant));
            for (StartedTool tool : tools) {
                transaction.appendRecord(new SessionRecordDraft.ToolStarted(
                        tool.name() + "-start", "main", "run", "assistant",
                        tool.index(), tool.name() + "-call", tool.name(),
                        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                        tool.name() + "-result", tool.replay()
                ));
            }
            return null;
        }));
        return session;
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

    private record StartedTool(
            int index,
            String name,
            SessionRecordDraft.Replay replay
    ) {
    }
}
