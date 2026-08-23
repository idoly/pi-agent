package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.AgentLoopConfig;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;
import io.github.idoly.pi.testkit.ScriptedModelStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlCrossProcessLeaseTest {
    @TempDir
    Path temporary;

    @Test
    @Timeout(30)
    void sameSessionIdCanBeCreatedByExactlyOneJvm() throws Exception {
        JsonlSessionRepository repository = repository();
        Path control = Files.createDirectories(temporary.resolve("create-control"));
        Process child = start(
                "create",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "created-once",
                control.toString()
        );
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var parent = executor.submit(() -> {
                    try {
                        return join(repository.create(
                                new SessionRepository.CreateOptions(
                                        "created-once", null
                                )
                        ));
                    } catch (CompletionException failure) {
                        return failure.getCause();
                    }
                });
                Files.writeString(control.resolve("go"), "go");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals(0, child.exitValue(), processOutput(child));
                Object parentResult = parent.get(10, TimeUnit.SECONDS);
                String childResult = Files.readString(
                        control.resolve("result"), StandardCharsets.UTF_8
                );
                assertEquals(1, (parentResult instanceof AgentSession ? 1 : 0)
                        + ("ok".equals(childResult) ? 1 : 0));
                if (parentResult instanceof SessionError error) {
                    assertEquals(SessionError.Code.ALREADY_EXISTS, error.code());
                } else {
                    assertTrue(childResult.startsWith("error:ALREADY_EXISTS:"), childResult);
                }
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        assertEquals(List.of("created-once"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());
        try (var paths = Files.walk(temporary.resolve("sessions"))) {
            assertEquals(1, paths.filter(path -> path.getFileName().toString()
                    .endsWith("_created-once.jsonl")).count());
        }
    }

    @Test
    @Timeout(30)
    void operatingSystemLeaseBlocksACommitInAnotherJvm() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "blocked");
        Path sessionFile = onlySessionFile();
        Path control = Files.createDirectories(temporary.resolve("hold-control"));
        Process child = start("hold", sessionFile.toString(), control.toString());
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var append = executor.submit(() -> join(session.append(
                        new SessionEntryDraft.Message(
                                "parent", UserMessage.text("parent", 1)
                        )
                )));
                Thread.sleep(250);
                assertFalse(append.isDone(),
                        "append completed while another JVM held the writer lease");
                Files.writeString(control.resolve("release"), "release");
                assertEquals(0, child.waitFor(10, TimeUnit.SECONDS)
                        ? child.exitValue() : -1, processOutput(child));
                assertEquals("parent", append.get(10, TimeUnit.SECONDS).id());
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        assertEquals(List.of("parent"), join(repository().open(
                join(session.metadata())
        )).findEntries().toCompletableFuture().join().stream()
                .map(SessionEntry::id).toList());
    }

    @Test
    @Timeout(30)
    void snapshotOpenedInAnotherJvmIsFencedAfterParentCommit() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession parent = create(repository, "stale-child");
        Path control = Files.createDirectories(temporary.resolve("stale-control"));
        Process child = start(
                "open-then-append",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "stale-child",
                control.toString()
        );
        try {
            await(control.resolve("ready"));
            join(parent.append(new SessionEntryDraft.Message(
                    "parent", UserMessage.text("parent", 1)
            )));
            Files.writeString(control.resolve("go"), "go");
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
            String result = Files.readString(
                    control.resolve("result"), StandardCharsets.UTF_8
            );
            assertTrue(result.startsWith("error:STORAGE:"), result);
            assertTrue(result.contains("Stale JSONL session writer"), result);
            assertTrue(result.contains("reopen the session"), result);
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }

        AgentSession reopened = join(repository().open(join(parent.metadata())));
        assertEquals(List.of("parent"), join(reopened.findEntries()).stream()
                .map(SessionEntry::id).toList());
        assertEquals(List.of(1L), join(reopened.log(0, null)).stream()
                .map(SessionLogItem::sequence).toList());
    }

    @Test
    @Timeout(30)
    void operationLeaseFencesStaleResumeBeforeProviderDispatch() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "operation-fence");
        UserMessage prompt = UserMessage.text("resume", 1);
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(prompt),
                            List.of(new SessionEntryDraft.Message("prompt", prompt)),
                            "durable-system", null
                    )
            ));
            transaction.append(new SessionEntryDraft.Message("prompt", prompt));
            return null;
        }));
        join(source.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run",
                SessionRecordDraft.Step.ASSISTANT, 1,
                "unknown-assistant", null
        )));
        SessionMetadata metadata = join(source.metadata());
        AgentSession staleParent = join(repository.open(metadata));
        Path control = Files.createDirectories(temporary.resolve("operation-control"));
        Process child = start(
                "resume-run-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "operation-fence",
                control.toString()
        );
        ScriptedModelStream parentStream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("parent", StopReason.STOP)
                )
        ));
        try {
            await(control.resolve("effect-ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var parent = executor.submit(() -> {
                    try {
                        return join(SessionRunOperation.resume(
                                staleParent,
                                new SessionRunOperation.RecoveryOptions(
                                        new SessionRunOperation.Options(
                                                "ignored",
                                                new AgentLoopConfig(
                                                        ScriptedMessages.model(), parentStream
                                                )
                                        ),
                                        3
                                )
                        ));
                    } catch (CompletionException failure) {
                        return failure.getCause();
                    }
                });
                Thread.sleep(250);
                assertFalse(parent.isDone(),
                        "stale resume did not wait for cross-process operation lease");
                assertEquals(0, parentStream.invocationCount());
                Files.writeString(control.resolve("release"), "release");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals(0, child.exitValue(), processOutput(child));
                assertEquals("ok", Files.readString(
                        control.resolve("result"), StandardCharsets.UTF_8
                ));
                SessionError stale = assertInstanceOf(
                        SessionError.class, parent.get(10, TimeUnit.SECONDS)
                );
                assertEquals(SessionError.Code.STORAGE, stale.code());
                assertTrue(stale.getMessage().contains("Stale JSONL session writer"));
                assertEquals(0, parentStream.invocationCount());
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }

        AgentSession verified = join(repository.open(metadata));
        assertEquals("child", ((io.github.idoly.pi.ai.TextContent)
                ((io.github.idoly.pi.ai.AssistantMessage)
                        ((SessionEntry.Message) join(verified.findEntries()).getFirst())
                                .message()).content().getFirst()).text());
        assertTrue(join(verified.findOpenOperations("main", null)).isEmpty());
        assertEquals(1, join(repository.inspectMaintenance()).count(
                JsonlSessionRepository.ArtifactKind.OPERATION_LOCK
        ));
    }

    @Test
    @Timeout(30)
    void operationSignalCleanupWaitsForIndependentJvmOwner() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession session = create(repository, "cross-process-signal-cleanup");
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        SessionMetadata metadata = join(session.metadata());
        assertTrue(join(SessionRunOperation.requestAbort(session, "run")));
        Path marker = join(repository.inspectMaintenance()).artifacts().stream()
                .filter(value -> value.kind()
                        == JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL)
                .findFirst().orElseThrow().path();
        join(repository.delete(metadata));
        Files.setLastModifiedTime(
                marker, java.nio.file.attribute.FileTime.from(java.time.Instant.EPOCH)
        );
        Path operationBase = Path.of(marker.toString().substring(
                0, marker.toString().length() - ".abort".length()
        ));
        Path control = Files.createDirectories(
                temporary.resolve("signal-cleanup-control")
        );
        Process child = start("hold", operationBase.toString(), control.toString());
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var cleanup = executor.submit(() -> join(
                        repository.cleanupUnassociatedOperationSignals()
                ));
                Thread.sleep(250);
                assertFalse(cleanup.isDone());
                assertTrue(Files.exists(marker));
                Files.writeString(control.resolve("release"), "release");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals(0, child.exitValue(), processOutput(child));
                assertEquals(
                        new JsonlSessionRepository.OperationSignalCleanupResult(
                                1, 1, 0, 0
                        ),
                        cleanup.get(10, TimeUnit.SECONDS)
                );
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        assertFalse(Files.exists(marker));
        assertTrue(Files.exists(Path.of(operationBase + ".lock")));
    }

    @Test
    @Timeout(30)
    void watchServiceCancelsProviderAcrossIndependentJvms() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "watch-service-cross-process");
        UserMessage prompt = UserMessage.text("resume", 1);
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(prompt),
                            List.of(new SessionEntryDraft.Message("prompt", prompt)),
                            "durable-system", null
                    )
            ));
            transaction.append(new SessionEntryDraft.Message("prompt", prompt));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "attempt-1", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1,
                    "unknown-assistant", null
            ));
            return null;
        }));
        SessionMetadata metadata = join(source.metadata());
        Path control = Files.createDirectories(
                temporary.resolve("watch-service-process-control")
        );
        Process child = start(
                "resume-run-watch-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "watch-service-cross-process",
                control.toString()
        );
        try {
            await(control.resolve("effect-ready"));
            AgentSession aborter = join(repository.open(metadata));
            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            await(control.resolve("cancelled"));
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
            assertEquals("ok", Files.readString(
                    control.resolve("result"), StandardCharsets.UTF_8
            ));
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        AgentSession verified = join(repository.open(metadata));
        SessionRecordDraft.OperationFinished finished = join(verified.findRecords(
                new SessionRecordQuery(
                        "main", SessionRecordDraft.Type.OPERATION_FINISHED,
                        "run", null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        )).stream().map(SessionRecord::value)
                .map(SessionRecordDraft.OperationFinished.class::cast)
                .findFirst().orElseThrow();
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED, finished.outcome());
        assertEquals("prompt", join(verified.leafId()));
        join(verified.validateRecordLog("main"));
    }

    @Test
    @Timeout(30)
    void remoteAbortIsReconciledByActiveProviderOwnerAtSettlement() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "remote-provider-abort");
        UserMessage prompt = UserMessage.text("resume", 1);
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(prompt),
                            List.of(new SessionEntryDraft.Message("prompt", prompt)),
                            "durable-system", null
                    )
            ));
            transaction.append(new SessionEntryDraft.Message("prompt", prompt));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "attempt-1", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1,
                    "unknown-assistant", null
            ));
            return null;
        }));
        SessionMetadata metadata = join(source.metadata());
        Path control = Files.createDirectories(temporary.resolve("remote-abort-control"));
        Process child = start(
                "resume-run-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "remote-provider-abort",
                control.toString()
        );
        try {
            await(control.resolve("effect-ready"));
            AgentSession aborter = join(repository.open(metadata));
            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            assertEquals(1, join(repository.inspectMaintenance()).count(
                    JsonlSessionRepository.ArtifactKind.OPERATION_ABORT_SIGNAL
            ));
            await(control.resolve("cancelled"));
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
            assertEquals("ok", Files.readString(
                    control.resolve("result"), StandardCharsets.UTF_8
            ));
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }

        AgentSession verified = join(repository.open(metadata));
        SessionRecordDraft.OperationFinished finished = join(verified.findRecords(
                new SessionRecordQuery(
                        "main", SessionRecordDraft.Type.OPERATION_FINISHED,
                        "run", null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        )).stream().map(SessionRecord::value)
                .map(SessionRecordDraft.OperationFinished.class::cast)
                .findFirst().orElseThrow();
        assertEquals(SessionRecordDraft.OperationOutcome.ABORTED, finished.outcome());
        assertEquals("prompt", join(verified.leafId()));
        assertTrue(join(verified.findOpenOperations("main", null)).isEmpty());
        join(verified.validateRecordLog("main"));
    }

    @Test
    @Timeout(30)
    void killedProviderOwnerReleasesLeaseAndRecoveryUsesLaterAttempt() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "killed-provider-owner");
        UserMessage prompt = UserMessage.text("resume", 1);
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(prompt),
                            List.of(new SessionEntryDraft.Message("prompt", prompt)),
                            "durable-system", null
                    )
            ));
            transaction.append(new SessionEntryDraft.Message("prompt", prompt));
            return null;
        }));
        join(source.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run",
                SessionRecordDraft.Step.ASSISTANT, 1,
                "unknown-assistant", null
        )));
        SessionMetadata metadata = join(source.metadata());
        AgentSession staleParent = join(repository.open(metadata));
        Path control = Files.createDirectories(temporary.resolve("killed-provider-control"));
        Process child = start(
                "resume-run-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "killed-provider-owner",
                control.toString()
        );
        await(control.resolve("effect-ready"));
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));

        ScriptedModelStream staleStream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("stale", StopReason.STOP)
                )
        ));
        SessionError stale = assertThrowsSessionError(() -> join(
                SessionRunOperation.resume(
                        staleParent,
                        new SessionRunOperation.RecoveryOptions(
                                new SessionRunOperation.Options(
                                        "ignored", new AgentLoopConfig(
                                                ScriptedMessages.model(), staleStream
                                        )
                                ),
                                4
                        )
                )
        ));
        assertEquals(SessionError.Code.STORAGE, stale.code());
        assertEquals(0, staleStream.invocationCount());

        ScriptedModelStream recoveryStream = new ScriptedModelStream(List.of(
                new AssistantStreamEvent.Done(
                        ScriptedMessages.assistant("recovered", StopReason.STOP)
                )
        ));
        AgentSession reopened = join(repository.open(metadata));
        SessionRunOperation.Outcome.Completed completed = assertInstanceOf(
                SessionRunOperation.Outcome.Completed.class,
                join(SessionRunOperation.resume(
                        reopened,
                        new SessionRunOperation.RecoveryOptions(
                                new SessionRunOperation.Options(
                                        "ignored", new AgentLoopConfig(
                                                ScriptedMessages.model(), recoveryStream
                                        )
                                ),
                                4
                        )
                ))
        );
        assertEquals("recovered", ((TextContent)
                ((AssistantMessage) completed.entry().message())
                        .content().getFirst()).text());
        assertEquals(List.of(1, 2, 3), join(reopened.findRecords(
                new SessionRecordQuery(
                        "main", SessionRecordDraft.Type.STEP_ATTEMPT,
                        "run", null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        )).stream().map(SessionRecord::value)
                .map(SessionRecordDraft.StepAttempt.class::cast)
                .map(SessionRecordDraft.StepAttempt::attempt).toList());
    }

    @Test
    @Timeout(30)
    void remoteAbortCancelsEveryParallelToolAndPublishesSourceOrder()
            throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "remote-parallel-abort");
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new ToolCallContent("first-call", "first", Map.of()),
                        new ToolCallContent("second-call", "second", Map.of())
                ),
                "api", "provider", "model", Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(), List.of(), "system", null
                    )
            ));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "assistant-attempt", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1,
                    "assistant", null
            ));
            transaction.append(new SessionEntryDraft.Message("assistant", assistant));
            return null;
        }));
        SessionMetadata metadata = join(source.metadata());
        Path control = Files.createDirectories(
                temporary.resolve("remote-parallel-abort-control")
        );
        Process child = start(
                "execute-parallel-tools-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "remote-parallel-abort",
                control.toString()
        );
        try {
            await(control.resolve("first-ready"));
            await(control.resolve("second-ready"));
            AgentSession aborter = join(repository.open(metadata));
            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            await(control.resolve("first-cancelled"));
            await(control.resolve("second-cancelled"));
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
            assertEquals("Aborted:[" + join(repository.open(metadata))
                    .findRecords(new SessionRecordQuery(
                            "main", SessionRecordDraft.Type.TOOL_STARTED,
                            "run", null, null,
                            SessionEntryQuery.Order.OLDEST_FIRST, null
                    )).toCompletableFuture().join().stream()
                    .map(SessionRecord::value)
                    .map(SessionRecordDraft.ToolStarted.class::cast)
                    .map(SessionRecordDraft.ToolStarted::resultEntryId)
                    .collect(java.util.stream.Collectors.joining(", ")) + "]",
                    Files.readString(control.resolve("result"), StandardCharsets.UTF_8));
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }

        AgentSession reopened = join(repository.open(metadata));
        List<SessionRecordDraft.ToolStarted> starts = join(reopened.findRecords(
                new SessionRecordQuery(
                        "main", SessionRecordDraft.Type.TOOL_STARTED,
                        "run", null, null,
                        SessionEntryQuery.Order.OLDEST_FIRST, null
                )
        )).stream().map(SessionRecord::value)
                .map(SessionRecordDraft.ToolStarted.class::cast).toList();
        assertEquals(List.of(0, 1), starts.stream()
                .map(SessionRecordDraft.ToolStarted::toolIndex).toList());
        List<SessionEntry> entries = join(reopened.findEntries(
                new SessionEntryQuery(
                        null, null, SessionEntryQuery.Order.OLDEST_FIRST, null, null
                )
        ));
        assertEquals(List.of(
                "assistant",
                starts.get(0).resultEntryId(),
                starts.get(1).resultEntryId()
        ), entries.stream().map(SessionEntry::id).toList());
        assertTrue(((io.github.idoly.pi.ai.ToolResultMessage)
                ((SessionEntry.Message) entries.get(1)).message()).error());
        assertTrue(((io.github.idoly.pi.ai.ToolResultMessage)
                ((SessionEntry.Message) entries.get(2)).message()).error());

        SessionRunOperation.Outcome.Aborted aborted = assertInstanceOf(
                SessionRunOperation.Outcome.Aborted.class,
                join(SessionRunOperation.resume(
                        reopened,
                        new SessionRunOperation.RecoveryOptions(
                                new SessionRunOperation.Options(
                                        "ignored", new AgentLoopConfig(
                                                ScriptedMessages.model(),
                                                new ScriptedModelStream(List.of())
                                        )
                                ),
                                3
                        )
                ))
        );
        assertEquals(starts.get(1).resultEntryId(), aborted.leafId());
        join(reopened.validateRecordLog("main"));
    }

    @Test
    @Timeout(30)
    void remoteAbortCancelsActiveToolAndReopenFinishesRunAborted()
            throws Exception {
        JsonlSessionRepository repository = repository();
        SessionMetadata metadata = createStartedTool(
                repository, "remote-tool-abort", SessionRecordDraft.Replay.SAFE
        );
        Path control = Files.createDirectories(temporary.resolve("remote-tool-abort-control"));
        Process child = start(
                "resume-tool-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "remote-tool-abort",
                control.toString()
        );
        try {
            await(control.resolve("effect-ready"));
            AgentSession aborter = join(repository.open(metadata));
            assertTrue(join(SessionRunOperation.requestAbort(aborter, "run")));
            await(control.resolve("cancelled"));
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
            assertEquals("ok", Files.readString(
                    control.resolve("result"), StandardCharsets.UTF_8
            ));
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }

        AgentSession reopened = join(repository.open(metadata));
        SessionEntry.Message toolResult = assertInstanceOf(
                SessionEntry.Message.class, join(reopened.entry("tool-result"))
        );
        assertTrue(((io.github.idoly.pi.ai.ToolResultMessage)
                toolResult.message()).error());
        SessionRunOperation.Outcome.Aborted aborted = assertInstanceOf(
                SessionRunOperation.Outcome.Aborted.class,
                join(SessionRunOperation.resume(
                        reopened,
                        new SessionRunOperation.RecoveryOptions(
                                new SessionRunOperation.Options(
                                        "ignored", new AgentLoopConfig(
                                                ScriptedMessages.model(),
                                                new ScriptedModelStream(List.of())
                                        )
                                ),
                                3
                        )
                ))
        );
        assertEquals("tool-result", aborted.leafId());
        assertTrue(join(reopened.findOpenOperations("main", null)).isEmpty());
        join(reopened.validateRecordLog("main"));
    }

    @Test
    @Timeout(30)
    void killedNeverToolOwnerSuspendsUntilAdministrativeResolution()
            throws Exception {
        JsonlSessionRepository repository = repository();
        SessionMetadata metadata = createStartedTool(
                repository, "killed-never-tool", null
        );
        AgentSession staleParent = join(repository.open(metadata));
        Path control = Files.createDirectories(temporary.resolve("killed-never-control"));
        Process child = start(
                "execute-tool-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "killed-never-tool",
                control.toString()
        );
        await(control.resolve("effect-ready"));
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));

        AtomicInteger effects = new AtomicInteger();
        AgentTool forbiddenReplay = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("tool", "tool", Map.of());
            }

            @Override
            public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                effects.incrementAndGet();
                return java.util.concurrent.CompletableFuture.completedFuture(
                        new AgentToolResult(List.of(new TextContent("wrong")), Map.of())
                );
            }
        };
        SessionError stale = assertThrowsSessionError(() -> join(
                SessionToolExecution.resume(
                        staleParent, "run", "assistant", List.of(forbiddenReplay),
                        SessionToolExecution.Options.DEFAULT
                )
        ));
        assertEquals(SessionError.Code.STORAGE, stale.code());
        assertEquals(0, effects.get());

        AgentSession reopened = join(repository.open(metadata));
        SessionToolExecution.Outcome.Suspended suspended = assertInstanceOf(
                SessionToolExecution.Outcome.Suspended.class,
                join(SessionToolExecution.resume(
                        reopened, "run", "assistant", List.of(forbiddenReplay),
                        SessionToolExecution.Options.DEFAULT
                ))
        );
        assertEquals(SessionRecordDraft.Replay.NEVER,
                suspended.unresolved().replay());
        assertEquals(0, effects.get());
        SessionEntry.Message resolved = join(SessionToolExecution.resolveNever(
                reopened, "run", "assistant", 0,
                new AgentToolResult(
                        List.of(new TextContent("operator-observed")), Map.of()
                ),
                false,
                SessionToolExecution.Options.DEFAULT
        ));
        assertEquals(suspended.unresolved().resultEntryId(), resolved.id());
        assertEquals("operator-observed", ((TextContent)
                ((io.github.idoly.pi.ai.ToolResultMessage) resolved.message())
                        .content().getFirst()).text());
        assertEquals(0, effects.get());
        join(reopened.validateRecordLog("main"));
    }

    @Test
    @Timeout(30)
    void killedSafeToolOwnerIsTakenOverWithoutReopenWhenLogDidNotAdvance()
            throws Exception {
        JsonlSessionRepository repository = repository();
        SessionMetadata metadata = createStartedTool(
                repository, "killed-safe-tool", SessionRecordDraft.Replay.SAFE
        );
        AgentSession waitingParent = join(repository.open(metadata));
        Path control = Files.createDirectories(temporary.resolve("killed-tool-control"));
        Process child = start(
                "resume-tool-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "killed-safe-tool",
                control.toString()
        );
        await(control.resolve("effect-ready"));
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));

        AtomicInteger effects = new AtomicInteger();
        AgentTool takeover = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("tool", "tool", Map.of());
            }

            @Override
            public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                effects.incrementAndGet();
                return java.util.concurrent.CompletableFuture.completedFuture(
                        new AgentToolResult(
                                List.of(new TextContent("taken-over")), Map.of()
                        )
                );
            }
        };
        SessionToolExecution.Outcome.Completed completed = assertInstanceOf(
                SessionToolExecution.Outcome.Completed.class,
                join(SessionToolExecution.resume(
                        waitingParent, "run", "assistant", List.of(takeover),
                        new SessionToolExecution.Options(
                                Map.of("tool", SessionRecordDraft.Replay.SAFE),
                                java.time.Clock.systemUTC()
                        )
                ))
        );
        assertEquals(1, effects.get());
        assertEquals(List.of("tool-result"), completed.results().stream()
                .map(SessionEntry::id).toList());
        assertEquals("tool-result", join(waitingParent.leafId()));
    }

    @Test
    @Timeout(30)
    void operationLeaseFencesStaleToolResumeBeforeEffectDispatch() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "tool-operation-fence");
        AssistantMessage assistant = new AssistantMessage(
                List.of(new ToolCallContent("call", "tool", Map.of())),
                "api", "provider", "model", Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(), List.of(), "system", null
                    )
            ));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "assistant-attempt", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1,
                    "assistant", null
            ));
            transaction.append(new SessionEntryDraft.Message("assistant", assistant));
            return null;
        }));
        join(source.appendRecord(new SessionRecordDraft.ToolStarted(
                "tool-start", "main", "run", "assistant", 0,
                "call", "tool",
                com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                "tool-result", SessionRecordDraft.Replay.SAFE
        )));
        SessionMetadata metadata = join(source.metadata());
        AgentSession staleParent = join(repository.open(metadata));
        Path control = Files.createDirectories(temporary.resolve("tool-operation-control"));
        Process child = start(
                "resume-tool-hold",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "tool-operation-fence",
                control.toString()
        );
        AtomicInteger parentEffects = new AtomicInteger();
        AgentTool parentTool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("tool", "tool", Map.of());
            }

            @Override
            public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                parentEffects.incrementAndGet();
                return java.util.concurrent.CompletableFuture.completedFuture(
                        new AgentToolResult(List.of(new TextContent("parent")), Map.of())
                );
            }
        };
        try {
            await(control.resolve("effect-ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var parent = executor.submit(() -> {
                    try {
                        return join(SessionToolExecution.resume(
                                staleParent, "run", "assistant", List.of(parentTool),
                                new SessionToolExecution.Options(
                                        Map.of("tool", SessionRecordDraft.Replay.SAFE),
                                        java.time.Clock.systemUTC()
                                )
                        ));
                    } catch (CompletionException failure) {
                        return failure.getCause();
                    }
                });
                Thread.sleep(250);
                assertFalse(parent.isDone());
                assertEquals(0, parentEffects.get());
                Files.writeString(control.resolve("release"), "release");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals("ok", Files.readString(
                        control.resolve("result"), StandardCharsets.UTF_8
                ));
                SessionError stale = assertInstanceOf(
                        SessionError.class, parent.get(10, TimeUnit.SECONDS)
                );
                assertEquals(SessionError.Code.STORAGE, stale.code());
                assertTrue(stale.getMessage().contains("Stale JSONL session writer"));
                assertEquals(0, parentEffects.get());
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        AgentSession verified = join(repository.open(metadata));
        assertEquals("tool-result", join(verified.leafId()));
        assertEquals("child-tool", ((TextContent)
                ((io.github.idoly.pi.ai.ToolResultMessage)
                        ((SessionEntry.Message) join(verified.entry("tool-result"))).message())
                        .content().getFirst()).text());
    }

    @Test
    @Timeout(60)
    void repeatedDeleteRecreateFencesEveryIndependentJvmGeneration()
            throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession current = create(repository, "generation-stress");
        java.util.LinkedHashSet<Path> generations = new java.util.LinkedHashSet<>();
        generations.add(onlySessionFile().toAbsolutePath().normalize());

        for (int cycle = 1; cycle <= 4; cycle++) {
            SessionMetadata oldMetadata = join(current.metadata());
            Path control = Files.createDirectories(
                    temporary.resolve("generation-stress-" + cycle)
            );
            Process child = start(
                    "open-then-append",
                    temporary.resolve("sessions").toString(),
                    temporary.toString(),
                    "generation-stress",
                    control.toString()
            );
            try {
                await(control.resolve("ready"));
                join(repository.delete(oldMetadata));
                AgentSession replacement = create(repository, "generation-stress");
                Path replacementPath = onlySessionFile()
                        .toAbsolutePath().normalize();
                assertTrue(generations.add(replacementPath),
                        "generation path was reused at cycle " + cycle);
                String entryId = "replacement-" + cycle;
                join(replacement.append(new SessionEntryDraft.Message(
                        entryId, UserMessage.text(entryId, cycle)
                )));
                Files.writeString(control.resolve("go"), "go");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals(0, child.exitValue(), processOutput(child));
                String result = Files.readString(
                        control.resolve("result"), StandardCharsets.UTF_8
                );
                assertTrue(result.startsWith("error:STORAGE:"), result);
                AgentSession verified = join(repository.open(
                        join(replacement.metadata())
                ));
                assertEquals(List.of(entryId), join(verified.findEntries()).stream()
                        .map(SessionEntry::id).toList());
                assertEquals(List.of(1L), join(verified.log(0, null)).stream()
                        .map(SessionLogItem::sequence).toList());
                current = replacement;
            } finally {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }

        assertEquals(5, generations.size());
        try (var paths = Files.walk(temporary.resolve("sessions"))) {
            List<Path> files = paths.filter(Files::isRegularFile).toList();
            assertEquals(1, files.stream().filter(path -> path.toString()
                    .endsWith("_generation-stress.jsonl")).count());
            assertTrue(files.stream().filter(path -> path.toString()
                    .endsWith("_generation-stress.jsonl.lock")).count() >= 5);
        }
        assertEquals(List.of("generation-stress"), join(repository.list()).stream()
                .map(SessionMetadata::id).toList());
    }

    @Test
    @Timeout(30)
    void handleInAnotherJvmCannotWriteAfterDeleteAndRecreate() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession original = create(repository, "recreated");
        SessionMetadata originalMetadata = join(original.metadata());
        Path originalPath = onlySessionFile();
        Path control = Files.createDirectories(temporary.resolve("recreate-control"));
        Process child = start(
                "open-then-append",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "recreated",
                control.toString()
        );
        try {
            await(control.resolve("ready"));
            join(repository.delete(originalMetadata));
            AgentSession replacement = create(repository, "recreated");
            Path replacementPath;
            try (var paths = Files.walk(temporary.resolve("sessions"))) {
                replacementPath = paths.filter(path -> path.toString().endsWith(".jsonl"))
                        .findFirst().orElseThrow();
            }
            assertFalse(originalPath.equals(replacementPath));
            join(replacement.append(new SessionEntryDraft.Message(
                    "replacement", UserMessage.text("replacement", 2)
            )));
            Files.writeString(control.resolve("go"), "go");
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
            assertEquals(0, child.exitValue(), processOutput(child));
            String result = Files.readString(
                    control.resolve("result"), StandardCharsets.UTF_8
            );
            assertTrue(result.startsWith("error:STORAGE:"), result);
            assertTrue(result.contains("Failed to append session"), result);
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        SessionMetadata replacementMetadata = join(repository.list()).getFirst();
        AgentSession reopened = join(repository.open(replacementMetadata));
        assertEquals(List.of("replacement"), join(reopened.findEntries()).stream()
                .map(SessionEntry::id).toList());
        assertEquals(List.of(1L), join(reopened.log(0, null)).stream()
                .map(SessionLogItem::sequence).toList());
    }

    @Test
    @Timeout(30)
    void retainedCopyDestinationIsFencedAcrossJvms() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "copy-source");
        join(source.append(new SessionEntryDraft.Message(
                "retained", UserMessage.text("retained", 1)
        )));
        join(source.append(new SessionEntryDraft.Message(
                "omitted", UserMessage.text("omitted", 2)
        )));
        SessionMetadata sourceMetadata = join(source.metadata());
        Path control = Files.createDirectories(temporary.resolve("copy-control"));
        Process child = start(
                "copy-retained",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "copy-source",
                "copy-destination",
                "1",
                control.toString()
        );
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var parent = executor.submit(() -> {
                    try {
                        return join(repository.copyRetained(
                                sourceMetadata,
                                new SessionRetainedCopyOptions(
                                        Set.of(1L), "copy-destination", null
                                )
                        ));
                    } catch (CompletionException failure) {
                        return failure.getCause();
                    }
                });
                Files.writeString(control.resolve("go"), "go");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals(0, child.exitValue(), processOutput(child));
                Object parentResult = parent.get(10, TimeUnit.SECONDS);
                String childResult = Files.readString(
                        control.resolve("result"), StandardCharsets.UTF_8
                );
                long successes = (parentResult instanceof AgentSession ? 1 : 0)
                        + ("ok".equals(childResult) ? 1 : 0);
                assertEquals(1, successes,
                        "parent=" + parentResult + ", child=" + childResult);
                if (parentResult instanceof SessionError error) {
                    assertEquals(SessionError.Code.ALREADY_EXISTS, error.code());
                } else {
                    assertTrue(childResult.startsWith("error:ALREADY_EXISTS:"), childResult);
                }
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }

        List<SessionMetadata> sessions = join(repository().list());
        assertEquals(List.of("copy-destination", "copy-source"), sessions.stream()
                .map(SessionMetadata::id).sorted().toList());
        AgentSession copied = join(repository().open(sessions.stream()
                .filter(value -> value.id().equals("copy-destination"))
                .findFirst().orElseThrow()));
        assertEquals("copy-source", join(copied.metadata()).parentSessionId());
        assertEquals(List.of("retained"), join(copied.findEntries()).stream()
                .map(SessionEntry::id).toList());
        assertEquals(List.of(1L), join(copied.log(0, null)).stream()
                .map(SessionLogItem::sequence).toList());
        try (var paths = Files.walk(temporary.resolve("sessions"))) {
            assertEquals(1, paths.filter(path -> path.getFileName().toString()
                    .endsWith("_copy-destination.jsonl")).count());
        }
    }

    @Test
    @Timeout(30)
    void forkDestinationIsFencedAcrossJvms() throws Exception {
        JsonlSessionRepository repository = repository();
        AgentSession source = create(repository, "fork-source");
        join(source.append(new SessionEntryDraft.Message(
                "forked-entry", UserMessage.text("forked", 1)
        )));
        SessionMetadata sourceMetadata = join(source.metadata());
        Path control = Files.createDirectories(temporary.resolve("fork-control"));
        Process child = start(
                "fork-tree",
                temporary.resolve("sessions").toString(),
                temporary.toString(),
                "fork-source",
                "fork-destination",
                control.toString()
        );
        try {
            await(control.resolve("ready"));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var parent = executor.submit(() -> {
                    try {
                        return join(repository.fork(
                                sourceMetadata,
                                new SessionForkOptions(
                                        SessionForkOptions.Scope.TREE, null, null,
                                        "fork-destination", null
                                )
                        ));
                    } catch (CompletionException failure) {
                        return failure.getCause();
                    }
                });
                Files.writeString(control.resolve("go"), "go");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), processOutput(child));
                assertEquals(0, child.exitValue(), processOutput(child));
                Object parentResult = parent.get(10, TimeUnit.SECONDS);
                String childResult = Files.readString(
                        control.resolve("result"), StandardCharsets.UTF_8
                );
                assertEquals(1, (parentResult instanceof AgentSession ? 1 : 0)
                        + ("ok".equals(childResult) ? 1 : 0));
                if (parentResult instanceof SessionError error) {
                    assertEquals(SessionError.Code.ALREADY_EXISTS, error.code());
                } else {
                    assertTrue(childResult.startsWith("error:ALREADY_EXISTS:"), childResult);
                }
            }
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
        }
        SessionMetadata destination = join(repository.list()).stream()
                .filter(value -> value.id().equals("fork-destination"))
                .findFirst().orElseThrow();
        AgentSession forked = join(repository.open(destination));
        assertEquals("fork-source", join(forked.metadata()).parentSessionId());
        assertEquals(List.of("forked-entry"), join(forked.findEntries()).stream()
                .map(SessionEntry::id).toList());
        assertTrue(join(forked.findRecords()).isEmpty());
        try (var paths = Files.walk(temporary.resolve("sessions"))) {
            assertEquals(1, paths.filter(path -> path.getFileName().toString()
                    .endsWith("_fork-destination.jsonl")).count());
        }
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
                .redirectOutput(temporary.resolve(
                        "process-" + System.nanoTime() + ".log"
                ).toFile())
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

    private static SessionMetadata createStartedTool(
            JsonlSessionRepository repository,
            String id,
            SessionRecordDraft.Replay replay
    ) {
        AgentSession source = create(repository, id);
        AssistantMessage assistant = new AssistantMessage(
                List.of(new ToolCallContent("call", "tool", Map.of())),
                "api", "provider", "model", Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        join(source.transaction(transaction -> {
            transaction.appendRecord(new SessionRecordDraft.OperationStarted(
                    "run", "main", null,
                    new SessionRecordDraft.OperationIntent.Run(
                            List.of(), List.of(), "system", null
                    )
            ));
            transaction.appendRecord(new SessionRecordDraft.StepAttempt(
                    "assistant-attempt", "main", "run",
                    SessionRecordDraft.Step.ASSISTANT, 1,
                    "assistant", null
            ));
            transaction.append(new SessionEntryDraft.Message("assistant", assistant));
            if (replay != null) {
                transaction.appendRecord(new SessionRecordDraft.ToolStarted(
                        "tool-start", "main", "run", "assistant", 0,
                        "call", "tool",
                        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                        "tool-result", replay
                ));
            }
            return null;
        }));
        return join(source.metadata());
    }

    private static SessionError assertThrowsSessionError(Runnable operation) {
        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                CompletionException.class, operation::run
        );
        return assertInstanceOf(SessionError.class, failure.getCause());
    }

    private static String processOutput(Process process) {
        try {
            return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            return "<process output unavailable>";
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

    private static AgentSession create(JsonlSessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
