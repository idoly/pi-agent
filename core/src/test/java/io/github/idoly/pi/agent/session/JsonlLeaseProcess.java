package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.AgentLoopConfig;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.testkit.ScriptedMessages;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** Subprocess entry point for cross-JVM JSONL lease tests. */
public final class JsonlLeaseProcess {
    private JsonlLeaseProcess() {
    }

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "hold" -> hold(
                    Path.of(args[1]), Path.of(args[2])
            );
            case "open-then-append" -> openThenAppend(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4])
            );
            case "copy-retained" -> copyRetained(
                    Path.of(args[1]), Path.of(args[2]), args[3], args[4],
                    args[5], Path.of(args[6])
            );
            case "fork-tree" -> forkTree(
                    Path.of(args[1]), Path.of(args[2]), args[3], args[4],
                    Path.of(args[5])
            );
            case "create" -> create(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4])
            );
            case "checkpoint-mutate-loop" -> checkpointMutateLoop(
                    Path.of(args[1]), Path.of(args[2]),
                    Integer.parseInt(args[3]), Path.of(args[4])
            );
            case "resume-run-hold" -> resumeRunHold(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4]),
                    JsonlSessionRepository.MarkerObservationMode.POLLING
            );
            case "resume-run-watch-hold" -> resumeRunHold(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4]),
                    JsonlSessionRepository.MarkerObservationMode.WATCH_SERVICE
            );
            case "resume-tool-hold" -> toolHold(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4]), false
            );
            case "execute-tool-hold" -> toolHold(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4]), true
            );
            case "execute-parallel-tools-hold" -> parallelToolsHold(
                    Path.of(args[1]), Path.of(args[2]), args[3], Path.of(args[4])
            );
            default -> throw new IllegalArgumentException("Unknown action: " + args[0]);
        }
    }

    private static void hold(Path sessionPath, Path control) throws Exception {
        try (JsonlWriterLease ignored = JsonlWriterLease.acquire(sessionPath)) {
            Files.writeString(control.resolve("ready"), "ready", StandardCharsets.UTF_8);
            await(control.resolve("release"));
        }
    }

    private static void openThenAppend(
            Path sessionsRoot,
            Path cwd,
            String sessionId,
            Path control
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        SessionMetadata metadata = repository.list().toCompletableFuture().join().stream()
                .filter(value -> value.id().equals(sessionId))
                .findFirst().orElseThrow();
        AgentSession session = repository.open(metadata).toCompletableFuture().join();
        Files.writeString(control.resolve("ready"), "ready", StandardCharsets.UTF_8);
        await(control.resolve("go"));
        String result;
        try {
            session.append(new SessionEntryDraft.Message(
                    "child", UserMessage.text("child", 1)
            )).toCompletableFuture().join();
            result = "ok";
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof SessionError error) {
                result = "error:" + error.code() + ':' + error.getMessage();
            } else {
                result = "error:" + cause;
            }
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void copyRetained(
            Path sessionsRoot,
            Path cwd,
            String sourceId,
            String destinationId,
            String encodedSequences,
            Path control
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        SessionMetadata source = repository.list().toCompletableFuture().join().stream()
                .filter(value -> value.id().equals(sourceId))
                .findFirst().orElseThrow();
        Set<Long> sequences = Arrays.stream(encodedSequences.split(","))
                .map(Long::parseLong)
                .collect(Collectors.toUnmodifiableSet());
        Files.writeString(control.resolve("ready"), "ready", StandardCharsets.UTF_8);
        await(control.resolve("go"));
        String result;
        try {
            repository.copyRetained(
                    source,
                    new SessionRetainedCopyOptions(sequences, destinationId, null)
            ).toCompletableFuture().join();
            result = "ok";
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof SessionError error) {
                result = "error:" + error.code() + ':' + error.getMessage();
            } else {
                result = "error:" + cause;
            }
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void forkTree(
            Path sessionsRoot,
            Path cwd,
            String sourceId,
            String destinationId,
            Path control
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        SessionMetadata source = repository.list().toCompletableFuture().join().stream()
                .filter(value -> value.id().equals(sourceId))
                .findFirst().orElseThrow();
        Files.writeString(control.resolve("ready"), "ready", StandardCharsets.UTF_8);
        await(control.resolve("go"));
        String result;
        try {
            repository.fork(
                    source,
                    new SessionForkOptions(
                            SessionForkOptions.Scope.TREE, null, null,
                            destinationId, null
                    )
            ).toCompletableFuture().join();
            result = "ok";
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof SessionError error) {
                result = "error:" + error.code() + ':' + error.getMessage();
            } else {
                result = "error:" + cause;
            }
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void create(
            Path sessionsRoot,
            Path cwd,
            String sessionId,
            Path control
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        Files.writeString(control.resolve("ready"), "ready", StandardCharsets.UTF_8);
        await(control.resolve("go"));
        String result;
        try {
            repository.create(
                    new SessionRepository.CreateOptions(sessionId, null)
            ).toCompletableFuture().join();
            result = "ok";
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof SessionError error) {
                result = "error:" + error.code() + ':' + error.getMessage();
            } else {
                result = "error:" + cause;
            }
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void checkpointMutateLoop(
            Path sessionsRoot,
            Path cwd,
            int rounds,
            Path control
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        Files.writeString(control.resolve("ready"), "ready", StandardCharsets.UTF_8);
        for (int round = 0; round < rounds; round++) {
            await(control.resolve("go-" + round));
            String appendId = "checkpoint-append-" + round;
            String deleteId = "checkpoint-delete-" + round;
            SessionMetadata appendMetadata = repository.list().toCompletableFuture().join()
                    .stream().filter(value -> value.id().equals(appendId))
                    .findFirst().orElseThrow();
            SessionMetadata deleteMetadata = repository.list().toCompletableFuture().join()
                    .stream().filter(value -> value.id().equals(deleteId))
                    .findFirst().orElseThrow();
            AgentSession append = repository.open(appendMetadata)
                    .toCompletableFuture().join();
            append.name("changed-" + round).toCompletableFuture().join();
            repository.delete(deleteMetadata).toCompletableFuture().join();
            repository.create(new SessionRepository.CreateOptions(
                    "checkpoint-added-" + round, null
            )).toCompletableFuture().join();
            Files.writeString(
                    control.resolve("done-" + round), "done", StandardCharsets.UTF_8
            );
        }
    }

    private static void resumeRunHold(
            Path sessionsRoot,
            Path cwd,
            String sessionId,
            Path control,
            JsonlSessionRepository.MarkerObservationMode markerMode
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                sessionsRoot, cwd, java.time.Clock.systemUTC(),
                SessionIdGenerator.uuidV7(), JsonlOperationAbortNotifier.NONE,
                markerMode
        );
        SessionMetadata metadata = repository.list().toCompletableFuture().join().stream()
                .filter(value -> value.id().equals(sessionId))
                .findFirst().orElseThrow();
        AgentSession session = repository.open(metadata).toCompletableFuture().join();
        io.github.idoly.pi.ai.ModelStream stream = (model, context, options) -> subscriber -> {
            options.cancellation().onCancel(() -> {
                try {
                    Files.writeString(
                            control.resolve("cancelled"), "cancelled",
                            StandardCharsets.UTF_8
                    );
                    Files.writeString(
                            control.resolve("release"), "release",
                            StandardCharsets.UTF_8
                    );
                } catch (IOException ignored) {
                }
            });
            subscriber.onSubscribe(new java.util.concurrent.Flow.Subscription() {
                    private boolean emitted;

                    @Override
                    public synchronized void request(long count) {
                        if (emitted) return;
                        emitted = true;
                        Thread.ofVirtual().start(() -> {
                            try {
                                Files.writeString(
                                        control.resolve("effect-ready"), "ready",
                                        StandardCharsets.UTF_8
                                );
                                await(control.resolve("release"));
                                options.cancellation().throwIfCancelled();
                                subscriber.onNext(new AssistantStreamEvent.Done(
                                        ScriptedMessages.assistant("child", StopReason.STOP)
                                ));
                                subscriber.onComplete();
                            } catch (Throwable failure) {
                                subscriber.onError(failure);
                            }
                        });
                    }

                    @Override
                    public void cancel() {
                        try {
                            Files.writeString(
                                    control.resolve("cancelled"), "cancelled",
                                    StandardCharsets.UTF_8
                            );
                            Files.writeString(
                                    control.resolve("release"), "release",
                                    StandardCharsets.UTF_8
                            );
                        } catch (IOException ignored) {
                        }
                    }
                });
        };
        String result;
        try {
            SessionRunOperation.resume(
                    session,
                    new SessionRunOperation.RecoveryOptions(
                            new SessionRunOperation.Options(
                                    "ignored",
                                    new AgentLoopConfig(ScriptedMessages.model(), stream)
                            ),
                            3
                    )
            ).toCompletableFuture().join();
            result = "ok";
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof SessionError error) {
                result = "error:" + error.code() + ':' + error.getMessage();
            } else {
                result = "error:" + cause;
            }
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void toolHold(
            Path sessionsRoot,
            Path cwd,
            String sessionId,
            Path control,
            boolean execute
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        SessionMetadata metadata = repository.list().toCompletableFuture().join().stream()
                .filter(value -> value.id().equals(sessionId))
                .findFirst().orElseThrow();
        AgentSession session = repository.open(metadata).toCompletableFuture().join();
        AgentTool tool = new AgentTool() {
            @Override
            public ToolDefinition definition() {
                return new ToolDefinition("tool", "tool", java.util.Map.of());
            }

            @Override
            public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String toolCallId,
                    java.util.Map<String, Object> arguments,
                    io.github.idoly.pi.ai.CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> onUpdate
            ) {
                try (AutoCloseable ignored = cancellation.onCancel(() -> {
                    try {
                        Files.writeString(
                                control.resolve("cancelled"), "cancelled",
                                StandardCharsets.UTF_8
                        );
                        Files.writeString(
                                control.resolve("release"), "release",
                                StandardCharsets.UTF_8
                        );
                    } catch (IOException ignoredFailure) {
                    }
                })) {
                    Files.writeString(
                            control.resolve("effect-ready"), "ready",
                            StandardCharsets.UTF_8
                    );
                    await(control.resolve("release"));
                    cancellation.throwIfCancelled();
                    return java.util.concurrent.CompletableFuture.completedFuture(
                            new AgentToolResult(
                                    java.util.List.of(new TextContent("child-tool")),
                                    java.util.Map.of()
                            )
                    );
                } catch (Throwable failure) {
                    return java.util.concurrent.CompletableFuture.failedFuture(failure);
                }
            }
        };
        String result;
        try {
            SessionToolExecution.Options options = new SessionToolExecution.Options(
                    java.util.Map.of(
                            "tool", execute
                                    ? SessionRecordDraft.Replay.NEVER
                                    : SessionRecordDraft.Replay.SAFE
                    ),
                    java.time.Clock.systemUTC()
            );
            (execute
                    ? SessionToolExecution.execute(
                            session, "run", "assistant",
                            java.util.List.of(tool), options
                    )
                    : SessionToolExecution.resume(
                            session, "run", "assistant",
                            java.util.List.of(tool), options
                    )).toCompletableFuture().join();
            result = "ok";
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof SessionError error) {
                result = "error:" + error.code() + ':' + error.getMessage();
            } else {
                result = "error:" + cause;
            }
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void parallelToolsHold(
            Path sessionsRoot,
            Path cwd,
            String sessionId,
            Path control
    ) throws Exception {
        JsonlSessionRepository repository = new JsonlSessionRepository(sessionsRoot, cwd);
        SessionMetadata metadata = repository.list().toCompletableFuture().join().stream()
                .filter(value -> value.id().equals(sessionId))
                .findFirst().orElseThrow();
        AgentSession session = repository.open(metadata).toCompletableFuture().join();
        java.util.List<AgentTool> tools = java.util.List.of("first", "second").stream()
                .map(name -> new AgentTool() {
                    @Override
                    public ToolDefinition definition() {
                        return new ToolDefinition(name, name, java.util.Map.of());
                    }

                    @Override
                    public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                            String toolCallId,
                            java.util.Map<String, Object> arguments,
                            io.github.idoly.pi.ai.CancellationSignal cancellation,
                            java.util.function.Consumer<AgentToolResult> onUpdate
                    ) {
                        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                            try (AutoCloseable ignored = cancellation.onCancel(() -> {
                                try {
                                    Files.writeString(
                                            control.resolve(name + "-cancelled"),
                                            "cancelled", StandardCharsets.UTF_8
                                    );
                                    Files.writeString(
                                            control.resolve(name + "-release"),
                                            "release", StandardCharsets.UTF_8
                                    );
                                } catch (IOException ignoredFailure) {
                                }
                            })) {
                                Files.writeString(
                                        control.resolve(name + "-ready"),
                                        "ready", StandardCharsets.UTF_8
                                );
                                await(control.resolve(name + "-release"));
                                cancellation.throwIfCancelled();
                                return new AgentToolResult(
                                        java.util.List.of(new TextContent(name)),
                                        java.util.Map.of()
                                );
                            } catch (Throwable failure) {
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                        });
                    }
                }).map(AgentTool.class::cast).toList();
        String result;
        try {
            SessionToolExecution.Outcome outcome = SessionToolExecution.execute(
                    session, "run", "assistant", tools,
                    new SessionToolExecution.Options(
                            java.util.Map.of(
                                    "first", SessionRecordDraft.Replay.SAFE,
                                    "second", SessionRecordDraft.Replay.SAFE
                            ),
                            java.time.Clock.systemUTC(),
                            io.github.idoly.pi.agent.ToolExecutionMode.PARALLEL,
                            new SessionToolExecutionEventBus()
                    )
            ).toCompletableFuture().join();
            result = outcome.getClass().getSimpleName() + ':'
                    + outcome.results().stream().map(SessionEntry::id).toList();
        } catch (java.util.concurrent.CompletionException failure) {
            result = "error:" + failure.getCause();
        }
        Files.writeString(control.resolve("result"), result, StandardCharsets.UTF_8);
    }

    private static void await(Path signal) throws Exception {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(20).toNanos();
        while (!Files.exists(signal)) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out waiting for " + signal);
            }
            Thread.sleep(10);
        }
    }
}
