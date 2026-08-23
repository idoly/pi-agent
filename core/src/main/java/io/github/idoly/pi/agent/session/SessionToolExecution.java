package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.agent.AfterToolCall;
import io.github.idoly.pi.agent.AfterToolCallResult;
import io.github.idoly.pi.agent.BeforeToolCall;
import io.github.idoly.pi.agent.BeforeToolCallResult;
import io.github.idoly.pi.agent.CancellationSource;
import io.github.idoly.pi.agent.ToolArgumentValidator;
import io.github.idoly.pi.agent.ToolExecutionMode;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Sequential durable tool dispatcher for one persisted assistant message. */
@ExperimentalSessionApi
public final class SessionToolExecution {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> ARGUMENT_MAP =
            new TypeReference<>() { };

    private SessionToolExecution() {
    }

    public static CompletionStage<Outcome> execute(
            AgentSession session,
            String runId,
            String assistantEntryId,
            List<AgentTool> tools,
            Options options
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(assistantEntryId, "assistantEntryId");
        List<AgentTool> safeTools = List.copyOf(tools);
        Options effective = options == null ? Options.DEFAULT : options;
        return session.validateRecordLog(session.lane())
                .thenCompose(ignored -> prepare(
                        session, runId, assistantEntryId, safeTools
                ))
                .thenCompose(prepared -> executeClaimed(
                        session, runId, () -> drive(
                                session, prepared, effective, false
                        )
                ));
    }

    public static CompletionStage<SessionEntry.Message> resolveNever(
            AgentSession session,
            String runId,
            String assistantEntryId,
            int toolIndex,
            AgentToolResult result,
            boolean error,
            Options options
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(assistantEntryId, "assistantEntryId");
        Objects.requireNonNull(result, "result");
        if (toolIndex < 0) {
            return failed(SessionError.Code.INVALID_QUERY,
                    "toolIndex must be non-negative");
        }
        Options effective = options == null ? Options.DEFAULT : options;
        return session.validateRecordLog(session.lane())
                .thenCompose(ignored -> prepare(
                        session, runId, assistantEntryId, List.of()
                ))
                .thenCompose(prepared -> executeClaimed(
                        session, runId, () -> resolveNeverClaimed(
                                session, prepared, toolIndex,
                                result, error, effective
                        )
                ));
    }

    public static CompletionStage<Outcome> resume(
            AgentSession session,
            String runId,
            String assistantEntryId,
            List<AgentTool> tools,
            Options options
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(assistantEntryId, "assistantEntryId");
        List<AgentTool> safeTools = List.copyOf(tools);
        Options effective = options == null ? Options.DEFAULT : options;
        return session.validateRecordLog(session.lane())
                .thenCompose(ignored -> prepare(
                        session, runId, assistantEntryId, safeTools
                ))
                .thenCompose(prepared -> executeClaimed(
                        session, runId, () -> drive(
                                session, prepared, effective, true
                        )
                ));
    }

    private static CompletionStage<SessionEntry.Message> resolveNeverClaimed(
            AgentSession session,
            Prepared prepared,
            int toolIndex,
            AgentToolResult result,
            boolean error,
            Options options
    ) {
        if (toolIndex >= prepared.calls().size()) {
            return failed(SessionError.Code.INVALID_QUERY,
                    "toolIndex is outside the assistant tool-call list");
        }
        return session.findRecords(new SessionRecordQuery(
                session.lane(), SessionRecordDraft.Type.TOOL_STARTED,
                prepared.runId(), null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )).thenCompose(records -> {
            Map<Integer, SessionRecordDraft.ToolStarted> starts =
                    new LinkedHashMap<>();
            for (SessionRecord record : records) {
                SessionRecordDraft.ToolStarted started =
                        (SessionRecordDraft.ToolStarted) record.value();
                if (started.assistantEntryId().equals(prepared.assistantEntryId())) {
                    starts.put(started.toolIndex(), started);
                }
            }
            SessionRecordDraft.ToolStarted target = starts.get(toolIndex);
            if (target == null) {
                return failed(SessionError.Code.NOT_FOUND,
                        "Durable tool start not found at index " + toolIndex);
            }
            if (target.replay() != SessionRecordDraft.Replay.NEVER) {
                return failed(SessionError.Code.INVALID_PAYLOAD,
                        "Administrative resolution is only valid for Replay.NEVER");
            }
            if (session.rawState().getEntry(target.resultEntryId()) != null) {
                return failed(SessionError.Code.ALREADY_EXISTS,
                        "Tool result is already published: " + target.resultEntryId());
            }
            String expectedLeaf = prepared.assistantEntryId();
            for (int index = 0; index < toolIndex; index++) {
                SessionRecordDraft.ToolStarted previous = starts.get(index);
                SessionEntry previousEntry = previous == null ? null
                        : session.rawState().getEntry(previous.resultEntryId());
                if (!(previousEntry instanceof SessionEntry.Message message)
                        || !Objects.equals(message.parentId(), expectedLeaf)) {
                    return CompletableFuture.failedFuture(new RecordLogCorruption(
                            RecordLogCorruption.Reason.INCONSISTENT_STEP,
                            "Cannot resolve a tool before its source-order prefix"
                    ));
                }
                expectedLeaf = message.id();
            }
            if (!Objects.equals(
                    session.rawState().leaf(session.lane()), expectedLeaf
            )) {
                return CompletableFuture.failedFuture(new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Administrative tool result does not match the lane leaf"
                ));
            }
            String publicationLeaf = expectedLeaf;
            return session.transaction(transaction -> {
                if (!Objects.equals(transaction.leafId(), publicationLeaf)) {
                    throw new RecordLogCorruption(
                            RecordLogCorruption.Reason.INCONSISTENT_STEP,
                            "Administrative tool result prefix changed before publication"
                    );
                }
                ToolResultMessage message = new ToolResultMessage(
                        target.toolCallId(), target.toolName(),
                        result.content(), result.details(), result.usage(), error,
                        options.clock().millis()
                );
                SessionEntry.Message entry = (SessionEntry.Message) transaction.append(
                        new SessionEntryDraft.Message(
                                target.resultEntryId(), message, result.terminate()
                        )
                );
                if (result.usage() != null) {
                    transaction.appendRecord(new SessionRecordDraft.UsageRecord(
                            session.idGenerator().next(), session.lane(), "tool_resolution",
                            result.usage(), prepared.runId(), entry.id(), null,
                            null, target.toolCallId(), null
                    ));
                }
                return entry;
            }).thenApply(entry -> {
                emitPublished(session, prepared, options, target, entry.id());
                return entry;
            });
        });
    }

    private static CompletionStage<Prepared> prepare(
            AgentSession session,
            String runId,
            String assistantEntryId,
            List<AgentTool> tools
    ) {
        return session.findOpenOperations(session.lane(), 2).thenCompose(open -> {
            if (open.isEmpty() || !open.getFirst().id().equals(runId)) {
                return failed(SessionError.Code.NOT_FOUND,
                        "Open operation not found: " + runId);
            }
            SessionRecordDraft.OperationStarted started =
                    (SessionRecordDraft.OperationStarted) open.getFirst().value();
            if (!(started.intent() instanceof SessionRecordDraft.OperationIntent.Run)) {
                return failed(SessionError.Code.INVALID_PAYLOAD,
                        "Open operation is not a run: " + runId);
            }
            return session.entry(assistantEntryId).thenCompose(entry -> {
                if (!(entry instanceof SessionEntry.Message message)
                        || !(message.message() instanceof AssistantMessage assistant)) {
                    return failed(SessionError.Code.INVALID_PAYLOAD,
                            "Assistant entry not found: " + assistantEntryId);
                }
                List<ToolCallContent> calls = assistant.content().stream()
                        .filter(ToolCallContent.class::isInstance)
                        .map(ToolCallContent.class::cast).toList();
                if (calls.isEmpty()) {
                    return failed(SessionError.Code.INVALID_PAYLOAD,
                            "Assistant entry has no tool calls: " + assistantEntryId);
                }
                LinkedHashMap<String, AgentTool> byName = new LinkedHashMap<>();
                for (AgentTool tool : tools) {
                    Objects.requireNonNull(tool, "tool");
                    if (byName.putIfAbsent(tool.name(), tool) != null) {
                        return failed(SessionError.Code.INVALID_PAYLOAD,
                                "Duplicate tool name: " + tool.name());
                    }
                }
                return CompletableFuture.completedFuture(new Prepared(
                        runId, assistantEntryId, calls, Map.copyOf(byName)
                ));
            });
        });
    }

    private static CompletionStage<Outcome> drive(
            AgentSession session,
            Prepared prepared,
            Options options,
            boolean recovering
    ) {
        return session.findRecords(new SessionRecordQuery(
                session.lane(), SessionRecordDraft.Type.TOOL_STARTED,
                prepared.runId(), null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )).thenCompose(records -> {
            LinkedHashMap<Integer, SessionRecordDraft.ToolStarted> starts =
                    new LinkedHashMap<>();
            for (SessionRecord record : records) {
                SessionRecordDraft.ToolStarted started =
                        (SessionRecordDraft.ToolStarted) record.value();
                if (!started.assistantEntryId().equals(prepared.assistantEntryId())) continue;
                if (starts.putIfAbsent(started.toolIndex(), started) != null) {
                    return CompletableFuture.failedFuture(new RecordLogCorruption(
                            RecordLogCorruption.Reason.DUPLICATE_TOOL_INVOCATION,
                            "Duplicate durable tool invocation "
                                    + prepared.assistantEntryId() + ':' + started.toolIndex()
                    ));
                }
            }
            if (!recovering && !starts.isEmpty()) {
                return failed(SessionError.Code.INVALID_PAYLOAD,
                        "Tool execution already has durable starts; use resume");
            }
            if (!recovering && options.executionMode() == ToolExecutionMode.PARALLEL) {
                return executeParallel(session, prepared, options);
            }
            return continueAt(
                    session, prepared, options, recovering,
                    starts, 0, prepared.assistantEntryId(), new ArrayList<>()
            );
        });
    }

    private static CompletionStage<Outcome> executeParallel(
            AgentSession session,
            Prepared prepared,
            Options options
    ) {
        session.rawState().reconcileOperationAbort(
                session.lane(), prepared.runId()
        );
        if (session.rawState().hasAbortRequest(session.lane(), prepared.runId())) {
            return CompletableFuture.completedFuture(new Outcome.Aborted(
                    prepared.runId(), prepared.assistantEntryId(), List.of()
            ));
        }
        ArrayList<PreparedInvocation> invocations = new ArrayList<>();
        boolean sequential = false;
        for (ToolCallContent call : prepared.calls()) {
            PreparedInvocation invocation = prepareInvocation(
                    call, prepared.tools(), options
            );
            invocations.add(invocation);
            if (invocation.tool() != null
                    && invocation.tool().executionMode()
                    == ToolExecutionMode.SEQUENTIAL) {
                sequential = true;
            }
        }
        if (sequential) {
            return continueAt(
                    session, prepared, options, false, new LinkedHashMap<>(),
                    0, prepared.assistantEntryId(), new ArrayList<>()
            );
        }
        return session.transaction(transaction -> {
            if (!Objects.equals(
                    transaction.leafId(), prepared.assistantEntryId()
            )) {
                throw new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Tool batch does not start at its assistant entry"
                );
            }
            ArrayList<ParallelInvocation> durable = new ArrayList<>();
            for (int index = 0; index < prepared.calls().size(); index++) {
                ToolCallContent call = prepared.calls().get(index);
                PreparedInvocation invocation = invocations.get(index);
                SessionRecordDraft.ToolStarted started =
                        new SessionRecordDraft.ToolStarted(
                                session.idGenerator().next(), session.lane(),
                                prepared.runId(), prepared.assistantEntryId(), index,
                                call.id(), call.name(),
                                MAPPER.valueToTree(invocation.arguments()),
                                session.idGenerator().next(), invocation.replay()
                        );
                transaction.appendRecord(started);
                durable.add(new ParallelInvocation(started, invocation));
            }
            return List.copyOf(durable);
        }).thenCompose(durable -> {
            durable.forEach(item -> emitStarted(
                    session, prepared, options, item.started(), false
            ));
            List<CompletableFuture<Executed>> effects = durable.stream()
                    .map(item -> {
                        CompletableFuture<Executed> effect;
                        if (item.invocation().immediate() != null) {
                            effect = CompletableFuture.completedFuture(new Executed(
                                    item.invocation().immediate(), true
                            ));
                        } else {
                            effect = executeInvocation(
                                    session, prepared, options,
                                    item.started(), item.invocation()
                            ).toCompletableFuture();
                        }
                        return effect.whenComplete((executed, failure) -> emitFinished(
                                session, prepared, options, item.started(),
                                failure != null || executed != null && executed.error()
                        ));
                    })
                    .toList();
            CompletableFuture<Void> all = CompletableFuture.allOf(
                    effects.toArray(CompletableFuture[]::new)
            );
            return all.thenCompose(ignored -> publishParallel(
                    session, prepared, options, durable,
                    effects.stream().map(CompletableFuture::join).toList()
            ));
        });
    }

    private static CompletionStage<Outcome> publishParallel(
            AgentSession session,
            Prepared prepared,
            Options options,
            List<ParallelInvocation> durable,
            List<Executed> executed
    ) {
        session.rawState().reconcileOperationAbort(
                session.lane(), prepared.runId()
        );
        return session.transaction(transaction -> {
            if (!Objects.equals(
                    transaction.leafId(), prepared.assistantEntryId()
            )) {
                throw new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Tool batch leaf changed before source-order publication"
                );
            }
            ArrayList<SessionEntry.Message> results = new ArrayList<>();
            for (int index = 0; index < durable.size(); index++) {
                ParallelInvocation item = durable.get(index);
                Executed effect = executed.get(index);
                AgentToolResult result = effect.result() == null
                        ? errorResult("Tool returned no result") : effect.result();
                boolean error = effect.error();
                SessionRecordDraft.ToolStarted started = item.started();
                ToolResultMessage message = new ToolResultMessage(
                        started.toolCallId(), started.toolName(),
                        result.content(), result.details(), result.usage(), error,
                        options.clock().millis()
                );
                SessionEntry.Message entry = (SessionEntry.Message) transaction.append(
                        new SessionEntryDraft.Message(
                                started.resultEntryId(), message, result.terminate()
                        )
                );
                results.add(entry);
                if (result.usage() != null) {
                    transaction.appendRecord(new SessionRecordDraft.UsageRecord(
                            session.idGenerator().next(), session.lane(), "tool",
                            result.usage(), prepared.runId(), entry.id(), null,
                            null, started.toolCallId(), null
                    ));
                }
            }
            return List.copyOf(results);
        }).thenApply(results -> {
            for (int index = 0; index < durable.size(); index++) {
                emitPublished(
                        session, prepared, options, durable.get(index).started(),
                        results.get(index).id()
                );
            }
            return session.rawState().hasAbortRequest(
                session.lane(), prepared.runId()
        ) ? new Outcome.Aborted(
                prepared.runId(), prepared.assistantEntryId(), results
        ) : new Outcome.Completed(
                prepared.runId(), prepared.assistantEntryId(), results
        );
        });
    }

    private static CompletionStage<Outcome> continueAt(
            AgentSession session,
            Prepared prepared,
            Options options,
            boolean recovering,
            Map<Integer, SessionRecordDraft.ToolStarted> starts,
            int index,
            String expectedLeaf,
            ArrayList<SessionEntry.Message> results
    ) {
        if (index == prepared.calls().size()) {
            return CompletableFuture.completedFuture(new Outcome.Completed(
                    prepared.runId(), prepared.assistantEntryId(), results
            ));
        }
        ToolCallContent call = prepared.calls().get(index);
        SessionRecordDraft.ToolStarted existingStart = starts.get(index);
        if (existingStart != null) {
            return session.entry(existingStart.resultEntryId()).thenCompose(existing -> {
                if (existing != null) {
                    if (!(existing instanceof SessionEntry.Message message)
                            || !(message.message() instanceof ToolResultMessage)) {
                        return CompletableFuture.failedFuture(new RecordLogCorruption(
                                RecordLogCorruption.Reason.PROVISIONED_ENTRY_MISMATCH,
                                "Tool result entry has incompatible content: "
                                        + existingStart.resultEntryId()
                        ));
                    }
                    if (!Objects.equals(message.parentId(), expectedLeaf)) {
                        return CompletableFuture.failedFuture(new RecordLogCorruption(
                                RecordLogCorruption.Reason.INCONSISTENT_STEP,
                                "Tool result entries are not a source-ordered prefix"
                        ));
                    }
                    results.add(message);
                    return continueAt(
                            session, prepared, options, recovering, starts,
                            index + 1, message.id(), results
                    );
                }
                if (!Objects.equals(session.rawState().leaf(session.lane()), expectedLeaf)) {
                    return CompletableFuture.failedFuture(new RecordLogCorruption(
                            RecordLogCorruption.Reason.INCONSISTENT_STEP,
                            "Tool result prefix does not match the lane leaf"
                    ));
                }
                if (recovering && existingStart.replay() == SessionRecordDraft.Replay.NEVER) {
                    return CompletableFuture.completedFuture(new Outcome.Suspended(
                            prepared.runId(), prepared.assistantEntryId(),
                            List.copyOf(results),
                            new UnresolvedTool(
                                    index, call.id(), call.name(),
                                    existingStart.resultEntryId(), existingStart.replay()
                            )
                    ));
                }
                return executeStarted(
                        session, prepared, options, starts, index,
                        expectedLeaf, results, existingStart, recovering
                );
            });
        }
        if (!Objects.equals(session.rawState().leaf(session.lane()), expectedLeaf)) {
            return CompletableFuture.failedFuture(new RecordLogCorruption(
                    RecordLogCorruption.Reason.INCONSISTENT_STEP,
                    "Tool result prefix does not match the lane leaf"
            ));
        }
        if (session.rawState().hasAbortRequest(session.lane(), prepared.runId())) {
            return CompletableFuture.completedFuture(new Outcome.Aborted(
                    prepared.runId(), prepared.assistantEntryId(), List.copyOf(results)
            ));
        }
        PreparedInvocation invocation = prepareInvocation(
                call, prepared.tools(), options
        );
        String resultEntryId = session.idGenerator().next();
        SessionRecordDraft.ToolStarted started = new SessionRecordDraft.ToolStarted(
                session.idGenerator().next(), session.lane(), prepared.runId(),
                prepared.assistantEntryId(), index, call.id(), call.name(),
                MAPPER.valueToTree(invocation.arguments()), resultEntryId,
                invocation.replay()
        );
        return session.appendRecord(started).thenCompose(ignored -> {
            starts.put(index, started);
            emitStarted(session, prepared, options, started, false);
            if (invocation.immediate() != null) {
                emitFinished(session, prepared, options, started, true);
                return persistResult(
                        session, prepared, options, starts, index,
                        expectedLeaf, results, started,
                        invocation.immediate(), true, recovering
                );
            }
            return executeInvocation(
                    session, prepared, options, started, invocation
            ).whenComplete((executed, failure) -> emitFinished(
                            session, prepared, options, started,
                            failure != null || executed != null && executed.error()
                    ))
                    .thenCompose(executed -> persistResult(
                            session, prepared, options, starts, index,
                            expectedLeaf, results, started,
                            executed.result(), executed.error(), recovering
                    ));
        });
    }

    private static CompletionStage<Outcome> executeStarted(
            AgentSession session,
            Prepared prepared,
            Options options,
            Map<Integer, SessionRecordDraft.ToolStarted> starts,
            int index,
            String expectedLeaf,
            ArrayList<SessionEntry.Message> results,
            SessionRecordDraft.ToolStarted started,
            boolean recovering
    ) {
        emitStarted(session, prepared, options, started, true);
        AgentTool tool = prepared.tools().get(started.toolName());
        if (tool == null) {
            emitFinished(session, prepared, options, started, true);
            return persistResult(
                    session, prepared, options, starts, index,
                    expectedLeaf, results, started,
                    errorResult("Tool " + started.toolName() + " not found"), true,
                    recovering
            );
        }
        Map<String, Object> arguments = MAPPER.convertValue(
                started.effectiveArgs(), ARGUMENT_MAP
        );
        PreparedInvocation invocation = new PreparedInvocation(
                tool, arguments, null, started.replay()
        );
        return executeInvocation(
                session, prepared, options, started, invocation
        ).whenComplete((executed, failure) -> emitFinished(
                        session, prepared, options, started,
                        failure != null || executed != null && executed.error()
                ))
                .thenCompose(executed -> persistResult(
                        session, prepared, options, starts, index,
                        expectedLeaf, results, started,
                        executed.result(), executed.error(), recovering
                ));
    }

    private static CompletionStage<Outcome> persistResult(
            AgentSession session,
            Prepared prepared,
            Options options,
            Map<Integer, SessionRecordDraft.ToolStarted> starts,
            int index,
            String expectedLeaf,
            ArrayList<SessionEntry.Message> results,
            SessionRecordDraft.ToolStarted started,
            AgentToolResult result,
            boolean error,
            boolean recovering
    ) {
        AgentToolResult safeResult = result == null
                ? errorResult("Tool returned no result") : result;
        AgentToolResult finalResult = safeResult;
        boolean finalError = error;
        session.rawState().reconcileOperationAbort(
                session.lane(), prepared.runId()
        );
        return session.transaction(transaction -> {
            if (!Objects.equals(transaction.leafId(), expectedLeaf)) {
                throw new RecordLogCorruption(
                        RecordLogCorruption.Reason.INCONSISTENT_STEP,
                        "Tool result prefix changed before publication"
                );
            }
            ToolResultMessage message = new ToolResultMessage(
                    started.toolCallId(), started.toolName(),
                    finalResult.content(), finalResult.details(), finalResult.usage(),
                    finalError, options.clock().millis()
            );
            SessionEntry.Message entry = (SessionEntry.Message) transaction.append(
                    new SessionEntryDraft.Message(
                            started.resultEntryId(), message, finalResult.terminate()
                    )
            );
            if (finalResult.usage() != null) {
                transaction.appendRecord(new SessionRecordDraft.UsageRecord(
                        session.idGenerator().next(), session.lane(), "tool",
                        finalResult.usage(), prepared.runId(), entry.id(), null,
                        null, started.toolCallId(), null
                ));
            }
            return entry;
        }).thenCompose(entry -> {
            emitPublished(session, prepared, options, started, entry.id());
            results.add(entry);
            if (session.rawState().hasAbortRequest(session.lane(), prepared.runId())) {
                return CompletableFuture.completedFuture(new Outcome.Aborted(
                        prepared.runId(), prepared.assistantEntryId(), List.copyOf(results)
                ));
            }
            return continueAt(
                    session, prepared, options, recovering, starts,
                    index + 1, entry.id(), results
            );
        });
    }

    private static PreparedInvocation prepareInvocation(
            ToolCallContent call,
            Map<String, AgentTool> tools,
            Options options
    ) {
        AgentTool tool = tools.get(call.name());
        if (tool == null) {
            return new PreparedInvocation(
                    null, call.arguments(),
                    errorResult("Tool " + call.name() + " not found"),
                    SessionRecordDraft.Replay.SAFE
            );
        }
        try {
            Map<String, Object> arguments = Map.copyOf(
                    tool.prepareArguments(call.arguments())
            );
            ToolArgumentValidator.validate(
                    tool.definition().parameters(), arguments
            );
            tool.validateArguments(arguments);
            return new PreparedInvocation(
                    tool, arguments, null, options.replay(tool.name())
            );
        } catch (Throwable failure) {
            return new PreparedInvocation(
                    null, call.arguments(), errorResult(messageOf(failure)),
                    SessionRecordDraft.Replay.SAFE
            );
        }
    }

    private static CompletionStage<Executed> executeInvocation(
            AgentSession session,
            Prepared prepared,
            Options options,
            SessionRecordDraft.ToolStarted started,
            PreparedInvocation invocation
    ) {
        ToolCallContent call = prepared.calls().get(started.toolIndex());
        CancellationSource cancellation = new CancellationSource();
        AutoCloseable registration = session.rawState()
                .registerOperationCancellation(
                        session.lane(), prepared.runId(), cancellation::cancel
                );
        CompletionStage<Executed> stage = applyBefore(
                session, options, call, invocation.arguments(), cancellation
        ).thenCompose(before -> {
            if (before.block()) {
                return CompletableFuture.completedFuture(new Executed(
                        errorResult(before.reason(), before.terminate()), true
                ));
            }
            return invokeTool(
                    invocation.tool(), call, invocation.arguments(), cancellation
            ).thenCompose(executed -> applyAfter(
                    session, options, call, invocation.arguments(),
                    executed, cancellation
            ));
        });
        return stage.whenComplete((ignored, failure) -> closeQuietly(registration));
    }

    private static CompletionStage<BeforeDecision> applyBefore(
            AgentSession session,
            Options options,
            ToolCallContent call,
            Map<String, Object> arguments,
            CancellationSource cancellation
    ) {
        if (options.beforeToolCall() == null) {
            return CompletableFuture.completedFuture(BeforeDecision.ALLOW);
        }
        return session.context().thenCompose(context -> {
            CompletionStage<BeforeToolCallResult> stage;
            try {
                stage = options.beforeToolCall().apply(
                        call, arguments, context.messages(), cancellation
                );
                if (stage == null) {
                    stage = CompletableFuture.completedFuture(null);
                }
            } catch (Throwable failure) {
                stage = CompletableFuture.failedFuture(failure);
            }
            return stage.handle((result, failure) -> {
                if (failure != null) {
                    return new BeforeDecision(
                            true, messageOf(failure), false
                    );
                }
                BeforeToolCallResult effective = result == null
                        ? BeforeToolCallResult.allow() : result;
                return new BeforeDecision(
                        effective.block(),
                        effective.reason() == null
                                ? "Tool execution was blocked" : effective.reason(),
                        effective.terminate()
                );
            });
        });
    }

    private static CompletionStage<Executed> applyAfter(
            AgentSession session,
            Options options,
            ToolCallContent call,
            Map<String, Object> arguments,
            Executed executed,
            CancellationSource cancellation
    ) {
        if (options.afterToolCall() == null) {
            return CompletableFuture.completedFuture(normalize(executed));
        }
        Executed normalized = normalize(executed);
        return session.context().thenCompose(context -> {
            CompletionStage<AfterToolCallResult> stage;
            try {
                stage = options.afterToolCall().apply(
                        call, arguments, normalized.result(), normalized.error(),
                        context.messages(), cancellation
                );
                if (stage == null) {
                    stage = CompletableFuture.completedFuture(null);
                }
            } catch (Throwable failure) {
                stage = CompletableFuture.failedFuture(failure);
            }
            return stage.handle((patch, failure) -> {
                if (failure != null) {
                    return new Executed(errorResult(messageOf(failure)), true);
                }
                AfterToolCallResult effective = patch == null
                        ? AfterToolCallResult.unchanged() : patch;
                AgentToolResult original = normalized.result();
                return new Executed(new AgentToolResult(
                        effective.content() == null
                                ? original.content() : effective.content(),
                        effective.details() == null
                                ? original.details() : effective.details(),
                        effective.usage() == null
                                ? original.usage() : effective.usage(),
                        effective.terminate() == null
                                ? original.terminate() : effective.terminate()
                ), effective.error() == null
                        ? normalized.error() : effective.error());
            });
        });
    }

    private static Executed normalize(Executed executed) {
        return executed.result() == null
                ? new Executed(errorResult("Tool returned no result"), true)
                : executed;
    }

    private static CompletionStage<Executed> invokeTool(
            AgentTool tool,
            ToolCallContent call,
            Map<String, Object> arguments,
            CancellationSource cancellation
    ) {
        CompletionStage<AgentToolResult> effect;
        try {
            effect = tool.execute(
                    call.id(), arguments, cancellation, ignored -> { }
            );
            if (effect == null) {
                effect = CompletableFuture.failedFuture(
                        new IllegalStateException("Tool returned no completion stage")
                );
            }
        } catch (Throwable failure) {
            effect = CompletableFuture.failedFuture(failure);
        }
        return effect.handle((result, failure) -> failure == null
                ? new Executed(result, false)
                : new Executed(errorResult(messageOf(unwrap(failure))), true));
    }

    private static void emitStarted(
            AgentSession session,
            Prepared prepared,
            Options options,
            SessionRecordDraft.ToolStarted started,
            boolean recovery
    ) {
        if (options.events() != null) {
            options.events().emit(new SessionToolExecutionEvent.EffectStarted(
                    session.lane(), prepared.runId(), prepared.assistantEntryId(),
                    started.toolIndex(), started.toolCallId(), started.toolName(),
                    started.replay(), recovery
            ));
        }
    }

    private static void emitFinished(
            AgentSession session,
            Prepared prepared,
            Options options,
            SessionRecordDraft.ToolStarted started,
            boolean error
    ) {
        if (options.events() != null) {
            options.events().emit(new SessionToolExecutionEvent.EffectFinished(
                    session.lane(), prepared.runId(), prepared.assistantEntryId(),
                    started.toolIndex(), started.toolCallId(), started.toolName(), error
            ));
        }
    }

    private static void emitPublished(
            AgentSession session,
            Prepared prepared,
            Options options,
            SessionRecordDraft.ToolStarted started,
            String resultEntryId
    ) {
        if (options.events() != null) {
            options.events().emit(new SessionToolExecutionEvent.ResultPublished(
                    session.lane(), prepared.runId(), prepared.assistantEntryId(),
                    started.toolIndex(), started.toolCallId(), started.toolName(),
                    resultEntryId
            ));
        }
    }

    private static AgentToolResult errorResult(String message) {
        return errorResult(message, false);
    }

    private static AgentToolResult errorResult(
            String message,
            boolean terminate
    ) {
        return new AgentToolResult(
                List.of(new TextContent(message)), Map.of(), null, terminate
        );
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Cancellation registration cleanup cannot change durable publication.
        }
    }

    private static String messageOf(Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        return unwrapped.getMessage() == null
                ? unwrapped.getClass().getSimpleName() : unwrapped.getMessage();
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletionStage<T> executeClaimed(
            AgentSession session,
            String runId,
            Supplier<CompletionStage<T>> operation
    ) {
        if (!session.state().claimOperationExecution(session.lane(), runId)) {
            return failed(SessionError.Code.STORAGE,
                    "Operation is already executing: " + runId);
        }
        CompletionStage<T> stage;
        try {
            stage = operation.get();
        } catch (Throwable failure) {
            session.rawState().releaseOperationExecution(session.lane(), runId);
            throw failure;
        }
        return stage.whenComplete((ignored, failure) ->
                session.rawState().releaseOperationExecution(session.lane(), runId));
    }

    private static <T> CompletionStage<T> failed(
            SessionError.Code code,
            String message
    ) {
        return CompletableFuture.failedFuture(new SessionError(code, message));
    }

    private record Prepared(
            String runId,
            String assistantEntryId,
            List<ToolCallContent> calls,
            Map<String, AgentTool> tools
    ) {
    }

    private record PreparedInvocation(
            AgentTool tool,
            Map<String, Object> arguments,
            AgentToolResult immediate,
            SessionRecordDraft.Replay replay
    ) {
    }

    private record Executed(AgentToolResult result, boolean error) {
    }

    private record BeforeDecision(
            boolean block,
            String reason,
            boolean terminate
    ) {
        private static final BeforeDecision ALLOW =
                new BeforeDecision(false, null, false);
    }

    private record ParallelInvocation(
            SessionRecordDraft.ToolStarted started,
            PreparedInvocation invocation
    ) {
    }

    public record Options(
            Map<String, SessionRecordDraft.Replay> replayByTool,
            Clock clock,
            ToolExecutionMode executionMode,
            SessionToolExecutionEventBus events,
            BeforeToolCall beforeToolCall,
            AfterToolCall afterToolCall
    ) {
        public static final Options DEFAULT = new Options(
                Map.of(), Clock.systemUTC(), ToolExecutionMode.SEQUENTIAL,
                null, null, null
        );

        public Options(
                Map<String, SessionRecordDraft.Replay> replayByTool,
                Clock clock
        ) {
            this(replayByTool, clock, ToolExecutionMode.SEQUENTIAL,
                    null, null, null);
        }

        public Options(
                Map<String, SessionRecordDraft.Replay> replayByTool,
                Clock clock,
                ToolExecutionMode executionMode
        ) {
            this(replayByTool, clock, executionMode, null, null, null);
        }

        public Options(
                Map<String, SessionRecordDraft.Replay> replayByTool,
                Clock clock,
                ToolExecutionMode executionMode,
                SessionToolExecutionEventBus events
        ) {
            this(replayByTool, clock, executionMode, events, null, null);
        }

        public Options {
            replayByTool = replayByTool == null ? Map.of() : Map.copyOf(replayByTool);
            clock = clock == null ? Clock.systemUTC() : clock;
            executionMode = executionMode == null
                    ? ToolExecutionMode.SEQUENTIAL : executionMode;
        }

        public SessionRecordDraft.Replay replay(String toolName) {
            return replayByTool.getOrDefault(
                    toolName, SessionRecordDraft.Replay.NEVER
            );
        }
    }

    public record UnresolvedTool(
            int toolIndex,
            String toolCallId,
            String toolName,
            String resultEntryId,
            SessionRecordDraft.Replay replay
    ) {
    }

    public sealed interface Outcome {
        String runId();

        String assistantEntryId();

        List<SessionEntry.Message> results();

        record Completed(
                String runId,
                String assistantEntryId,
                List<SessionEntry.Message> results
        ) implements Outcome {
            public Completed {
                results = List.copyOf(results);
            }
        }

        record Suspended(
                String runId,
                String assistantEntryId,
                List<SessionEntry.Message> results,
                UnresolvedTool unresolved
        ) implements Outcome {
            public Suspended {
                results = List.copyOf(results);
                Objects.requireNonNull(unresolved, "unresolved");
            }
        }

        record Aborted(
                String runId,
                String assistantEntryId,
                List<SessionEntry.Message> results
        ) implements Outcome {
            public Aborted {
                results = List.copyOf(results);
            }
        }
    }
}
