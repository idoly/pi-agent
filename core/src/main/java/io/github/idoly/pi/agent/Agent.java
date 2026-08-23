package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolResultMessage;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

/** Stateful, single-run agent with ordered and awaited lifecycle events. */
public final class Agent {
    private final Object stateLock = new Object();
    private final Object eventLock = new Object();
    private final AgentOptions options;
    private final Clock clock;
    private final ArrayList<AgentMessage> messages = new ArrayList<>();
    private final HashSet<String> pendingToolCalls = new HashSet<>();
    private final ArrayDeque<AgentMessage> steeringQueue = new ArrayDeque<>();
    private final ArrayDeque<AgentMessage> followUpQueue = new ArrayDeque<>();
    private final CopyOnWriteArrayList<AgentListener> listeners = new CopyOnWriteArrayList<>();

    private CompletionStage<Void> eventTail = CompletableFuture.completedFuture(null);
    private String systemPrompt;
    private io.github.idoly.pi.ai.Model model;
    private String thinkingLevel;
    private String sessionId;
    private List<AgentTool> tools;
    private boolean streaming;
    private AgentMessage streamingMessage;
    private String errorMessage;
    private CancellationSource activeCancellation;
    private AgentContext activeContext;
    private CompletableFuture<Void> activeRun;
    private QueueMode steeringMode;
    private QueueMode followUpMode;

    public Agent(AgentOptions options) {
        this(options, Clock.systemUTC());
    }

    Agent(AgentOptions options, Clock clock) {
        this.options = Objects.requireNonNull(options, "options");
        this.clock = Objects.requireNonNull(clock, "clock");
        systemPrompt = options.systemPrompt();
        model = options.model();
        thinkingLevel = options.thinkingLevel();
        sessionId = options.sessionId();
        tools = options.tools();
        steeringMode = options.steeringMode();
        followUpMode = options.followUpMode();
    }

    public AutoCloseable subscribe(AgentListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
        return () -> listeners.remove(listener);
    }

    public CompletionStage<Void> prompt(String text) {
        return prompt(UserMessage.text(text, clock.millis()));
    }

    public CompletionStage<Void> prompt(AgentMessage prompt) {
        Objects.requireNonNull(prompt, "prompt");
        return prompt(List.of(prompt));
    }

    /** Starts one run with an ordered batch of prompt messages. */
    public CompletionStage<Void> prompt(List<AgentMessage> prompts) {
        Objects.requireNonNull(prompts, "prompts");
        List<AgentMessage> safePrompts = List.copyOf(prompts);
        safePrompts.forEach(message -> Objects.requireNonNull(message, "prompt message"));
        return startRun(cancellation -> runPromptMessages(safePrompts, cancellation));
    }

    /** Continue from existing user/tool context, or from queued input after an assistant message. */
    public CompletionStage<Void> continueRun() {
        synchronized (stateLock) {
            if (activeRun != null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Agent is already processing a prompt")
                );
            }
            if (messages.isEmpty()) {
                return CompletableFuture.failedFuture(new IllegalStateException("No messages to continue from"));
            }
            AgentMessage last = messages.get(messages.size() - 1);
            if (last instanceof AssistantMessage) {
                List<AgentMessage> queued = drainQueue(true);
                if (queued.isEmpty()) {
                    queued = drainQueue(false);
                }
                if (queued.isEmpty()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Cannot continue from message role: assistant")
                    );
                }
                List<AgentMessage> prompts = queued;
                return startRun(cancellation -> runPromptMessages(prompts, cancellation));
            }
            return startRun(this::runContinuation);
        }
    }

    public void reset() {
        synchronized (stateLock) {
            if (activeRun != null) {
                throw new IllegalStateException("Agent is already processing. Wait for completion before resetting");
            }
            messages.clear();
            pendingToolCalls.clear();
            steeringQueue.clear();
            followUpQueue.clear();
            streaming = false;
            streamingMessage = null;
            errorMessage = null;
        }
    }

    public void systemPrompt(String value) {
        synchronized (stateLock) {
            systemPrompt = Objects.requireNonNull(value, "value");
        }
    }

    public void model(io.github.idoly.pi.ai.Model value) {
        synchronized (stateLock) {
            model = Objects.requireNonNull(value, "value");
        }
    }

    public void thinkingLevel(String value) {
        synchronized (stateLock) {
            thinkingLevel = Objects.requireNonNull(value, "value");
        }
    }

    public void tools(List<AgentTool> value) {
        synchronized (stateLock) {
            tools = List.copyOf(Objects.requireNonNull(value, "value"));
        }
    }

    public void messages(List<AgentMessage> value) {
        synchronized (stateLock) {
            if (activeRun != null) {
                throw new IllegalStateException("Cannot replace messages while the agent is running");
            }
            messages.clear();
            messages.addAll(List.copyOf(Objects.requireNonNull(value, "value")));
        }
    }

    public void sessionId(String value) {
        synchronized (stateLock) {
            sessionId = value;
        }
    }

    private CompletionStage<Void> startRun(
            java.util.function.Function<CancellationSource, CompletionStage<Void>> operation
    ) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CancellationSource cancellation = new CancellationSource();
        synchronized (stateLock) {
            if (activeRun != null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Agent is already processing a prompt")
                );
            }
            activeRun = result;
            activeCancellation = cancellation;
            activeContext = new AgentContext(systemPrompt, messages, tools);
            streaming = true;
            streamingMessage = null;
            errorMessage = null;
        }
        synchronized (eventLock) {
            eventTail = CompletableFuture.completedFuture(null);
        }

        CompletionStage<Void> running;
        try {
            running = operation.apply(cancellation);
        } catch (Throwable failure) {
            running = CompletableFuture.failedFuture(failure);
        }
        running.whenComplete((ignored, failure) -> {
            synchronized (stateLock) {
                streaming = false;
                streamingMessage = null;
                pendingToolCalls.clear();
                activeCancellation = null;
                activeContext = null;
                activeRun = null;
            }
            if (failure == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(unwrap(failure));
            }
        });
        return result;
    }

    public void steer(AgentMessage message) {
        synchronized (stateLock) {
            steeringQueue.addLast(Objects.requireNonNull(message, "message"));
        }
    }

    public void followUp(AgentMessage message) {
        synchronized (stateLock) {
            followUpQueue.addLast(Objects.requireNonNull(message, "message"));
        }
    }

    public void clearSteeringQueue() {
        synchronized (stateLock) {
            steeringQueue.clear();
        }
    }

    public void clearFollowUpQueue() {
        synchronized (stateLock) {
            followUpQueue.clear();
        }
    }

    public void clearAllQueues() {
        synchronized (stateLock) {
            steeringQueue.clear();
            followUpQueue.clear();
        }
    }

    public boolean hasQueuedMessages() {
        synchronized (stateLock) {
            return !steeringQueue.isEmpty() || !followUpQueue.isEmpty();
        }
    }

    public QueueMode steeringMode() {
        synchronized (stateLock) {
            return steeringMode;
        }
    }

    public void steeringMode(QueueMode mode) {
        synchronized (stateLock) {
            steeringMode = Objects.requireNonNull(mode, "mode");
        }
    }

    public QueueMode followUpMode() {
        synchronized (stateLock) {
            return followUpMode;
        }
    }

    public void followUpMode(QueueMode mode) {
        synchronized (stateLock) {
            followUpMode = Objects.requireNonNull(mode, "mode");
        }
    }

    public void abort() {
        CancellationSource cancellation;
        synchronized (stateLock) {
            cancellation = activeCancellation;
        }
        if (cancellation != null) {
            cancellation.cancel();
        }
    }

    public CompletionStage<Void> waitForIdle() {
        synchronized (stateLock) {
            return activeRun == null ? CompletableFuture.completedFuture(null) : activeRun;
        }
    }

    public AgentState state() {
        synchronized (stateLock) {
            return new AgentState(
                    systemPrompt,
                    model,
                    thinkingLevel,
                    tools,
                    messages,
                    streaming,
                    streamingMessage,
                    pendingToolCalls,
                    errorMessage
            );
        }
    }

    private CompletionStage<Void> runPromptMessages(
            List<AgentMessage> prompts,
            CancellationSource cancellation
    ) {
        List<AgentMessage> newMessages = new ArrayList<>(prompts);
        CompletionStage<Void> stage = emit(new AgentEvent.AgentStart(), cancellation)
                .thenCompose(ignored -> emit(new AgentEvent.TurnStart(), cancellation));
        for (AgentMessage prompt : prompts) {
            stage = stage.thenCompose(ignored -> emit(new AgentEvent.MessageStart(prompt), cancellation))
                    .thenCompose(ignored -> emit(new AgentEvent.MessageEnd(prompt), cancellation));
        }
        return stage.thenCompose(ignored -> pollMessages(true))
                .thenCompose(pending -> injectCurrentTurnAndContinue(
                        pending, newMessages, cancellation
                ))
                .thenCompose(ignored -> emit(new AgentEvent.AgentEnd(newMessages), cancellation));
    }

    private CompletionStage<Void> runContinuation(CancellationSource cancellation) {
        List<AgentMessage> newMessages = new ArrayList<>();
        return emit(new AgentEvent.AgentStart(), cancellation)
                .thenCompose(ignored -> emit(new AgentEvent.TurnStart(), cancellation))
                .thenCompose(ignored -> pollMessages(true))
                .thenCompose(pending -> injectCurrentTurnAndContinue(
                        pending, newMessages, cancellation
                ))
                .thenCompose(ignored -> emit(new AgentEvent.AgentEnd(newMessages), cancellation));
    }

    private CompletionStage<Void> runTurn(
            List<AgentMessage> newMessages,
            CancellationSource cancellation
    ) {
        AgentState snapshot = state();
        AgentContext loopContext = activeContextSnapshot();
        CompletionStage<List<AgentMessage>> transformed = options.contextTransformer() == null
                ? CompletableFuture.completedFuture(loopContext.messages())
                : options.contextTransformer().transform(loopContext.messages(), cancellation);
        return transformed
                .thenCompose(options.contextConverter()::convert)
                .thenCompose(llmMessages -> streamAssistant(
                        new ModelContext(
                                loopContext.systemPrompt(),
                                llmMessages,
                                loopContext.tools().stream().map(tool -> tool.definition()).toList()
                        ),
                        cancellation
                ))
                .thenCompose(assistant -> {
                    newMessages.add(assistant);
                    if (assistant.stopReason() == StopReason.ERROR
                            || assistant.stopReason() == StopReason.ABORTED) {
                        return emit(new AgentEvent.TurnEnd(assistant, List.of()), cancellation);
                    }
                    List<ToolCallContent> calls = assistant.content().stream()
                            .filter(ToolCallContent.class::isInstance)
                            .map(ToolCallContent.class::cast)
                            .toList();
                    if (calls.isEmpty()) {
                        return finishTurn(
                                assistant, List.of(), newMessages, cancellation, false, false
                        );
                    }
                    return executeToolBatch(assistant, calls, cancellation)
                            .thenCompose(batch -> {
                                newMessages.addAll(batch.messages());
                                return finishTurn(
                                        assistant, batch.messages(), newMessages,
                                        cancellation, true, batch.terminate()
                                );
                            });
                });
    }

    private CompletionStage<Void> finishTurn(
            AssistantMessage assistant,
            List<ToolResultMessage> toolResults,
            List<AgentMessage> newMessages,
            CancellationSource cancellation,
            boolean hadToolCalls,
            boolean terminate
    ) {
        return emit(new AgentEvent.TurnEnd(assistant, toolResults), cancellation)
                .thenCompose(ignored -> prepareNextTurn(
                        new TurnContext(
                                assistant,
                                toolResults,
                                activeContextSnapshot(),
                                newMessages
                        ),
                        cancellation
                ))
                .thenCompose(turnContext -> shouldStop(turnContext, cancellation))
                .thenCompose(stop -> {
                    if (stop) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return pollMessages(true).thenCompose(steering -> {
                        if (!steering.isEmpty()) {
                            return injectAndContinue(steering, newMessages, cancellation);
                        }
                        if (hadToolCalls && !terminate) {
                            return emit(new AgentEvent.TurnStart(), cancellation)
                                    .thenCompose(next -> runTurn(newMessages, cancellation));
                        }
                        return pollMessages(false).thenCompose(followUps ->
                                followUps.isEmpty()
                                        ? CompletableFuture.completedFuture(null)
                                        : injectAndContinue(followUps, newMessages, cancellation)
                        );
                    });
                });
    }

    private CompletionStage<TurnContext> prepareNextTurn(
            TurnContext context,
            CancellationSource cancellation
    ) {
        if (options.prepareNextTurn() == null) {
            return CompletableFuture.completedFuture(context);
        }
        return options.prepareNextTurn().prepare(context, cancellation)
                .thenApply(update -> {
                    NextTurnUpdate effective = update == null ? NextTurnUpdate.unchanged() : update;
                    AgentContext replacement = effective.context();
                    synchronized (stateLock) {
                        if (replacement != null) {
                            activeContext = replacement;
                        }
                        if (effective.model() != null) {
                            model = effective.model();
                        }
                        if (effective.thinkingLevel() != null) {
                            thinkingLevel = effective.thinkingLevel();
                        }
                        return new TurnContext(
                                context.message(),
                                context.toolResults(),
                                activeContext == null
                                        ? new AgentContext(systemPrompt, messages, tools)
                                        : activeContext,
                                context.newMessages()
                        );
                    }
                });
    }

    private CompletionStage<Boolean> shouldStop(
            TurnContext context,
            CancellationSource cancellation
    ) {
        return options.shouldStopAfterTurn() == null
                ? CompletableFuture.completedFuture(false)
                : options.shouldStopAfterTurn().shouldStop(context, cancellation);
    }

    private CompletionStage<List<AgentMessage>> pollMessages(boolean steering) {
        List<AgentMessage> queued = drainQueue(steering);
        AgentMessageSupplier supplier = steering
                ? options.steeringMessages()
                : options.followUpMessages();
        if (supplier == null) {
            return CompletableFuture.completedFuture(queued);
        }
        CompletionStage<List<AgentMessage>> supplied;
        try {
            supplied = supplier.get();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        if (supplied == null) {
            return CompletableFuture.completedFuture(queued);
        }
        return supplied.thenApply(messages -> {
            if (messages == null || messages.isEmpty()) {
                return queued;
            }
            ArrayList<AgentMessage> combined = new ArrayList<>(queued);
            combined.addAll(List.copyOf(messages));
            return List.copyOf(combined);
        });
    }

    private CompletionStage<Void> injectCurrentTurnAndContinue(
            List<AgentMessage> pending,
            List<AgentMessage> newMessages,
            CancellationSource cancellation
    ) {
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (AgentMessage message : pending) {
            newMessages.add(message);
            stage = stage.thenCompose(ignored -> emit(
                            new AgentEvent.MessageStart(message), cancellation
                    ))
                    .thenCompose(ignored -> emit(
                            new AgentEvent.MessageEnd(message), cancellation
                    ));
        }
        return stage.thenCompose(ignored -> runTurn(newMessages, cancellation));
    }

    private CompletionStage<Void> injectAndContinue(
            List<AgentMessage> queued,
            List<AgentMessage> newMessages,
            CancellationSource cancellation
    ) {
        CompletionStage<Void> stage = emit(new AgentEvent.TurnStart(), cancellation);
        for (AgentMessage message : queued) {
            newMessages.add(message);
            stage = stage.thenCompose(ignored -> emit(new AgentEvent.MessageStart(message), cancellation))
                    .thenCompose(ignored -> emit(new AgentEvent.MessageEnd(message), cancellation));
        }
        return stage.thenCompose(ignored -> runTurn(newMessages, cancellation));
    }

    private List<AgentMessage> drainQueue(boolean steering) {
        synchronized (stateLock) {
            ArrayDeque<AgentMessage> queue = steering ? steeringQueue : followUpQueue;
            QueueMode mode = steering ? steeringMode : followUpMode;
            if (queue.isEmpty()) {
                return List.of();
            }
            if (mode == QueueMode.ONE_AT_A_TIME) {
                return List.of(queue.removeFirst());
            }
            List<AgentMessage> drained = List.copyOf(queue);
            queue.clear();
            return drained;
        }
    }

    private CompletionStage<BatchResult> executeToolBatch(
            AssistantMessage assistant,
            List<ToolCallContent> calls,
            CancellationSource cancellation
    ) {
        boolean sequential = options.toolExecution() == ToolExecutionMode.SEQUENTIAL
                || calls.stream().anyMatch(call -> findTool(call.name())
                        .map(tool -> tool.executionMode() == ToolExecutionMode.SEQUENTIAL)
                        .orElse(false));
        if (assistant.stopReason() == StopReason.LENGTH) {
            return failTruncatedToolCalls(calls, cancellation);
        }
        return sequential
                ? executeSequential(assistant, calls, cancellation)
                : executeParallel(assistant, calls, cancellation);
    }

    private CompletionStage<BatchResult> failTruncatedToolCalls(
            List<ToolCallContent> calls,
            CancellationSource cancellation
    ) {
        List<FinalizedToolCall> finalized = new ArrayList<>();
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (ToolCallContent call : calls) {
            stage = stage.thenCompose(ignored -> emit(
                            new AgentEvent.ToolExecutionStart(call.id(), call.name(), call.arguments()),
                            cancellation
                    ))
                    .thenCompose(ignored -> {
                        FinalizedToolCall failure = new FinalizedToolCall(
                                call,
                                call.arguments(),
                                errorResult(
                                        "Tool call \"" + call.name() + "\" was not executed: "
                                                + "the response hit the output token limit, so its arguments may be truncated. "
                                                + "Re-issue the tool call with complete arguments.",
                                        false
                                ),
                                true
                        );
                        finalized.add(failure);
                        return emitToolEnd(failure, cancellation);
                    })
                    .thenCompose(ignored -> {
                        ToolResultMessage message = toToolResultMessage(finalized.get(finalized.size() - 1));
                        return emit(new AgentEvent.MessageStart(message), cancellation)
                                .thenCompose(next -> emit(new AgentEvent.MessageEnd(message), cancellation));
                    });
        }
        return stage.thenApply(ignored -> toBatchResult(finalized));
    }

    private CompletionStage<BatchResult> executeSequential(
            AssistantMessage assistant,
            List<ToolCallContent> calls,
            CancellationSource cancellation
    ) {
        List<FinalizedToolCall> finalized = new ArrayList<>();
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (ToolCallContent call : calls) {
            stage = stage.thenCompose(ignored -> prepare(call, assistant, cancellation))
                    .thenCompose(prepared -> executePrepared(prepared, cancellation))
                    .thenCompose(result -> {
                        finalized.add(result);
                        ToolResultMessage message = toToolResultMessage(result);
                        return emit(new AgentEvent.MessageStart(message), cancellation)
                                .thenCompose(ignored -> emit(new AgentEvent.MessageEnd(message), cancellation));
                    });
        }
        return stage.thenApply(ignored -> toBatchResult(finalized));
    }

    private CompletionStage<BatchResult> executeParallel(
            AssistantMessage assistant,
            List<ToolCallContent> calls,
            CancellationSource cancellation
    ) {
        List<PreparedToolCall> prepared = new ArrayList<>();
        CompletionStage<Void> preflight = CompletableFuture.completedFuture(null);
        for (ToolCallContent call : calls) {
            preflight = preflight.thenCompose(ignored -> prepare(call, assistant, cancellation))
                    .thenCompose(item -> {
                        if (item.immediateResult() == null) {
                            prepared.add(item);
                            return CompletableFuture.completedFuture(null);
                        }
                        FinalizedToolCall finalized = new FinalizedToolCall(
                                item.call(), item.arguments(), item.immediateResult(), item.immediateError()
                        );
                        return emitToolEnd(finalized, cancellation).thenRun(() -> prepared.add(
                                new PreparedToolCall(
                                        item.call(), item.tool(), item.arguments(),
                                        item.immediateResult(), item.immediateError(), true
                                )
                        ));
                    });
        }
        return preflight.thenCompose(ignored -> {
            List<CompletableFuture<FinalizedToolCall>> executions = prepared.stream()
                    .map(item -> executePrepared(item, cancellation).toCompletableFuture())
                    .toList();
            CompletableFuture<Void> all = CompletableFuture.allOf(
                    executions.toArray(CompletableFuture[]::new)
            );
            return all.thenCompose(done -> {
                List<FinalizedToolCall> finalized = executions.stream()
                        .map(CompletableFuture::join)
                        .toList();
                CompletionStage<Void> messagesStage = CompletableFuture.completedFuture(null);
                for (FinalizedToolCall result : finalized) {
                    ToolResultMessage message = toToolResultMessage(result);
                    messagesStage = messagesStage
                            .thenCompose(next -> emit(new AgentEvent.MessageStart(message), cancellation))
                            .thenCompose(next -> emit(new AgentEvent.MessageEnd(message), cancellation));
                }
                return messagesStage.thenApply(next -> toBatchResult(finalized));
            });
        });
    }

    private CompletionStage<PreparedToolCall> prepare(
            ToolCallContent call,
            AssistantMessage assistant,
            CancellationSource cancellation
    ) {
        return emit(new AgentEvent.ToolExecutionStart(call.id(), call.name(), call.arguments()), cancellation)
                .thenCompose(ignored -> {
                    AgentTool tool = findTool(call.name()).orElse(null);
                    if (tool == null) {
                        return completedPrepared(call, errorResult("Tool " + call.name() + " not found", false), true);
                    }
                    Map<String, Object> arguments;
                    try {
                        arguments = Map.copyOf(tool.prepareArguments(call.arguments()));
                        ToolArgumentValidator.validate(tool.definition().parameters(), arguments);
                        tool.validateArguments(arguments);
                    } catch (Throwable failure) {
                        return completedPrepared(call, errorResult(messageOf(failure), false), true);
                    }
                    if (options.beforeToolCall() == null) {
                        return CompletableFuture.completedFuture(
                                new PreparedToolCall(call, tool, arguments, null, false, false)
                        );
                    }
                    CompletionStage<BeforeToolCallResult> before;
                    try {
                        before = options.beforeToolCall().apply(
                                call, arguments, activeContextSnapshot().messages(), cancellation
                        );
                    } catch (Throwable failure) {
                        return CompletableFuture.completedFuture(new PreparedToolCall(
                                call, tool, arguments,
                                errorResult(messageOf(failure), false), true, false
                        ));
                    }
                    if (before == null) {
                        before = CompletableFuture.completedFuture(null);
                    }
                    return before.thenApply(result -> {
                                BeforeToolCallResult decision = result == null
                                        ? BeforeToolCallResult.allow()
                                        : result;
                                if (!decision.block()) {
                                    return new PreparedToolCall(call, tool, arguments, null, false, false);
                                }
                                String reason = decision.reason() == null
                                        ? "Tool execution was blocked"
                                        : decision.reason();
                                return new PreparedToolCall(
                                        call, tool, arguments,
                                        errorResult(reason, decision.terminate()), true, false
                                );
                            })
                            .exceptionally(failure -> new PreparedToolCall(
                                    call, tool, arguments,
                                    errorResult(messageOf(failure), false), true, false
                            ));
                });
    }

    private CompletionStage<PreparedToolCall> completedPrepared(
            ToolCallContent call,
            AgentToolResult result,
            boolean error
    ) {
        return CompletableFuture.completedFuture(
                new PreparedToolCall(call, null, call.arguments(), result, error, false)
        );
    }

    private CompletionStage<FinalizedToolCall> executePrepared(
            PreparedToolCall prepared,
            CancellationSource cancellation
    ) {
        if (prepared.immediateResult() != null) {
            FinalizedToolCall finalized = new FinalizedToolCall(
                    prepared.call(), prepared.arguments(), prepared.immediateResult(), prepared.immediateError()
            );
            return prepared.endEmitted()
                    ? CompletableFuture.completedFuture(finalized)
                    : emitToolEnd(finalized, cancellation).thenApply(ignored -> finalized);
        }

        List<CompletionStage<Void>> updates = new ArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean acceptingUpdates =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        CompletionStage<AgentToolResult> execution;
        try {
            execution = prepared.tool().execute(
                    prepared.call().id(),
                    prepared.arguments(),
                    cancellation,
                    update -> {
                        if (!acceptingUpdates.get()) {
                            return;
                        }
                        synchronized (updates) {
                            if (acceptingUpdates.get()) {
                                updates.add(emit(new AgentEvent.ToolExecutionUpdate(
                                        prepared.call().id(), prepared.call().name(),
                                        prepared.call().arguments(), update
                                ), cancellation));
                            }
                        }
                    }
            );
        } catch (Throwable failure) {
            execution = CompletableFuture.failedFuture(failure);
        }

        return execution.handle((result, failure) -> {
                    acceptingUpdates.set(false);
                    return failure == null
                            ? new ExecutedToolCall(result, false)
                            : new ExecutedToolCall(errorResult(messageOf(failure), false), true);
                })
                .thenCompose(executed -> awaitUpdates(updates).thenApply(ignored -> executed))
                .thenCompose(executed -> applyAfterHook(prepared, executed, cancellation))
                .thenCompose(finalized -> emitToolEnd(finalized, cancellation)
                        .thenApply(ignored -> finalized));
    }

    private CompletionStage<FinalizedToolCall> applyAfterHook(
            PreparedToolCall prepared,
            ExecutedToolCall executed,
            CancellationSource cancellation
    ) {
        if (options.afterToolCall() == null) {
            return CompletableFuture.completedFuture(new FinalizedToolCall(
                    prepared.call(), prepared.arguments(), executed.result(), executed.error()
            ));
        }
        CompletionStage<AfterToolCallResult> after;
        try {
            after = options.afterToolCall().apply(
                    prepared.call(), prepared.arguments(), executed.result(), executed.error(),
                    activeContextSnapshot().messages(), cancellation
            );
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(new FinalizedToolCall(
                    prepared.call(), prepared.arguments(),
                    errorResult(messageOf(failure), false), true
            ));
        }
        if (after == null) {
            after = CompletableFuture.completedFuture(null);
        }
        return after.handle((patch, failure) -> {
                    if (failure != null) {
                        return new FinalizedToolCall(
                                prepared.call(), prepared.arguments(),
                                errorResult(messageOf(failure), false), true
                        );
                    }
                    AfterToolCallResult effective = patch == null ? AfterToolCallResult.unchanged() : patch;
                    AgentToolResult original = executed.result();
                    AgentToolResult result = new AgentToolResult(
                            effective.content() == null ? original.content() : effective.content(),
                            effective.details() == null ? original.details() : effective.details(),
                            effective.usage() == null ? original.usage() : effective.usage(),
                            effective.terminate() == null ? original.terminate() : effective.terminate()
                    );
                    boolean error = effective.error() == null ? executed.error() : effective.error();
                    return new FinalizedToolCall(prepared.call(), prepared.arguments(), result, error);
                });
    }

    private CompletionStage<Void> awaitUpdates(List<CompletionStage<Void>> updates) {
        CompletableFuture<?>[] futures;
        synchronized (updates) {
            futures = updates.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new);
        }
        return CompletableFuture.allOf(futures);
    }

    private CompletionStage<Void> emitToolEnd(
            FinalizedToolCall finalized,
            CancellationSource cancellation
    ) {
        return emit(new AgentEvent.ToolExecutionEnd(
                finalized.call().id(), finalized.call().name(),
                finalized.result(), finalized.error()
        ), cancellation);
    }

    private BatchResult toBatchResult(List<FinalizedToolCall> finalized) {
        List<ToolResultMessage> toolMessages = finalized.stream()
                .map(this::toToolResultMessage)
                .toList();
        boolean terminate = !finalized.isEmpty()
                && finalized.stream().allMatch(item -> item.result().terminate());
        return new BatchResult(toolMessages, terminate);
    }

    private ToolResultMessage toToolResultMessage(FinalizedToolCall finalized) {
        return new ToolResultMessage(
                finalized.call().id(),
                finalized.call().name(),
                finalized.result().content(),
                finalized.result().details(),
                finalized.result().usage(),
                finalized.error(),
                clock.millis()
        );
    }

    private java.util.Optional<AgentTool> findTool(String name) {
        return activeContextSnapshot().tools().stream()
                .filter(tool -> tool.name().equals(name))
                .findFirst();
    }

    private AgentContext activeContextSnapshot() {
        synchronized (stateLock) {
            return activeContext == null
                    ? new AgentContext(systemPrompt, messages, tools)
                    : activeContext;
        }
    }

    private AgentToolResult errorResult(String message, boolean terminate) {
        return new AgentToolResult(
                List.of(new TextContent(message)), Map.of(), null, terminate
        );
    }

    private CompletionStage<AssistantMessage> streamAssistant(
            ModelContext context,
            CancellationSource cancellation
    ) {
        AgentState snapshot = state();
        CompletionStage<String> apiKey;
        try {
            apiKey = options.apiKeyResolver() == null
                    ? CompletableFuture.completedFuture(null)
                    : options.apiKeyResolver().resolve(snapshot.model().provider());
        } catch (Throwable failure) {
            return emitSyntheticFailure(failure, cancellation, false);
        }
        return apiKey.handle(ResolvedApiKey::new)
                .thenCompose(resolved -> {
                    if (resolved.failure() != null) {
                        return emitSyntheticFailure(resolved.failure(), cancellation, false);
                    }
                    CompletableFuture<AssistantMessage> result = new CompletableFuture<>();
                    Flow.Publisher<AssistantStreamEvent> publisher;
                    try {
                        publisher = options.modelStream().stream(
                                snapshot.model(),
                                context,
                                new StreamOptions(
                                        sessionId, resolved.value(), snapshot.thinkingLevel(), cancellation
                                )
                        );
                    } catch (Throwable failure) {
                        return emitSyntheticFailure(failure, cancellation, false);
                    }
                    publisher.subscribe(new AssistantSubscriber(result, cancellation));
                    return result;
                });
    }

    private CompletionStage<AssistantMessage> emitSyntheticFailure(
            Throwable failure,
            CancellationSource cancellation,
            boolean messageStarted
    ) {
        StopReason reason = cancellation.isCancelled() ? StopReason.ABORTED : StopReason.ERROR;
        AssistantMessage message = new AssistantMessage(
                List.of(),
                state().model().api(),
                state().model().provider(),
                state().model().id(),
                Usage.ZERO,
                reason,
                messageOf(failure),
                clock.millis()
        );
        CompletionStage<Void> stage = messageStarted
                ? CompletableFuture.completedFuture(null)
                : emit(new AgentEvent.MessageStart(message), cancellation);
        return stage.thenCompose(ignored -> emit(new AgentEvent.MessageEnd(message), cancellation))
                .thenApply(ignored -> message);
    }

    private CompletionStage<Void> emit(AgentEvent event, CancellationSource cancellation) {
        synchronized (eventLock) {
            CompletionStage<Void> next = eventTail.thenCompose(ignored -> dispatch(event, cancellation));
            eventTail = next;
            return next;
        }
    }

    private CompletionStage<Void> dispatch(AgentEvent event, CancellationSource cancellation) {
        reduce(event);
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (AgentListener listener : listeners) {
            stage = stage.thenCompose(ignored -> listener.onEvent(event, cancellation));
        }
        return stage;
    }

    private void reduce(AgentEvent event) {
        synchronized (stateLock) {
            if (event instanceof AgentEvent.MessageStart start
                    && start.message() instanceof AssistantMessage) {
                streamingMessage = start.message();
            } else if (event instanceof AgentEvent.MessageUpdate update) {
                streamingMessage = update.message();
            } else if (event instanceof AgentEvent.MessageEnd end) {
                streamingMessage = null;
                messages.add(end.message());
                if (activeContext != null) {
                    ArrayList<AgentMessage> contextMessages = new ArrayList<>(activeContext.messages());
                    contextMessages.add(end.message());
                    activeContext = new AgentContext(
                            activeContext.systemPrompt(), contextMessages, activeContext.tools()
                    );
                }
            } else if (event instanceof AgentEvent.ToolExecutionStart start) {
                pendingToolCalls.add(start.toolCallId());
            } else if (event instanceof AgentEvent.ToolExecutionEnd end) {
                pendingToolCalls.remove(end.toolCallId());
            } else if (event instanceof AgentEvent.TurnEnd end
                    && end.message().errorMessage() != null) {
                errorMessage = end.message().errorMessage();
            } else if (event instanceof AgentEvent.AgentEnd) {
                streamingMessage = null;
            }
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String messageOf(Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        return unwrapped.getMessage() == null ? unwrapped.toString() : unwrapped.getMessage();
    }

    private final class AssistantSubscriber implements Flow.Subscriber<AssistantStreamEvent> {
        private final CompletableFuture<AssistantMessage> result;
        private final CancellationSource cancellation;
        private Flow.Subscription subscription;
        private boolean messageStarted;
        private boolean terminal;

        private AssistantSubscriber(
                CompletableFuture<AssistantMessage> result,
                CancellationSource cancellation
        ) {
            this.result = result;
            this.cancellation = cancellation;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            cancellation.onCancel(this::cancelStream);
            if (!terminal) {
                subscription.request(1);
            }
        }

        @Override
        public void onNext(AssistantStreamEvent event) {
            synchronized (this) {
                if (terminal) {
                    return;
                }
            }
            handle(event).whenComplete((ignored, failure) -> {
                synchronized (this) {
                    if (failure != null && !terminal) {
                        terminal = true;
                        subscription.cancel();
                        result.completeExceptionally(unwrap(failure));
                    } else if (!terminal) {
                        subscription.request(1);
                    }
                }
            });
        }

        private CompletionStage<Void> handle(AssistantStreamEvent event) {
            if (event instanceof AssistantStreamEvent.Start start) {
                messageStarted = true;
                return emit(new AgentEvent.MessageStart(start.partial()), cancellation);
            }
            if (event instanceof AssistantStreamEvent.ContentStart update) {
                return emit(new AgentEvent.MessageUpdate(update.partial(), update), cancellation);
            }
            if (event instanceof AssistantStreamEvent.ContentDelta update) {
                return emit(new AgentEvent.MessageUpdate(update.partial(), update), cancellation);
            }
            if (event instanceof AssistantStreamEvent.ContentEnd update) {
                return emit(new AgentEvent.MessageUpdate(update.partial(), update), cancellation);
            }
            AssistantMessage message = event instanceof AssistantStreamEvent.Done done
                    ? done.message()
                    : ((AssistantStreamEvent.Error) event).message();
            synchronized (this) {
                terminal = true;
            }
            CompletionStage<Void> stage = messageStarted
                    ? CompletableFuture.completedFuture(null)
                    : emit(new AgentEvent.MessageStart(message), cancellation);
            return stage.thenCompose(ignored -> emit(new AgentEvent.MessageEnd(message), cancellation))
                    .thenRun(() -> result.complete(message));
        }

        private void cancelStream() {
            synchronized (this) {
                if (terminal) {
                    return;
                }
                terminal = true;
                if (subscription != null) {
                    subscription.cancel();
                }
            }
            emitSyntheticFailure(
                    new java.util.concurrent.CancellationException("Operation cancelled"),
                    cancellation,
                    messageStarted
            ).whenComplete((message, failure) -> {
                if (failure == null) {
                    result.complete(message);
                } else {
                    result.completeExceptionally(unwrap(failure));
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (this) {
                if (terminal) {
                    return;
                }
                terminal = true;
            }
            emitSyntheticFailure(throwable, cancellation, messageStarted)
                    .whenComplete((message, failure) -> {
                        if (failure == null) {
                            result.complete(message);
                        } else {
                            result.completeExceptionally(unwrap(failure));
                        }
                    });
        }

        @Override
        public void onComplete() {
            synchronized (this) {
                if (terminal) {
                    return;
                }
            }
            onError(new IllegalStateException("Assistant stream completed without a terminal event"));
        }
    }

    private record PreparedToolCall(
            ToolCallContent call,
            AgentTool tool,
            Map<String, Object> arguments,
            AgentToolResult immediateResult,
            boolean immediateError,
            boolean endEmitted
    ) { }

    private record ExecutedToolCall(AgentToolResult result, boolean error) { }

    private record FinalizedToolCall(
            ToolCallContent call,
            Map<String, Object> arguments,
            AgentToolResult result,
            boolean error
    ) { }

    private record BatchResult(List<ToolResultMessage> messages, boolean terminate) { }

    private record ResolvedApiKey(String value, Throwable failure) {
        private ResolvedApiKey(String value, Throwable failure) {
            this.value = value;
            this.failure = failure == null ? null : unwrap(failure);
        }
    }
}
