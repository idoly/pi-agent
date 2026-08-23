package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.AfterToolCall;
import io.github.idoly.pi.agent.AfterToolCallResult;
import io.github.idoly.pi.agent.Agent;
import io.github.idoly.pi.agent.AgentOptions;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.agent.BeforeToolCall;
import io.github.idoly.pi.agent.BeforeToolCallResult;
import io.github.idoly.pi.agent.ContextTransformer;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.ProviderRegistry;
import io.github.idoly.pi.ai.ProviderRequestHooks;
import io.github.idoly.pi.agent.skill.SkillDiscoveryOptions;
import io.github.idoly.pi.agent.skill.SkillRegistry;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/** Ordered native Java extension host that composes directly with AgentOptions. */
public final class ExtensionRuntime implements AutoCloseable {
    private final ExtensionContext context;
    private final ProviderRegistry providers;
    private final boolean ownsProviders;
    private final ExtensionEventBus eventBus = new ExtensionEventBus();
    private final ArrayList<AgentTool> tools = new ArrayList<>();
    private final ArrayList<ExtensionCommand> commands = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.SessionHook>> sessionStarts =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.SessionHook>> sessionShutdowns =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.ResourceDiscoveryHook>>
            resourceDiscoveries = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.InputHook>> inputs =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.SessionTransitionHook>>
            sessionTransitions = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.BeforeCompactionHook>>
            beforeCompactions = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.AfterCompactionHook>>
            afterCompactions = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.ModelChangeHook>> modelChanges =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.ProviderHeadersHook>>
            providerHeaders = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.ProviderRequestHook>>
            providerRequests = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.ProviderResponseHook>>
            providerResponses = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.BeforeAgentStartHook>>
            beforeAgentStarts = new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.AgentEventHook>> agentEvents =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.ContextHook>> contexts =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.BeforeToolHook>> beforeTools =
            new ArrayList<>();
    private final ArrayList<Owned<ExtensionHooks.AfterToolHook>> afterTools =
            new ArrayList<>();
    private final CopyOnWriteArrayList<ExtensionFailure> failures =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Agent> agents =
            new CopyOnWriteArrayList<>();
    private final java.util.concurrent.ConcurrentHashMap<Agent, List<AgentTool>>
            agentAvailableTools = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile List<String> activeToolNames;
    private CompletionStage<Void> sessionLifecycle =
            CompletableFuture.completedFuture(null);
    private boolean sessionStarted;
    private boolean sessionShutdown;
    private boolean closed;
    private CompletionStage<Void> closeStage;

    private ExtensionRuntime(ExtensionContext context) {
        this.context = context;
        this.ownsProviders = context.providers() == null;
        this.providers = ownsProviders
                ? new ProviderRegistry() : context.providers();
    }

    public static CompletionStage<ExtensionRuntime> load(
            ExtensionContext context,
            List<AgentExtension> extensions
    ) {
        Objects.requireNonNull(context, "context");
        ArrayList<AgentExtension> ordered = new ArrayList<>(extensions);
        ordered.sort(Comparator.comparingInt(AgentExtension::order)
                .thenComparing(AgentExtension::id));
        LinkedHashMap<String, AgentExtension> unique = new LinkedHashMap<>();
        for (AgentExtension extension : ordered) {
            Objects.requireNonNull(extension, "extension");
            if (extension.id() == null || extension.id().isBlank()) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException(
                                "extension id must not be blank"
                        )
                );
            }
            if (unique.putIfAbsent(extension.id(), extension) != null) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException(
                                "Duplicate extension id " + extension.id()
                        )
                );
            }
        }
        ExtensionRuntime runtime = new ExtensionRuntime(context);
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (AgentExtension extension : unique.values()) {
            stage = stage.thenCompose(ignored -> extension.initialize(
                    runtime.api(extension.id())
            ));
        }
        return stage.thenApply(ignored -> runtime);
    }

    public static CompletionStage<ExtensionRuntime> loadServices(
            ExtensionContext context,
            ClassLoader loader
    ) {
        ArrayList<AgentExtension> extensions = new ArrayList<>();
        ServiceLoader.load(AgentExtension.class, loader).forEach(extensions::add);
        return load(context, extensions);
    }

    public List<AgentTool> tools() {
        return List.copyOf(tools);
    }

    public List<ExtensionCommand> commands() {
        return List.copyOf(commands);
    }

    public List<ExtensionFailure> failures() {
        return List.copyOf(failures);
    }

    public ProviderRegistry providers() {
        return providers;
    }

    public CompletionStage<Object> executeCommand(
            String name,
            String arguments
    ) {
        requireOpen();
        ExtensionCommand command = commands.stream()
                .filter(value -> value.name().equals(name))
                .findFirst().orElseThrow(() ->
                        new IllegalArgumentException("Unknown command: " + name)
                );
        try {
            return command.handler().execute(arguments, context);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public synchronized CompletionStage<Void> startSession() {
        requireOpen();
        if (sessionShutdown) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Extension session has already shut down"
            ));
        }
        if (sessionStarted) return sessionLifecycle;
        sessionStarted = true;
        sessionLifecycle = sessionLifecycle.thenCompose(ignored ->
                runPassive(sessionStarts, hook -> hook.handle(context))
        );
        return sessionLifecycle;
    }

    public synchronized CompletionStage<Void> shutdownSession() {
        return scheduleShutdown();
    }

    private CompletionStage<Void> scheduleShutdown() {
        if (sessionShutdown) return sessionLifecycle;
        sessionShutdown = true;
        if (sessionStarted) {
            sessionLifecycle = sessionLifecycle.thenCompose(ignored ->
                    runPassive(
                            sessionShutdowns.reversed(),
                            hook -> hook.handle(context)
                    )
            );
        }
        return sessionLifecycle;
    }

    public CompletionStage<ExtensionResources> discoverResources(
            ExtensionResources.Reason reason
    ) {
        requireOpen();
        Objects.requireNonNull(reason, "reason");
        CompletionStage<ExtensionResources> stage =
                CompletableFuture.completedFuture(ExtensionResources.EMPTY);
        for (Owned<ExtensionHooks.ResourceDiscoveryHook> owned
                : resourceDiscoveries) {
            stage = stage.thenCompose(current -> passiveValue(
                    owned,
                    () -> owned.value().discover(reason, context)
                            .thenApply(current::merge),
                    current
            ));
        }
        return stage;
    }

    public CompletionStage<SkillRegistry> discoverSkills(
            SkillDiscoveryOptions options, ExtensionResources.Reason reason
    ) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(reason, "reason");
        return discoverResources(reason).thenApply(resources -> {
            ArrayList<Path> explicit = new ArrayList<>(options.explicitPaths());
            explicit.addAll(resources.skillPaths());
            return SkillRegistry.discover(new SkillDiscoveryOptions(
                    options.home(), options.cwd(), options.projectTrusted(),
                    options.discoverDefaults(), explicit, options.packagePaths()
            ));
        });
    }

    public CompletionStage<ExtensionInputResult> processInput(
            ExtensionInput input
    ) {
        requireOpen();
        Objects.requireNonNull(input, "input");
        CompletionStage<ExtensionInputResult> stage =
                CompletableFuture.completedFuture(
                        ExtensionInputResult.continueWith(input)
                );
        for (Owned<ExtensionHooks.InputHook> owned : inputs) {
            stage = stage.thenCompose(current -> {
                if (current.action() == ExtensionInputResult.Action.HANDLED) {
                    return CompletableFuture.completedFuture(current);
                }
                ExtensionInput next = new ExtensionInput(
                        current.text(), current.images(), input.source()
                );
                return passiveValue(
                        owned,
                        () -> owned.value().handle(next, context)
                                .thenApply(value -> mergeInputResult(
                                        current, value
                                )),
                        current
                );
            });
        }
        return stage;
    }

    private static ExtensionInputResult mergeInputResult(
            ExtensionInputResult current,
            ExtensionInputResult next
    ) {
        if (next == null) return current;
        if (current.action() == ExtensionInputResult.Action.TRANSFORM
                && next.action() == ExtensionInputResult.Action.CONTINUE) {
            return ExtensionInputResult.transform(
                    next.text(), next.images()
            );
        }
        return next;
    }

    public CompletionStage<SessionTransitionResult> beforeSessionTransition(
            SessionTransition transition
    ) {
        requireOpen();
        Objects.requireNonNull(transition, "transition");
        CompletionStage<SessionTransitionResult> stage =
                CompletableFuture.completedFuture(
                        SessionTransitionResult.allow()
                );
        for (Owned<ExtensionHooks.SessionTransitionHook> owned
                : sessionTransitions) {
            stage = stage.thenCompose(current -> current.cancel()
                    ? CompletableFuture.completedFuture(current)
                    : passiveValue(
                            owned,
                            () -> owned.value().before(transition, context)
                                    .thenApply(value -> value == null
                                            ? current : value),
                            current
                    ));
        }
        return stage;
    }

    public CompletionStage<BeforeCompactionResult> beforeCompaction(
            ExtensionCompaction compaction
    ) {
        requireOpen();
        Objects.requireNonNull(compaction, "compaction");
        CompletionStage<BeforeCompactionResult> stage =
                CompletableFuture.completedFuture(
                        BeforeCompactionResult.proceed()
                );
        for (Owned<ExtensionHooks.BeforeCompactionHook> owned
                : beforeCompactions) {
            stage = stage.thenCompose(current ->
                    current.cancel() || current.replacement() != null
                            ? CompletableFuture.completedFuture(current)
                            : passiveValue(
                                    owned,
                                    () -> owned.value().before(
                                            compaction, context
                                    ).thenApply(value -> value == null
                                            ? current : value),
                                    current
                            ));
        }
        return stage;
    }

    public CompletionStage<Void> afterCompaction(
            ExtensionCompaction compaction
    ) {
        requireOpen();
        Objects.requireNonNull(compaction, "compaction");
        return runPassive(afterCompactions, hook ->
                hook.after(compaction, context));
    }

    public CompletionStage<Void> modelChanged(
            ExtensionModelChange change
    ) {
        requireOpen();
        Objects.requireNonNull(change, "change");
        return runPassive(modelChanges, hook ->
                hook.changed(change, context));
    }

    public ExtensionAgent createExtensionAgent(AgentOptions options) {
        Agent agent = createAgent(options);
        return new ExtensionAgent(
                this, agent, options.systemPrompt()
        );
    }

    CompletionStage<BeforeAgentStartResult> beforeAgentStart(
            List<AgentMessage> prompts,
            String systemPrompt
    ) {
        CompletionStage<BeforeAgentStartResult> stage =
                CompletableFuture.completedFuture(new BeforeAgentStartResult(
                        prompts, systemPrompt
                ));
        for (Owned<ExtensionHooks.BeforeAgentStartHook> owned
                : beforeAgentStarts) {
            stage = stage.thenCompose(current -> passiveValue(
                    owned,
                    () -> owned.value().handle(
                            current.prompts(), current.systemPrompt(), context
                    ),
                    current
            ));
        }
        return stage;
    }

    public Agent createAgent(AgentOptions options) {
        requireOpen();
        Agent agent = new Agent(applyTo(options));
        agents.add(agent);
        agentAvailableTools.put(agent, mergeTools(options.tools(), tools));
        agent.subscribe((event, cancellation) -> runPassive(
                agentEvents,
                hook -> hook.handle(
                        event, context.withCancellation(cancellation)
                )
        ));
        return agent;
    }

    public AgentOptions applyTo(AgentOptions options) {
        requireOpen();
        Objects.requireNonNull(options, "options");
        List<AgentTool> combinedTools = activeTools(
                mergeTools(options.tools(), tools)
        );
        ModelStream modelStream = (model, modelContext, streamOptions) ->
                options.modelStream().stream(
                        model, modelContext,
                        streamOptions.withRequestHooks(providerHooks(
                                streamOptions.requestHooks()
                        ))
                );
        ContextTransformer contextTransformer = composeContext(
                options.contextTransformer()
        );
        BeforeToolCall beforeToolCall = composeBefore(options.beforeToolCall());
        AfterToolCall afterToolCall = composeAfter(options.afterToolCall());
        return new AgentOptions(
                options.systemPrompt(), options.model(), options.thinkingLevel(),
                options.sessionId(), modelStream,
                options.contextConverter(), contextTransformer,
                options.apiKeyResolver(), combinedTools, options.toolExecution(),
                options.steeringMode(), options.followUpMode(), beforeToolCall,
                afterToolCall, options.prepareNextTurn(),
                options.shouldStopAfterTurn(), options.steeringMessages(),
                options.followUpMessages()
        );
    }

    private ProviderRequestHooks providerHooks(ProviderRequestHooks existing) {
        return new ProviderRequestHooks() {
            @Override
            public CompletionStage<Map<String, String>> beforeHeaders(
                    Model model, Map<String, String> headers,
                    io.github.idoly.pi.ai.CancellationSignal cancellation
            ) {
                CompletionStage<Map<String, String>> stage =
                        existing.beforeHeaders(model, headers, cancellation);
                for (Owned<ExtensionHooks.ProviderHeadersHook> owned
                        : providerHeaders) {
                    stage = stage.thenCompose(current -> passiveValue(
                            owned,
                            () -> owned.value().transform(
                                    model, current,
                                    context.withCancellation(cancellation)
                            ).thenApply(value -> value == null
                                    ? current : value),
                            current
                    ));
                }
                return stage.thenApply(Map::copyOf);
            }

            @Override
            public CompletionStage<Object> beforeRequest(
                    Model model, Object payload,
                    io.github.idoly.pi.ai.CancellationSignal cancellation
            ) {
                CompletionStage<Object> stage = existing.beforeRequest(
                        model, payload, cancellation
                );
                for (Owned<ExtensionHooks.ProviderRequestHook> owned
                        : providerRequests) {
                    stage = stage.thenCompose(current -> passiveValue(
                            owned,
                            () -> owned.value().transform(
                                    model, current,
                                    context.withCancellation(cancellation)
                            ).thenApply(value -> value == null
                                    ? current : value),
                            current
                    ));
                }
                return stage;
            }

            @Override
            public CompletionStage<Void> afterResponse(
                    Model model, int status,
                    Map<String, List<String>> headers,
                    io.github.idoly.pi.ai.CancellationSignal cancellation
            ) {
                return existing.afterResponse(
                        model, status, headers, cancellation
                ).thenCompose(ignored -> runPassive(
                        providerResponses,
                        hook -> hook.handle(
                                model, status, headers,
                                context.withCancellation(cancellation)
                        )
                ));
            }
        };
    }

    private ContextTransformer composeContext(ContextTransformer existing) {
        if (existing == null && contexts.isEmpty()) return null;
        return (messages, cancellation) -> {
            CompletionStage<List<AgentMessage>> stage = existing == null
                    ? CompletableFuture.completedFuture(messages)
                    : existing.transform(messages, cancellation);
            for (Owned<ExtensionHooks.ContextHook> owned : contexts) {
                stage = stage.thenCompose(current -> passiveValue(
                        owned,
                        () -> owned.value().transform(
                                current,
                                context.withCancellation(cancellation)
                        ),
                        current
                ));
            }
            return stage.thenApply(List::copyOf);
        };
    }

    private BeforeToolCall composeBefore(BeforeToolCall existing) {
        if (existing == null && beforeTools.isEmpty()) return null;
        return (call, arguments, messages, cancellation) -> {
            CompletionStage<BeforeToolCallResult> stage = existing == null
                    ? CompletableFuture.completedFuture(
                            BeforeToolCallResult.allow()
                    )
                    : existing.apply(
                            call, arguments, messages, cancellation
                    );
            for (Owned<ExtensionHooks.BeforeToolHook> owned : beforeTools) {
                stage = stage.thenCompose(decision -> {
                    if (decision.block()) {
                        return CompletableFuture.completedFuture(decision);
                    }
                    try {
                        return owned.value().handle(
                                call, arguments, messages,
                                context.withCancellation(cancellation)
                        ).exceptionally(failure -> {
                            recordFailure(owned.id(), "before_tool", failure);
                            return new BeforeToolCallResult(
                                    true,
                                    "Extension " + owned.id()
                                            + " failed before tool execution",
                                    false
                            );
                        });
                    } catch (Throwable failure) {
                        recordFailure(owned.id(), "before_tool", failure);
                        return CompletableFuture.completedFuture(
                                new BeforeToolCallResult(
                                        true,
                                        "Extension " + owned.id()
                                                + " failed before tool execution",
                                        false
                                )
                        );
                    }
                });
            }
            return stage;
        };
    }

    private AfterToolCall composeAfter(AfterToolCall existing) {
        if (existing == null && afterTools.isEmpty()) return null;
        return (call, arguments, result, error, messages, cancellation) -> {
            CompletionStage<PatchedResult> stage;
            if (existing == null) {
                stage = CompletableFuture.completedFuture(
                        new PatchedResult(result, error)
                );
            } else {
                stage = existing.apply(
                        call, arguments, result, error, messages, cancellation
                ).thenApply(patch -> patch(result, error, patch));
            }
            for (Owned<ExtensionHooks.AfterToolHook> owned : afterTools) {
                stage = stage.thenCompose(current -> passiveValue(
                        owned,
                        () -> owned.value().handle(
                                call, arguments, current.result(), current.error(),
                                messages, context.withCancellation(cancellation)
                        ).thenApply(patch -> patch(
                                current.result(), current.error(), patch
                        )),
                        current
                ));
            }
            return stage.thenApply(current -> new AfterToolCallResult(
                    current.result().content(), current.result().details(),
                    current.result().usage(), current.error(),
                    current.result().terminate()
            ));
        };
    }

    private static PatchedResult patch(
            AgentToolResult result,
            boolean error,
            AfterToolCallResult patch
    ) {
        if (patch == null) return new PatchedResult(result, error);
        List<ContentBlock> content = patch.content() == null
                ? result.content() : patch.content();
        Map<String, Object> details = patch.details() == null
                ? result.details() : patch.details();
        AgentToolResult updated = new AgentToolResult(
                content, details,
                patch.usage() == null ? result.usage() : patch.usage(),
                patch.terminate() == null
                        ? result.terminate() : patch.terminate()
        );
        return new PatchedResult(
                updated, patch.error() == null ? error : patch.error()
        );
    }

    private <T> CompletionStage<Void> runPassive(
            List<Owned<T>> handlers,
            Function<T, CompletionStage<Void>> invocation
    ) {
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (Owned<T> owned : handlers) {
            stage = stage.thenCompose(ignored -> passiveValue(
                    owned, () -> invocation.apply(owned.value()), null
            ));
        }
        return stage;
    }

    private <T> CompletionStage<T> passiveValue(
            Owned<?> owned,
            java.util.function.Supplier<CompletionStage<T>> invocation,
            T fallback
    ) {
        try {
            return invocation.get().exceptionally(failure -> {
                recordFailure(owned.id(), "hook", failure);
                return fallback;
            });
        } catch (Throwable failure) {
            recordFailure(owned.id(), "hook", failure);
            return CompletableFuture.completedFuture(fallback);
        }
    }

    private void recordFailure(String extensionId, String phase, Throwable error) {
        Throwable failure = error instanceof CompletionException completion
                && completion.getCause() != null
                ? completion.getCause() : error;
        failures.add(new ExtensionFailure(
                extensionId, phase, failure.getClass().getName(),
                failure.getMessage()
        ));
    }

    private ExtensionApi api(String id) {
        return new ExtensionApi() {
            @Override public String extensionId() { return id; }
            @Override public void registerTool(AgentTool tool) {
                AgentTool registered = Objects.requireNonNull(tool, "tool");
                tools.removeIf(value -> value.name().equals(registered.name()));
                tools.add(registered);
                agentAvailableTools.replaceAll((agent, available) ->
                        mergeTools(available, List.of(registered))
                );
                refreshAgentTools();
            }
            @Override public List<AgentTool> getAllTools() {
                LinkedHashMap<String, AgentTool> all = new LinkedHashMap<>();
                tools.forEach(tool -> all.put(tool.name(), tool));
                agentAvailableTools.values().forEach(available ->
                        available.forEach(tool -> all.put(tool.name(), tool))
                );
                return List.copyOf(all.values());
            }
            @Override public List<String> getActiveTools() {
                List<String> selected = activeToolNames;
                return selected == null
                        ? tools.stream().map(AgentTool::name).toList()
                        : selected;
            }
            @Override public void setActiveTools(List<String> names) {
                Objects.requireNonNull(names, "names");
                java.util.LinkedHashSet<String> known = getAllTools().stream()
                        .map(AgentTool::name)
                        .collect(java.util.stream.Collectors.toCollection(
                                java.util.LinkedHashSet::new
                        ));
                activeToolNames = names.stream().filter(known::contains)
                        .distinct().toList();
                refreshAgentTools();
            }
            @Override public void registerProvider(
                    io.github.idoly.pi.ai.ModelProvider provider
            ) { providers.register(provider); }
            @Override public ExtensionStateStore state() {
                return new ExtensionStateStore(id, context.session());
            }
            @Override public void registerCommand(
                    String name, String description,
                    ExtensionCommand.Handler handler
            ) {
                if (name == null || name.isBlank()
                        || name.chars().anyMatch(Character::isWhitespace)) {
                    throw new IllegalArgumentException("Invalid command name");
                }
                String effective = commandName(name);
                commands.add(new ExtensionCommand(
                        effective, description, handler, id
                ));
            }
            @Override public void onSessionStart(
                    ExtensionHooks.SessionHook hook
            ) { sessionStarts.add(new Owned<>(id, hook)); }
            @Override public void onSessionShutdown(
                    ExtensionHooks.SessionHook hook
            ) { sessionShutdowns.add(new Owned<>(id, hook)); }
            @Override public void onResourcesDiscover(
                    ExtensionHooks.ResourceDiscoveryHook hook
            ) { resourceDiscoveries.add(new Owned<>(id, hook)); }
            @Override public void onInput(
                    ExtensionHooks.InputHook hook
            ) { inputs.add(new Owned<>(id, hook)); }
            @Override public void onSessionTransition(
                    ExtensionHooks.SessionTransitionHook hook
            ) { sessionTransitions.add(new Owned<>(id, hook)); }
            @Override public void onBeforeCompaction(
                    ExtensionHooks.BeforeCompactionHook hook
            ) { beforeCompactions.add(new Owned<>(id, hook)); }
            @Override public void onAfterCompaction(
                    ExtensionHooks.AfterCompactionHook hook
            ) { afterCompactions.add(new Owned<>(id, hook)); }
            @Override public void onModelChange(
                    ExtensionHooks.ModelChangeHook hook
            ) { modelChanges.add(new Owned<>(id, hook)); }
            @Override public void onProviderHeaders(
                    ExtensionHooks.ProviderHeadersHook hook
            ) { providerHeaders.add(new Owned<>(id, hook)); }
            @Override public void onProviderRequest(
                    ExtensionHooks.ProviderRequestHook hook
            ) { providerRequests.add(new Owned<>(id, hook)); }
            @Override public void onProviderResponse(
                    ExtensionHooks.ProviderResponseHook hook
            ) { providerResponses.add(new Owned<>(id, hook)); }
            @Override public void onBeforeAgentStart(
                    ExtensionHooks.BeforeAgentStartHook hook
            ) { beforeAgentStarts.add(new Owned<>(id, hook)); }
            @Override public void onAgentEvent(
                    ExtensionHooks.AgentEventHook hook
            ) { agentEvents.add(new Owned<>(id, hook)); }
            @Override public void onContext(
                    ExtensionHooks.ContextHook hook
            ) { contexts.add(new Owned<>(id, hook)); }
            @Override public void onBeforeTool(
                    ExtensionHooks.BeforeToolHook hook
            ) { beforeTools.add(new Owned<>(id, hook)); }
            @Override public void onAfterTool(
                    ExtensionHooks.AfterToolHook hook
            ) { afterTools.add(new Owned<>(id, hook)); }
            @Override public void onEvent(
                    String topic, ExtensionEventBus.Listener listener
            ) { eventBus.on(topic, listener); }
            @Override public void emit(String topic, Object value) {
                eventBus.emit(topic, value);
            }
        };
    }

    private String commandName(String requested) {
        if (commands.stream().noneMatch(value -> value.name().equals(requested))) {
            return requested;
        }
        int suffix = 1;
        while (true) {
            String candidate = requested + ':' + suffix++;
            if (commands.stream().noneMatch(
                    value -> value.name().equals(candidate)
            )) return candidate;
        }
    }

    private List<AgentTool> activeTools(List<AgentTool> available) {
        List<String> selected = activeToolNames;
        if (selected == null) return available;
        java.util.LinkedHashSet<String> names =
                new java.util.LinkedHashSet<>(selected);
        return available.stream().filter(tool -> names.contains(tool.name()))
                .toList();
    }

    private void refreshAgentTools() {
        for (Agent agent : agents) {
            agent.tools(activeTools(agentAvailableTools.getOrDefault(
                    agent, mergeTools(agent.state().tools(), tools)
            )));
        }
    }

    private static List<AgentTool> mergeTools(
            List<AgentTool> base,
            List<AgentTool> additions
    ) {
        LinkedHashMap<String, AgentTool> values = new LinkedHashMap<>();
        base.forEach(tool -> values.put(tool.name(), tool));
        additions.forEach(tool -> values.put(tool.name(), tool));
        return List.copyOf(values.values());
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Extension runtime is closed");
    }

    @Override
    public void close() {
        CompletionStage<Void> stage;
        synchronized (this) {
            if (closeStage == null) {
                closed = true;
                closeStage = scheduleShutdown().thenRun(() -> {
                    if (ownsProviders) providers.close();
                });
            }
            stage = closeStage;
        }
        stage.toCompletableFuture().join();
    }

    private record Owned<T>(String id, T value) {
        private Owned {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(value, "value");
        }
    }

    private record PatchedResult(AgentToolResult result, boolean error) {
    }

    public record ExtensionFailure(
            String extensionId,
            String phase,
            String errorType,
            String message
    ) {
    }
}
