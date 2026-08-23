package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.*;
import io.github.idoly.pi.ai.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.*;

class ExtensionRuntimeTest {
    @TempDir
    Path temporary;

    @Test
    void loadsInOrderComposesHooksCommandsAndIsolatesPassiveFailures() {
        ArrayList<String> order = new ArrayList<>();
        AgentExtension later = extension("later", 10, api -> {
            order.add("later");
            api.registerCommand("status", "later", (args, ctx) ->
                    CompletableFuture.completedFuture("later:" + args));
            api.onContext((messages, context) -> {
                ArrayList<AgentMessage> changed = new ArrayList<>(messages);
                changed.add(UserMessage.text("later", 2));
                return CompletableFuture.completedFuture(changed);
            });
            api.onAfterTool((call, args, result, error, messages, context) ->
                    CompletableFuture.completedFuture(new AfterToolCallResult(
                            List.of(new TextContent("patched")), null,
                            null, false, true
                    )));
        });
        AgentExtension earlier = extension("earlier", -1, api -> {
            order.add("earlier");
            api.registerCommand("status", "earlier", (args, ctx) ->
                    CompletableFuture.completedFuture("earlier:" + args));
            api.onContext((messages, context) ->
                    CompletableFuture.failedFuture(new IllegalStateException("ignored")));
            api.onBeforeTool((call, args, messages, context) ->
                    CompletableFuture.completedFuture(BeforeToolCallResult.allow()));
        });
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(), List.of(later, earlier)
        ));
        assertEquals(List.of("earlier", "later"), order);
        assertEquals(List.of("status", "status:1"), runtime.commands().stream()
                .map(ExtensionCommand::name).toList());
        assertEquals("earlier:x", join(runtime.executeCommand("status", "x")));
        assertEquals("later:y", join(runtime.executeCommand("status:1", "y")));

        AgentOptions applied = runtime.applyTo(options());
        List<AgentMessage> transformed = join(applied.contextTransformer().transform(
                List.of(UserMessage.text("base", 1)), CancellationSignal.NONE
        ));
        assertEquals(2, transformed.size());
        assertEquals(1, runtime.failures().size());
        BeforeToolCallResult before = join(applied.beforeToolCall().apply(
                new ToolCallContent("call", "tool", Map.of()),
                new java.util.LinkedHashMap<>(), transformed,
                CancellationSignal.NONE
        ));
        assertFalse(before.block());
        AfterToolCallResult after = join(applied.afterToolCall().apply(
                new ToolCallContent("call", "tool", Map.of()), Map.of(),
                new AgentToolResult(List.of(new TextContent("old")), Map.of()),
                true, transformed, CancellationSignal.NONE
        ));
        assertEquals(List.of(new TextContent("patched")), after.content());
        assertEquals(false, after.error());
        assertEquals(true, after.terminate());
        runtime.close();
    }

    @Test
    void sessionLifecycleIsOrderedAndIdempotent() {
        ArrayList<String> calls = new ArrayList<>();
        AgentExtension first = extension("first", 0, api -> {
            api.onSessionStart(context -> {
                calls.add("start:first");
                return CompletableFuture.completedFuture(null);
            });
            api.onSessionShutdown(context -> {
                calls.add("stop:first");
                return CompletableFuture.completedFuture(null);
            });
        });
        AgentExtension second = extension("second", 1, api -> {
            api.onSessionStart(context -> {
                calls.add("start:second");
                return CompletableFuture.completedFuture(null);
            });
            api.onSessionShutdown(context -> {
                calls.add("stop:second");
                return CompletableFuture.completedFuture(null);
            });
        });
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(), List.of(second, first)
        ));
        join(runtime.startSession());
        join(runtime.startSession());
        join(runtime.shutdownSession());
        join(runtime.shutdownSession());
        assertEquals(List.of(
                "start:first", "start:second", "stop:second", "stop:first"
        ), calls);
        assertThrows(java.util.concurrent.CompletionException.class, () ->
                join(runtime.startSession()));
        runtime.close();
        assertEquals(4, calls.size());

        ArrayList<String> unopenedCalls = new ArrayList<>();
        ExtensionRuntime unopened = join(ExtensionRuntime.load(
                context(), List.of(extension("unopened", 0, api ->
                        api.onSessionShutdown(ignored -> {
                            unopenedCalls.add("stop");
                            return CompletableFuture.completedFuture(null);
                        })
                ))
        ));
        unopened.close();
        assertTrue(unopenedCalls.isEmpty());

        java.util.concurrent.atomic.AtomicInteger concurrentStops =
                new java.util.concurrent.atomic.AtomicInteger();
        ExtensionRuntime concurrent = join(ExtensionRuntime.load(
                context(), List.of(extension("concurrent", 0, api ->
                        api.onSessionShutdown(ignored ->
                                CompletableFuture.runAsync(() -> {
                                    concurrentStops.incrementAndGet();
                                    try {
                                        Thread.sleep(25);
                                    } catch (InterruptedException failure) {
                                        Thread.currentThread().interrupt();
                                    }
                                })
                        )
                ))
        ));
        join(concurrent.startSession());
        CompletableFuture.allOf(
                CompletableFuture.runAsync(concurrent::close),
                CompletableFuture.runAsync(concurrent::close)
        ).orTimeout(3, java.util.concurrent.TimeUnit.SECONDS).join();
        assertEquals(1, concurrentStops.get());
    }

    @Test
    void dynamicallyRegistersAndActivatesToolsOnExistingAgents() {
        java.util.concurrent.atomic.AtomicReference<ExtensionApi> api =
                new java.util.concurrent.atomic.AtomicReference<>();
        AgentTool first = tool("first");
        AgentTool second = tool("second");
        AgentExtension extension = extension("tools", 0, value -> {
            api.set(value);
            value.registerTool(first);
        });
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(), List.of(extension)
        ));
        Agent agent = runtime.createAgent(options());
        assertEquals(List.of("first"), agent.state().tools().stream()
                .map(AgentTool::name).toList());
        api.get().registerTool(second);
        assertEquals(List.of("first", "second"), agent.state().tools().stream()
                .map(AgentTool::name).toList());
        api.get().setActiveTools(List.of("second"));
        assertEquals(List.of("second"), agent.state().tools().stream()
                .map(AgentTool::name).toList());
        api.get().setActiveTools(List.of("first", "second"));
        assertEquals(List.of("first", "second"), agent.state().tools().stream()
                .map(AgentTool::name).toList());
        runtime.close();
    }

    @Test
    void beforeAgentStartChainsPromptAndSystemChanges() {
        AgentExtension first = extension("first", 0, api ->
                api.onBeforeAgentStart((prompts, system, context) -> {
                    ArrayList<AgentMessage> changed = new ArrayList<>(prompts);
                    changed.add(UserMessage.text("injected", 2));
                    return CompletableFuture.completedFuture(
                            new BeforeAgentStartResult(changed, system + " first")
                    );
                })
        );
        AgentExtension second = extension("second", 1, api ->
                api.onBeforeAgentStart((prompts, system, context) ->
                        CompletableFuture.completedFuture(
                                new BeforeAgentStartResult(
                                        prompts, system + " second"
                                )
                        ))
        );
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(), List.of(second, first)
        ));
        BeforeAgentStartResult result = join(runtime.beforeAgentStart(
                List.of(UserMessage.text("prompt", 1)), "system"
        ));
        assertEquals(2, result.prompts().size());
        assertEquals("system first second", result.systemPrompt());
        runtime.close();
    }

    @Test
    void beforeToolFailureBlocksFailSafeAndDuplicateIdsAreRejected() {
        AgentExtension extension = extension("gate", 0, api ->
                api.onBeforeTool((call, args, messages, context) -> {
                    throw new IllegalStateException("gate failed");
                })
        );
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(), List.of(extension)
        ));
        BeforeToolCallResult decision = join(runtime.applyTo(options())
                .beforeToolCall().apply(
                        new ToolCallContent("call", "tool", Map.of()),
                        new java.util.LinkedHashMap<>(), List.of(),
                        CancellationSignal.NONE
                ));
        assertTrue(decision.block());
        assertTrue(decision.reason().contains("gate"));
        assertEquals(1, runtime.failures().size());

        assertThrows(java.util.concurrent.CompletionException.class, () ->
                ExtensionRuntime.load(context(), List.of(extension, extension))
                        .toCompletableFuture().join());
        runtime.close();
    }

    private ExtensionContext context() {
        return new ExtensionContext(
                temporary, null, new ProviderRegistry(),
                CancellationSignal.NONE, Map.of()
        );
    }

    private static AgentExtension extension(
            String id,
            int order,
            java.util.function.Consumer<ExtensionApi> configure
    ) {
        return new AgentExtension() {
            @Override public String id() { return id; }
            @Override public int order() { return order; }
            @Override public void configure(ExtensionApi api) {
                configure.accept(api);
            }
        };
    }

    private static AgentTool tool(String name) {
        return new AgentTool() {
            @Override public ToolDefinition definition() {
                return new ToolDefinition(name, name, Map.of("type", "object"));
            }
            @Override public java.util.concurrent.CompletionStage<AgentToolResult> execute(
                    String id, Map<String, Object> arguments,
                    CancellationSignal cancellation,
                    java.util.function.Consumer<AgentToolResult> update
            ) {
                return CompletableFuture.completedFuture(new AgentToolResult(
                        List.of(new TextContent(name)), Map.of()
                ));
            }
        };
    }

    private static AgentOptions options() {
        Model model = new Model(
                "model", "Model", "test", "test", "https://example.test",
                false, List.of("text"), 1000, 100
        );
        ModelStream stream = (ignoredModel, ignoredContext, ignoredOptions) ->
                subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long n) { }
                    @Override public void cancel() { }
                });
        return new AgentOptions("system", model, stream);
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
