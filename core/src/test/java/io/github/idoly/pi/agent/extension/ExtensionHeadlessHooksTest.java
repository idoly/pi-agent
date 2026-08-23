package io.github.idoly.pi.agent.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.agent.AgentOptions;
import io.github.idoly.pi.agent.compaction.CompactionResult;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.JsonlSessionRepository;
import io.github.idoly.pi.agent.session.SessionRecordDraft;
import io.github.idoly.pi.agent.session.SessionRepository;
import io.github.idoly.pi.agent.skill.SkillDiscoveryOptions;
import io.github.idoly.pi.ai.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ExtensionHeadlessHooksTest {
    @TempDir
    Path temporary;

    @Test
    void chainsHeadlessHostHooksAndDiscoversContributedSkills() throws Exception {
        Path skill = temporary.resolve("extension-skills/demo/SKILL.md");
        Files.createDirectories(skill.getParent());
        Files.writeString(skill, "---\nname: extension-skill\n"
                + "description: Extension skill\n---\nInstructions\n");
        ArrayList<String> calls = new ArrayList<>();
        AgentExtension first = extension("first", 0, api -> {
            api.onResourcesDiscover((reason, context) ->
                    CompletableFuture.completedFuture(
                            new ExtensionResources(List.of(
                                    temporary.resolve("extension-skills")
                            ))
                    ));
            api.onInput((input, context) -> CompletableFuture.completedFuture(
                    ExtensionInputResult.transform(
                            input.text() + ":first", input.images()
                    )
            ));
            api.onSessionTransition((transition, context) ->
                    CompletableFuture.completedFuture(
                            SessionTransitionResult.allow()
                    ));
            api.onBeforeCompaction((compaction, context) -> {
                calls.add("before:first");
                return CompletableFuture.completedFuture(
                        BeforeCompactionResult.proceed()
                );
            });
            api.onAfterCompaction((compaction, context) -> {
                calls.add("after:first");
                return CompletableFuture.completedFuture(null);
            });
            api.onModelChange((change, context) -> {
                calls.add("model:" + change.source());
                return CompletableFuture.completedFuture(null);
            });
        });
        AgentExtension second = extension("second", 1, api -> {
            api.onInput((input, context) -> CompletableFuture.completedFuture(
                    ExtensionInputResult.transform(
                            input.text() + ":second", input.images()
                    )
            ));
            api.onSessionTransition((transition, context) ->
                    CompletableFuture.completedFuture(
                            SessionTransitionResult.cancel("policy")
                    ));
            api.onBeforeCompaction((compaction, context) -> {
                calls.add("before:second");
                return CompletableFuture.completedFuture(
                        BeforeCompactionResult.cancelled()
                );
            });
        });
        AgentExtension third = extension("third", 2, api ->
                api.onInput((input, context) ->
                        CompletableFuture.completedFuture(
                                ExtensionInputResult.continueWith(input)
                        )
                )
        );
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(null), List.of(second, third, first)
        ));

        ExtensionInputResult input = join(runtime.processInput(
                new ExtensionInput("prompt", List.of(), ExtensionInput.Source.RPC)
        ));
        assertEquals("prompt:first:second", input.text());
        assertEquals(ExtensionInputResult.Action.TRANSFORM, input.action());
        SessionTransitionResult transition = join(
                runtime.beforeSessionTransition(new SessionTransition(
                        SessionTransition.Reason.RESUME,
                        temporary.resolve("session.jsonl"), null
                ))
        );
        assertTrue(transition.cancel());
        assertEquals("policy", transition.reason());

        BeforeCompactionResult compaction = join(runtime.beforeCompaction(
                new ExtensionCompaction(
                        null, SessionRecordDraft.CompactionReason.MANUAL,
                        false, null
                )
        ));
        assertTrue(compaction.cancel());
        join(runtime.afterCompaction(new ExtensionCompaction(
                null, SessionRecordDraft.CompactionReason.MANUAL, false,
                new CompactionResult("summary", 1, Usage.ZERO, List.of(), null)
        )));
        join(runtime.modelChanged(new ExtensionModelChange(
                model(), null, ExtensionModelChange.Source.SET
        )));
        assertEquals(List.of(
                "before:first", "before:second", "after:first", "model:SET"
        ), calls);

        var skills = join(runtime.discoverSkills(
                new SkillDiscoveryOptions(
                        temporary, temporary, false, false,
                        List.of(), List.of()
                ),
                ExtensionResources.Reason.STARTUP
        ));
        assertEquals("extension-skill", skills.skills().getFirst().name());
        runtime.close();
    }

    @Test
    void durableStateUsesSessionCustomEntries() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession session = join(repository.create(
                new SessionRepository.CreateOptions("extension-state", null)
        ));
        AtomicReference<ExtensionStateStore> store = new AtomicReference<>();
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(session), List.of(extension("stateful", 0, api ->
                        store.set(api.state())
                ))
        ));
        ObjectMapper mapper = new ObjectMapper();
        join(store.get().put("settings", mapper.valueToTree(Map.of("n", 1))));
        join(store.get().put("settings", mapper.valueToTree(Map.of("n", 2))));
        assertEquals(2, join(store.get().get("settings"))
                .orElseThrow().path("n").asInt());
        assertEquals(List.of(2, 1), join(store.get().history("settings", 10))
                .stream().map(value -> value.path("n").asInt()).toList());
        assertEquals(2, join(session.findEntries()).stream()
                .filter(entry -> entry.type()
                        == io.github.idoly.pi.agent.session.SessionEntry.Type.CUSTOM)
                .count());
        var metadata = join(session.metadata());
        runtime.close();
        join(session.close());

        AgentSession reopened = join(repository.open(metadata));
        ExtensionStateStore replayed = new ExtensionStateStore(
                "stateful", reopened
        );
        assertEquals(2, join(replayed.get("settings"))
                .orElseThrow().path("n").asInt());
        assertEquals(List.of(2, 1), join(replayed.history("settings", 10))
                .stream().map(value -> value.path("n").asInt()).toList());
        join(reopened.close());
    }

    @Test
    void composesProviderHooksAndIsolatesExtensionFailures() {
        AtomicReference<StreamOptions> captured = new AtomicReference<>();
        ModelStream stream = (model, context, options) -> {
            captured.set(options);
            return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
        };
        AgentExtension extension = extension("provider", 0, api -> {
            api.onProviderHeaders((model, headers, context) -> {
                var changed = new java.util.LinkedHashMap<>(headers);
                changed.put("x-extension", "yes");
                return CompletableFuture.completedFuture(changed);
            });
            api.onProviderRequest((model, payload, context) ->
                    CompletableFuture.failedFuture(
                            new IllegalStateException("ignored")
                    ));
        });
        ExtensionRuntime runtime = join(ExtensionRuntime.load(
                context(null), List.of(extension)
        ));
        AgentOptions applied = runtime.applyTo(new AgentOptions(
                "system", model(), stream
        ));
        applied.modelStream().stream(
                model(), new ModelContext("", List.of()),
                new StreamOptions(null, null, "off", CancellationSignal.NONE)
        );
        ProviderRequestHooks hooks = captured.get().requestHooks();
        Map<String, String> headers = join(hooks.beforeHeaders(
                model(), Map.of("base", "value"), CancellationSignal.NONE
        ));
        assertEquals("yes", headers.get("x-extension"));
        Object payload = Map.of("model", "test");
        assertSame(payload, join(hooks.beforeRequest(
                model(), payload, CancellationSignal.NONE
        )));
        assertEquals(1, runtime.failures().size());
        runtime.close();
    }

    private ExtensionContext context(AgentSession session) {
        return new ExtensionContext(
                temporary, session, new ProviderRegistry(),
                CancellationSignal.NONE, Map.of()
        );
    }

    private static AgentExtension extension(
            String id, int order,
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

    private static Model model() {
        return new Model(
                "model", "Model", "test", "test",
                "https://example.test", false,
                List.of("text"), 1000, 100
        );
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
