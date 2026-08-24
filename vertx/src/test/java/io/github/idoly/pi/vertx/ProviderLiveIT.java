package io.github.idoly.pi.vertx;

import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.CacheRetention;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.ThinkingContent;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.ToolDefinition;
import io.github.idoly.pi.ai.UserMessage;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Credential-gated service tests executed only by -Pprovider-live-tests. */
class ProviderLiveIT {
    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final boolean DEEP = Boolean.parseBoolean(
            env("PI_LIVE_DEEP", "false")
    );

    @Test
    void openAi() {
        runCatalog(
                "openai", env("PI_LIVE_OPENAI_MODEL", "gpt-4.1-mini"),
                requiredCredential("OPENAI_API_KEY"), true, false
        );
    }

    @Test
    void anthropic() {
        runCatalog(
                "anthropic",
                env("PI_LIVE_ANTHROPIC_MODEL", "claude-haiku-4-5"),
                requiredCredential("ANTHROPIC_API_KEY"), true, true
        );
    }

    @Test
    void googleAiStudio() {
        runCatalog(
                "google",
                env("PI_LIVE_GOOGLE_MODEL", "gemini-2.5-flash-lite"),
                requiredCredential("GOOGLE_API_KEY"), true, false
        );
    }

    @Test
    void googleVertex() {
        String token = requiredCredential("GOOGLE_VERTEX_ACCESS_TOKEN");
        assumeEnvironment("GOOGLE_CLOUD_PROJECT");
        assumeEnvironment("GOOGLE_CLOUD_LOCATION");
        runCatalog(
                "google-vertex",
                env("PI_LIVE_VERTEX_MODEL", "gemini-2.5-flash-lite"),
                token, true, false
        );
    }

    @Test
    void mistral() {
        runCatalog(
                "mistral",
                env("PI_LIVE_MISTRAL_MODEL", "ministral-3b-latest"),
                requiredCredential("MISTRAL_API_KEY"), true, false
        );
    }

    @Test
    void bedrock() {
        String bearer = System.getenv("AWS_BEARER_TOKEN_BEDROCK");
        boolean sigV4 = present(System.getenv("AWS_ACCESS_KEY_ID"))
                && present(System.getenv("AWS_SECRET_ACCESS_KEY"));
        assumeTrue(present(bearer) || sigV4,
                "AWS Bedrock bearer token or environment credentials required");
        Model model = catalogModel(
                "amazon-bedrock",
                env("PI_LIVE_BEDROCK_MODEL", "amazon.nova-micro-v1:0")
        );
        String region = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"));
        model = withBaseUrl(model,
                "https://bedrock-runtime." + region + ".amazonaws.com");
        runProvider(model, bearer, true, false);
    }

    @Test
    void localOpenAiCompatible() {
        String baseUrl = System.getenv("PI_LIVE_OPENAI_COMPAT_BASE_URL");
        String modelId = System.getenv("PI_LIVE_OPENAI_COMPAT_MODEL");
        assumeTrue(present(baseUrl) && present(modelId),
                "PI_LIVE_OPENAI_COMPAT_BASE_URL and PI_LIVE_OPENAI_COMPAT_MODEL required");
        Model model = new Model(
                modelId, modelId, "openai-completions", "live-local",
                baseUrl, false, List.of("text"), 32_768, 1_024
        );
        runProvider(
                model, env("PI_LIVE_OPENAI_COMPAT_API_KEY", "local-test"),
                false, false
        );
    }

    private static void runCatalog(
            String provider,
            String modelId,
            String credential,
            boolean verifyInvalidCredential,
            boolean verifyCache
    ) {
        runProvider(
                catalogModel(provider, modelId), credential,
                verifyInvalidCredential, verifyCache
        );
    }

    private static void runProvider(
            Model model,
            String credential,
            boolean verifyInvalidCredential,
            boolean verifyCache
    ) {
        try (VertxModelProviders providers = new VertxModelProviders()) {
            AssistantMessage text = runText(providers, model, credential);
            assertUsage(text, "text");
            if (!DEEP) return;

            AssistantMessage tool = runTool(providers, model, credential);
            assertUsage(tool, "tool");
            if (model.reasoning()) {
                AssistantMessage thinking = runThinking(providers, model, credential);
                assertUsage(thinking, "thinking");
            }
            runCancellation(providers, model, credential);
            if (verifyInvalidCredential) {
                runInvalidCredential(providers, model);
            }
            if (verifyCache) {
                runAnthropicCache(providers, model, credential);
            }
        }
    }

    private static AssistantMessage runText(
            VertxModelProviders providers, Model model, String credential
    ) {
        AssistantMessage done = requireDone(collect(
                providers, model,
                new ModelContext(
                        "Reply with exactly LIVE_OK.",
                        List.of(UserMessage.text(
                                "LIVE_OK", System.currentTimeMillis()
                        ))
                ),
                options(credential, "off", CancellationSignal.NONE)
        ), "text");
        String text = done.content().stream()
                .filter(TextContent.class::isInstance)
                .map(TextContent.class::cast)
                .map(TextContent::text)
                .reduce("", String::concat);
        assertFalse(text.isBlank(), "provider returned no text");
        return done;
    }

    private static AssistantMessage runTool(
            VertxModelProviders providers, Model model, String credential
    ) {
        ToolDefinition tool = new ToolDefinition(
                "live_probe", "Return the supplied probe value.",
                Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "value", Map.of("type", "string")
                        ),
                        "required", List.of("value"),
                        "additionalProperties", false
                )
        );
        AssistantMessage done = requireDone(collect(
                providers, model,
                new ModelContext(
                        "You must call live_probe exactly once. Do not answer in text.",
                        List.of(UserMessage.text(
                                "Call live_probe with value LIVE_OK.",
                                System.currentTimeMillis()
                        )),
                        List.of(tool)
                ),
                options(credential, "off", CancellationSignal.NONE)
        ), "tool");
        ToolCallContent call = done.content().stream()
                .filter(ToolCallContent.class::isInstance)
                .map(ToolCallContent.class::cast)
                .filter(candidate -> candidate.name().equals("live_probe"))
                .findFirst().orElseThrow(() -> new AssertionError(
                        "provider returned no live_probe tool call"
                ));
        assertEquals("LIVE_OK", call.arguments().get("value"));
        return done;
    }

    private static AssistantMessage runThinking(
            VertxModelProviders providers, Model model, String credential
    ) {
        AssistantMessage done = requireDone(collect(
                providers, model,
                new ModelContext(
                        "Reason briefly before answering with only 4.",
                        List.of(UserMessage.text(
                                "What is 2 + 2?", System.currentTimeMillis()
                        ))
                ),
                options(credential, "low", CancellationSignal.NONE)
        ), "thinking");
        boolean thinkingContent = done.content().stream()
                .anyMatch(ThinkingContent.class::isInstance);
        assertTrue(thinkingContent || done.usage().reasoning() > 0,
                "reasoning model returned no thinking content or reasoning usage");
        return done;
    }

    private static void runCancellation(
            VertxModelProviders providers, Model model, String credential
    ) {
        TestCancellationSignal cancellation = new TestCancellationSignal();
        CompletableFuture<List<AssistantStreamEvent>> result = Multi.createFrom()
                .publisher(providers.stream(
                        model,
                        new ModelContext(
                                "Write a detailed response with at least 1000 words.",
                                List.of(UserMessage.text(
                                        "Explain streaming cancellation.",
                                        System.currentTimeMillis()
                                ))
                        ),
                        options(credential, "off", cancellation)
                ))
                .onItem().invoke(event -> {
                    if (event instanceof AssistantStreamEvent.Start) {
                        cancellation.cancel();
                    }
                })
                .collect().asList().subscribeAsCompletionStage()
                .toCompletableFuture();
        try {
            List<AssistantStreamEvent> events = result.get(20, TimeUnit.SECONDS);
            assertTrue(events.stream().noneMatch(AssistantStreamEvent.Done.class::isInstance),
                    "cancelled stream published Done");
        } catch (ExecutionException failure) {
            Throwable cause = unwrap(failure);
            assertTrue(cause instanceof CancellationException,
                    "cancelled stream failed with " + cause);
        } catch (TimeoutException failure) {
            fail("cancelled stream did not settle within 20 seconds", failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            fail("interrupted while awaiting cancelled stream", failure);
        }
        assertTrue(cancellation.isCancelled(), "cancellation was not triggered");
    }

    private static void runInvalidCredential(
            VertxModelProviders providers, Model model
    ) {
        try {
            collect(
                    providers, model,
                    new ModelContext(
                            "Reply with AUTH_FAIL.",
                            List.of(UserMessage.text(
                                    "AUTH_FAIL", System.currentTimeMillis()
                            ))
                    ),
                    options("pi-agent-deliberately-invalid", "off",
                            CancellationSignal.NONE)
            );
            fail("invalid credential was accepted");
        } catch (RuntimeException failure) {
            Throwable cause = unwrap(failure);
            assertTrue(cause instanceof HttpResponseException,
                    "invalid credential failed without HTTP status: " + cause);
            int status = ((HttpResponseException) cause).status();
            assertTrue(status == 400 || status == 401 || status == 403,
                    "invalid credential returned unexpected HTTP " + status);
        }
    }

    private static void runAnthropicCache(
            VertxModelProviders providers, Model model, String credential
    ) {
        String prefix = "stable live cache prefix ".repeat(1_600);
        ModelContext context = new ModelContext(
                prefix + " Reply with exactly CACHE_OK.",
                List.of(UserMessage.text("CACHE_OK", 1L))
        );
        StreamOptions options = options(
                credential, "off", CancellationSignal.NONE
        ).withCacheRetention(CacheRetention.SHORT);
        AssistantMessage first = requireDone(
                collect(providers, model, context, options), "cache-write"
        );
        AssistantMessage second = requireDone(
                collect(providers, model, context, options), "cache-read"
        );
        assertTrue(first.usage().cacheWrite() > 0 || second.usage().cacheRead() > 0,
                "Anthropic returned no prompt-cache write or read usage");
    }

    private static List<AssistantStreamEvent> collect(
            VertxModelProviders providers,
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        return Multi.createFrom().publisher(providers.stream(model, context, options))
                .collect().asList().await().atMost(TIMEOUT);
    }

    private static AssistantMessage requireDone(
            List<AssistantStreamEvent> events, String scenario
    ) {
        assertFalse(events.isEmpty(), scenario + ": provider returned no events");
        assertTrue(events.stream().noneMatch(AssistantStreamEvent.Error.class::isInstance),
                scenario + ": provider returned a terminal error event");
        List<AssistantStreamEvent.Done> terminal = events.stream()
                .filter(AssistantStreamEvent.Done.class::isInstance)
                .map(AssistantStreamEvent.Done.class::cast)
                .toList();
        assertEquals(1, terminal.size(),
                scenario + ": provider must return exactly one Done event");
        AssistantMessage message = terminal.getFirst().message();
        assertNotNull(message, scenario + ": Done message is null");
        return message;
    }

    private static void assertUsage(AssistantMessage message, String scenario) {
        assertTrue(message.usage().input() > 0,
                scenario + ": provider returned no input usage");
        assertTrue(message.usage().output() > 0,
                scenario + ": provider returned no output usage");
        assertTrue(message.usage().totalTokens() > 0,
                scenario + ": provider returned no total usage");
    }

    private static StreamOptions options(
            String credential,
            String thinking,
            CancellationSignal cancellation
    ) {
        return new StreamOptions(
                "provider-live-test", credential, thinking,
                cancellation, Map.of()
        );
    }

    private static Model catalogModel(String provider, String modelId) {
        return ProviderModelCatalog.bundled().find(provider, modelId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Live-test model is not in catalog: "
                                + provider + '/' + modelId
                )).model();
    }

    private static String requiredCredential(String name) {
        String value = System.getenv(name);
        assumeTrue(present(value), name + " required");
        return value;
    }

    private static void assumeEnvironment(String name) {
        assumeTrue(present(System.getenv(name)), name + " required");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return present(value) ? value : fallback;
    }

    private static boolean present(String value) {
        return value != null && !value.isBlank();
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Model withBaseUrl(Model model, String baseUrl) {
        return new Model(
                model.id(), model.name(), model.api(), model.provider(),
                baseUrl, model.reasoning(), model.input(),
                model.contextWindow(), model.maxTokens(),
                model.thinkingLevelMap(), model.pricing()
        );
    }

    private static final class TestCancellationSignal
            implements CancellationSignal {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final CopyOnWriteArrayList<Runnable> callbacks =
                new CopyOnWriteArrayList<>();

        void cancel() {
            if (!cancelled.compareAndSet(false, true)) return;
            callbacks.forEach(Runnable::run);
            callbacks.clear();
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void throwIfCancelled() {
            if (cancelled.get()) throw new CancellationException("Operation cancelled");
        }

        @Override
        public AutoCloseable onCancel(Runnable callback) {
            if (cancelled.get()) {
                callback.run();
                return () -> { };
            }
            callbacks.add(callback);
            if (cancelled.get() && callbacks.remove(callback)) callback.run();
            return () -> callbacks.remove(callback);
        }
    }
}
