package io.github.idoly.pi.vertx;

import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.UserMessage;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Credential-gated smoke tests executed only by -Pprovider-live-tests. */
class ProviderLiveIT {
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @Test
    void openAi() {
        runCatalog(
                "openai", env("PI_LIVE_OPENAI_MODEL", "gpt-4.1-mini"),
                requiredCredential("OPENAI_API_KEY")
        );
    }

    @Test
    void anthropic() {
        runCatalog(
                "anthropic",
                env("PI_LIVE_ANTHROPIC_MODEL", "claude-haiku-4-5"),
                requiredCredential("ANTHROPIC_API_KEY")
        );
    }

    @Test
    void googleAiStudio() {
        runCatalog(
                "google",
                env("PI_LIVE_GOOGLE_MODEL", "gemini-2.5-flash-lite"),
                requiredCredential("GOOGLE_API_KEY")
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
                token
        );
    }

    @Test
    void mistral() {
        runCatalog(
                "mistral",
                env("PI_LIVE_MISTRAL_MODEL", "ministral-3b-latest"),
                requiredCredential("MISTRAL_API_KEY")
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
        run(model, bearer);
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
        run(model, env("PI_LIVE_OPENAI_COMPAT_API_KEY", "local-test"));
    }

    private static void runCatalog(
            String provider, String modelId, String credential
    ) {
        run(catalogModel(provider, modelId), credential);
    }

    private static Model catalogModel(String provider, String modelId) {
        return ProviderModelCatalog.bundled().find(provider, modelId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Live-test model is not in catalog: "
                                + provider + '/' + modelId
                )).model();
    }

    private static void run(Model model, String credential) {
        try (VertxModelProviders providers = new VertxModelProviders()) {
            List<AssistantStreamEvent> events = Multi.createFrom()
                    .publisher(providers.stream(
                            model,
                            new ModelContext(
                                    "Reply with exactly LIVE_OK.",
                                    List.of(UserMessage.text(
                                            "LIVE_OK", System.currentTimeMillis()
                                    ))
                            ),
                            new StreamOptions(
                                    "provider-live-test", credential, "off",
                                    CancellationSignal.NONE, Map.of()
                            )
                    ))
                    .collect().asList().await().atMost(TIMEOUT);
            assertFalse(events.isEmpty(), "provider returned no events");
            assertTrue(events.stream().noneMatch(AssistantStreamEvent.Error.class::isInstance),
                    "provider returned a terminal error event");
            AssistantStreamEvent.Done done = events.stream()
                    .filter(AssistantStreamEvent.Done.class::isInstance)
                    .map(AssistantStreamEvent.Done.class::cast)
                    .reduce((first, second) -> second).orElse(null);
            assertNotNull(done, "provider returned no Done event");
            String text = done.message().content().stream()
                    .filter(TextContent.class::isInstance)
                    .map(TextContent.class::cast)
                    .map(TextContent::text)
                    .reduce("", String::concat);
            assertFalse(text.isBlank(), "provider returned no text");
        }
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

    private static Model withBaseUrl(Model model, String baseUrl) {
        return new Model(
                model.id(), model.name(), model.api(), model.provider(),
                baseUrl, model.reasoning(), model.input(),
                model.contextWindow(), model.maxTokens(),
                model.thinkingLevelMap(), model.pricing()
        );
    }
}
