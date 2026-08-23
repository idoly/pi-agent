package io.github.idoly.pi.agent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Common headless environment-variable API-key resolution. */
public final class ProviderApiKeyResolvers {
    private static final Map<String, List<String>> VARIABLES = Map.ofEntries(
            Map.entry("openai", List.of("OPENAI_API_KEY")),
            Map.entry("openai-codex", List.of("OPENAI_API_KEY")),
            Map.entry("azure-openai-responses", List.of("AZURE_OPENAI_API_KEY")),
            Map.entry("anthropic", List.of(
                    "ANTHROPIC_API_KEY", "ANTHROPIC_OAUTH_TOKEN"
            )),
            Map.entry("google", List.of("GEMINI_API_KEY", "GOOGLE_API_KEY")),
            Map.entry("google-vertex", List.of("GOOGLE_CLOUD_API_KEY")),
            Map.entry("mistral", List.of("MISTRAL_API_KEY")),
            Map.entry("amazon-bedrock", List.of("AWS_BEARER_TOKEN_BEDROCK")),
            Map.entry("openrouter", List.of("OPENROUTER_API_KEY")),
            Map.entry("github-copilot", List.of("GITHUB_TOKEN"))
    );

    private ProviderApiKeyResolvers() {
    }

    public static ApiKeyResolver environment() {
        return provider -> CompletableFuture.completedFuture(resolve(provider));
    }

    public static String resolve(String provider) {
        for (String variable : VARIABLES.getOrDefault(provider, List.of(
                provider.toUpperCase(java.util.Locale.ROOT)
                        .replace('-', '_') + "_API_KEY"
        ))) {
            String value = System.getenv(variable);
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }
}
