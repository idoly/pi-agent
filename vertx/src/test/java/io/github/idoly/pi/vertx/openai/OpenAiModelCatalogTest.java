package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiModelCatalogTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void loadsPublishedUpstreamCatalogAndCompatibilityMetadata() {
        OpenAiModelCatalog catalog = OpenAiModelCatalog.bundled();

        assertEquals(83, catalog.entries().size());
        assertEquals(38, catalog.models("openai").size());
        assertEquals(7, catalog.models("openai-codex").size());
        assertEquals(38, catalog.models("azure-openai-responses").size());

        OpenAiModelCatalog.Entry gpt4 = catalog.find("openai", "gpt-4").orElseThrow();
        assertEquals("openai-responses", gpt4.model().api());
        assertEquals(8192, gpt4.model().contextWindow());
        assertTrue(gpt4.responsesCompatibility().supportsStrictMode());
        assertFalse(gpt4.responsesCompatibility().supportsGrammarTools());

        OpenAiModelCatalog.Entry gpt56 = catalog.find("openai", "gpt-5.6-luna")
                .orElseThrow();
        assertEquals("max", gpt56.model().thinkingLevelMap().providerValue("max"));
        assertTrue(gpt56.responsesCompatibility().supportsGrammarTools());
        assertEquals(Boolean.TRUE, gpt56.capabilities().supportsAdditionalTools());
        assertEquals(Boolean.TRUE, gpt56.capabilities().supportsToolSearch());
        assertEquals(Boolean.TRUE, gpt56.capabilities().supportsExplicitPromptCacheMode());

        OpenAiModelCatalog.Entry codex = catalog.find("openai-codex", "gpt-5.4")
                .orElseThrow();
        assertEquals("low", codex.model().thinkingLevelMap().providerValue("minimal"));
        assertTrue(codex.responsesCompatibility().supportsStrictMode());
        assertTrue(codex.responsesCompatibility().supportsGrammarTools());

        OpenAiModelCatalog.Entry azure = catalog.find(
                "azure-openai-responses", "gpt-5.2"
        ).orElseThrow();
        assertEquals("", azure.model().baseUrl());
        Model configuredAzure = azure.withBaseUrl(
                "https://example.openai.azure.com/openai/v1"
        );
        assertEquals(
                "https://example.openai.azure.com/openai/v1",
                configuredAzure.baseUrl()
        );
        assertEquals(
                azure.responsesCompatibility(),
                catalog.find(configuredAzure).orElseThrow().responsesCompatibility()
        );
        assertEquals("xhigh", azure.model().thinkingLevelMap().providerValue("xhigh"));
        assertTrue(azure.responsesCompatibility().supportsStrictMode());
        assertTrue(azure.responsesCompatibility().supportsGrammarTools());
    }

    @Test
    void resolvesPerModelProfilesAndPreservesExplicitOverrides() {
        OpenAiModelCatalog catalog = OpenAiModelCatalog.bundled();
        try (VertxSseHttpClient transport = new VertxSseHttpClient()) {
            OpenAiModelStream stream = new OpenAiModelStream(transport, mapper, catalog);
            Model gpt4 = catalog.find("openai", "gpt-4").orElseThrow().model();
            Model gpt5 = catalog.find("openai", "gpt-5").orElseThrow().model();
            assertFalse(stream.responsesCompatibility(gpt4).supportsGrammarTools());
            assertTrue(stream.responsesCompatibility(gpt5).supportsGrammarTools());

            Model custom = new Model(
                    "custom", "Custom", "openai-responses", "custom",
                    "https://example.test/v1", false, List.of("text"), 8192, 1024
            );
            assertSame(
                    OpenAiResponsesCompatibility.DEFAULT,
                    stream.responsesCompatibility(custom)
            );

            OpenAiModelStream overridden = new OpenAiModelStream(
                    transport, mapper, OpenAiCompatibility.LEGACY,
                    OpenAiResponsesCompatibility.GITHUB_COPILOT
            );
            assertSame(
                    OpenAiResponsesCompatibility.GITHUB_COPILOT,
                    overridden.responsesCompatibility(gpt5)
            );
            assertSame(OpenAiCompatibility.LEGACY, overridden.completionsCompatibility(gpt5));
        }
    }

    @Test
    void rejectsMalformedAndDuplicateCatalogEntries() {
        assertInvalid("""
                {"schemaVersion":2,"upstreamVersion":"0.84.2","entries":[]}
                """);
        assertInvalid("""
                {"schemaVersion":1,"upstreamVersion":"0.84.1","entries":[]}
                """);
        assertInvalid(catalogJson("unsupported-api", "model", "provider", "https://example.test"));
        assertInvalid(catalogJson("openai-responses", "model", "provider", ""));

        String entry = entryJson("openai-responses", "model", "provider", "https://example.test");
        assertInvalid("""
                {"schemaVersion":1,"upstreamVersion":"0.84.2","entries":[%s,%s]}
                """.formatted(entry, entry));
    }

    private void assertInvalid(String json) {
        assertThrows(IllegalArgumentException.class, () -> OpenAiModelCatalog.load(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), mapper
        ));
    }

    private static String catalogJson(String api, String id, String provider, String baseUrl) {
        return """
                {"schemaVersion":1,"upstreamVersion":"0.84.2","entries":[%s]}
                """.formatted(entryJson(api, id, provider, baseUrl));
    }

    private static String entryJson(String api, String id, String provider, String baseUrl) {
        return """
                {"model":{"id":"%s","name":"Model","api":"%s","provider":"%s",\
                "baseUrl":"%s","reasoning":false,"input":["text"],\
                "contextWindow":8192,"maxTokens":1024},"compat":{}}
                """.formatted(id, api, provider, baseUrl).replace("\\\n", "");
    }
}
