package io.github.idoly.pi.agent.provider;

import io.github.idoly.pi.ai.ProviderDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProviderConfigurationTest {
    @Test
    void parsesPiStyleCustomProviderDefaultsAndPricing() {
        ProviderConfiguration configuration = ProviderConfiguration.parse("""
                {
                  "providers": {
                    "ollama": {
                      "name": "Local Ollama",
                      "baseUrl": "http://localhost:11434/v1",
                      "api": "openai-completions",
                      "apiKey": "ollama",
                      "headers": {"x-tenant": "$TENANT"},
                      "models": [
                        {"id":"qwen","reasoning":true,"cost":{"input":1,"output":2}}
                      ]
                    }
                  }
                }
                """);
        ProviderDefinition provider = configuration.provider("ollama");
        assertEquals("Local Ollama", provider.name());
        assertEquals(1, provider.models().size());
        assertEquals("qwen", provider.models().getFirst().name());
        assertEquals(128_000, provider.models().getFirst().contextWindow());
        assertEquals(2, provider.models().getFirst().pricing().output());
        assertEquals("$TENANT", provider.headers().get("x-tenant"));
    }

    @Test
    void rejectsUnknownApiAndCommandSecretsByDefault() {
        assertThrows(IllegalArgumentException.class, () ->
                ProviderConfiguration.parse("""
                        {"providers":{"x":{"baseUrl":"http://localhost","api":"unknown","models":[{"id":"x"}]}}}
                        """));
        assertThrows(IllegalArgumentException.class, () ->
                ProviderConfiguration.resolveEnvironment("!secret-command"));
        assertEquals("literal$value!", ProviderConfiguration.resolveEnvironment(
                "literal$$value$!"
        ));
    }
}
