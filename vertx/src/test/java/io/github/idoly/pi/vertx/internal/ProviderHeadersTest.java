package io.github.idoly.pi.vertx.internal;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProviderHeadersTest {
    @Test
    void laterLayersReplaceHeadersCaseInsensitively() {
        LinkedHashMap<String, String> defaults = new LinkedHashMap<>();
        defaults.put("content-type", "application/json");
        defaults.put("authorization", "Bearer generated");
        LinkedHashMap<String, String> request = new LinkedHashMap<>();
        request.put("Content-Type", "application/custom");
        request.put("Authorization", "Proxy request");
        Map<String, String> hook = Map.of(
                "AUTHORIZATION", "Proxy hook"
        );

        Map<String, String> merged = ProviderHeaders.merge(
                defaults, request, hook
        );

        assertEquals(2, merged.size());
        assertEquals("application/custom", merged.get("Content-Type"));
        assertEquals("Proxy hook", merged.get("AUTHORIZATION"));
    }
}
