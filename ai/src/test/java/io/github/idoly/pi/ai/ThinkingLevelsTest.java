package io.github.idoly.pi.ai;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ThinkingLevelsTest {
    @Test
    void exposesMappedLevelsAndClampsUpwardBeforeDownward() {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        values.put("off", null);
        values.put("minimal", null);
        values.put("low", "low");
        values.put("medium", null);
        values.put("high", "high");
        values.put("xhigh", null);
        values.put("max", "max");
        Model model = model(true, new ThinkingLevelMap(values));

        assertEquals(List.of("low", "high", "max"), ThinkingLevels.supported(model));
        assertEquals("low", ThinkingLevels.clamp(model, "off"));
        assertEquals("high", ThinkingLevels.clamp(model, "medium"));
        assertEquals("max", ThinkingLevels.clamp(model, "xhigh"));
        assertEquals("max", ThinkingLevels.providerValue(model, "max"));
    }

    @Test
    void defaultsToStandardLevelsAndOnlyExplicitlyEnablesExtendedLevels() {
        assertEquals(
                List.of("off", "minimal", "low", "medium", "high"),
                ThinkingLevels.supported(model(true, null))
        );
        assertEquals(List.of("off"), ThinkingLevels.supported(model(false, null)));
    }

    private static Model model(boolean reasoning, ThinkingLevelMap mapping) {
        return new Model(
                "model", "Model", "openai-responses", "openai",
                "https://example.invalid/v1", reasoning, List.of("text"),
                128_000, 1_024, mapping
        );
    }
}
