package io.github.idoly.pi.ai;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Provider wire values for pi thinking levels; a present null value marks a level unsupported. */
public record ThinkingLevelMap(Map<String, String> mappings) {
    private static final Set<String> LEVELS = Set.of(
            "off", "minimal", "low", "medium", "high", "xhigh", "max"
    );

    public ThinkingLevelMap {
        Objects.requireNonNull(mappings, "mappings");
        LinkedHashMap<String, String> copy = new LinkedHashMap<>();
        mappings.forEach((level, value) -> {
            if (!LEVELS.contains(level)) {
                throw new IllegalArgumentException("Unknown thinking level: " + level);
            }
            copy.put(level, value);
        });
        mappings = Collections.unmodifiableMap(copy);
    }

    public boolean defines(String level) {
        return mappings.containsKey(level);
    }

    public String providerValue(String level) {
        return mappings.get(level);
    }
}
