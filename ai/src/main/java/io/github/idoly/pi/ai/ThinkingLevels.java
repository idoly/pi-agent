package io.github.idoly.pi.ai;

import java.util.ArrayList;
import java.util.List;

public final class ThinkingLevels {
    public static final List<String> ORDERED = List.of(
            "off", "minimal", "low", "medium", "high", "xhigh", "max"
    );

    private ThinkingLevels() {
    }

    public static List<String> supported(Model model) {
        if (!model.reasoning()) {
            return List.of("off");
        }
        ThinkingLevelMap mapping = model.thinkingLevelMap();
        List<String> supported = new ArrayList<>();
        for (String level : ORDERED) {
            boolean explicitlyUnsupported = mapping != null
                    && mapping.defines(level)
                    && mapping.providerValue(level) == null;
            boolean extendedWithoutMapping = (level.equals("xhigh") || level.equals("max"))
                    && (mapping == null || !mapping.defines(level));
            if (!explicitlyUnsupported && !extendedWithoutMapping) {
                supported.add(level);
            }
        }
        return List.copyOf(supported);
    }

    public static String clamp(Model model, String requested) {
        List<String> supported = supported(model);
        if (supported.contains(requested)) {
            return requested;
        }
        int index = ORDERED.indexOf(requested);
        if (index < 0) {
            return supported.isEmpty() ? "off" : supported.getFirst();
        }
        for (int candidate = index; candidate < ORDERED.size(); candidate++) {
            if (supported.contains(ORDERED.get(candidate))) {
                return ORDERED.get(candidate);
            }
        }
        for (int candidate = index - 1; candidate >= 0; candidate--) {
            if (supported.contains(ORDERED.get(candidate))) {
                return ORDERED.get(candidate);
            }
        }
        return supported.isEmpty() ? "off" : supported.getFirst();
    }

    public static String providerValue(Model model, String level) {
        ThinkingLevelMap mapping = model.thinkingLevelMap();
        if (mapping != null && mapping.defines(level)) {
            return mapping.providerValue(level);
        }
        return level;
    }
}
