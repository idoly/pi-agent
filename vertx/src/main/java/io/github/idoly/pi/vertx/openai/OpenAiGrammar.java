package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.ConstrainedSampling;
import io.github.idoly.pi.ai.ToolDefinition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class OpenAiGrammar {
    private OpenAiGrammar() {
    }

    static Grammar resolve(
            ObjectMapper mapper,
            ToolDefinition tool,
            boolean supported
    ) {
        if (!(tool.constrainedSampling() instanceof ConstrainedSampling.Grammar config)
                || !supported) {
            return null;
        }
        String lark = nonBlank(config.openAiLark());
        String regex = nonBlank(config.openAiRegex());
        if (lark == null && regex == null) {
            throw failure(tool, "no supported grammar variant was provided");
        }
        JsonNode schema = mapper.valueToTree(tool.parameters());
        if (!schema.path("type").asText().equals("object")) {
            throw failure(tool, "grammar constrained sampling requires an object parameter schema");
        }
        JsonNode required = schema.path("required");
        if (!required.isArray() || required.size() != 1 || !required.get(0).isTextual()) {
            throw failure(
                    tool,
                    "grammar constrained sampling requires exactly one required string property"
            );
        }
        String property = required.get(0).asText();
        JsonNode propertySchema = schema.path("properties").path(property);
        if (propertySchema.isMissingNode()) {
            throw failure(
                    tool,
                    "grammar constrained sampling requires a properties entry for " + property
            );
        }
        if (!propertySchema.path("type").asText().equals("string")) {
            throw failure(
                    tool,
                    "grammar constrained sampling property " + property + " must have type string"
            );
        }
        return new Grammar(
                lark == null ? "regex" : "lark",
                lark == null ? regex : lark,
                property
        );
    }

    static Map<String, Grammar> resolveAll(
            ObjectMapper mapper,
            List<ToolDefinition> tools,
            boolean supported
    ) {
        Map<String, Grammar> output = new LinkedHashMap<>();
        for (ToolDefinition tool : tools) {
            Grammar grammar = resolve(mapper, tool, supported);
            if (grammar != null) {
                output.put(tool.name(), grammar);
            }
        }
        return Map.copyOf(output);
    }

    static String input(ToolDefinition tool, Map<String, Object> arguments, String property) {
        Object input = arguments.get(property);
        if (!(input instanceof String value)) {
            throw new IllegalArgumentException(
                    "Grammar tool call \"" + tool.name() + "\" requires argument \""
                            + property + "\" to be a string."
            );
        }
        return value;
    }

    static String input(String toolName, Map<String, Object> arguments, String property) {
        Object input = arguments.get(property);
        if (!(input instanceof String value)) {
            throw new IllegalArgumentException(
                    "Grammar tool call \"" + toolName + "\" requires argument \""
                            + property + "\" to be a string."
            );
        }
        return value;
    }

    private static String nonBlank(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static IllegalArgumentException failure(ToolDefinition tool, String message) {
        return new IllegalArgumentException(
                "Tool \"" + tool.name()
                        + "\" cannot use grammar constrained sampling: " + message + "."
        );
    }

    record Grammar(String syntax, String definition, String inputProperty) {
    }
}
