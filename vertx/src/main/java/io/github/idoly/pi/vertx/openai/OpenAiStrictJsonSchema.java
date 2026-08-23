package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.ConstrainedSampling;
import io.github.idoly.pi.ai.ToolDefinition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class OpenAiStrictJsonSchema {
    private static final List<String> UNSUPPORTED_KEYS = List.of(
            "$ref", "$defs", "definitions", "allOf", "oneOf", "patternProperties",
            "dependentSchemas", "dependencies", "unevaluatedProperties",
            "propertyNames", "contains", "prefixItems", "not", "if", "then", "else"
    );

    private OpenAiStrictJsonSchema() {
    }

    static Resolution resolve(
            ObjectMapper mapper,
            ToolDefinition tool,
            boolean supportsStrictMode
    ) {
        if (!(tool.constrainedSampling() instanceof ConstrainedSampling.JsonSchema config)) {
            return new Resolution(mapper.valueToTree(tool.parameters()), false, supportsStrictMode);
        }
        if (!supportsStrictMode) {
            if (config.strictness() == ConstrainedSampling.Strictness.REQUIRE) {
                throw new IllegalArgumentException(
                        "Tool \"" + tool.name()
                                + "\" requires JSON-schema constrained sampling, "
                                + "but strict tools are unsupported."
                );
            }
            return new Resolution(mapper.valueToTree(tool.parameters()), false, false);
        }
        ObjectNode strict;
        try {
            JsonNode cloned = mapper.valueToTree(tool.parameters());
            if (!(cloned instanceof ObjectNode root)) {
                throw new UnsupportedSchema("root schema must have type object");
            }
            makeStrict(root);
            if (!root.path("type").asText().equals("object")) {
                throw new UnsupportedSchema("root schema must have type object");
            }
            strict = root;
        } catch (UnsupportedSchema unsupported) {
            if (config.strictness() == ConstrainedSampling.Strictness.REQUIRE) {
                throw new IllegalArgumentException(
                        "Tool \"" + tool.name()
                                + "\" requires JSON-schema constrained sampling, but "
                                + unsupported.getMessage() + ".",
                        unsupported
                );
            }
            return new Resolution(mapper.valueToTree(tool.parameters()), false, true);
        }
        return new Resolution(strict, true, true);
    }

    private static void makeStrict(ObjectNode schema) {
        for (String key : UNSUPPORTED_KEYS) {
            if (schema.has(key)) {
                throw new UnsupportedSchema(key + " schemas are unsupported");
            }
        }
        if (schema.has("anyOf")) {
            JsonNode anyOf = schema.path("anyOf");
            if (!anyOf.isArray() || anyOf.isEmpty()) {
                throw new UnsupportedSchema("anyOf must contain at least one schema");
            }
            for (JsonNode variant : anyOf) {
                if (!(variant instanceof ObjectNode object)) {
                    throw new UnsupportedSchema("boolean schemas are unsupported");
                }
                if (isStructured(object)) {
                    throw new UnsupportedSchema("object and array unions are unsupported");
                }
                makeStrict(object);
            }
        }
        if (schema.has("items")) {
            JsonNode items = schema.path("items");
            if (!(items instanceof ObjectNode object)) {
                throw new UnsupportedSchema(
                        items.isArray() ? "tuple schemas are unsupported" : "boolean schemas are unsupported"
                );
            }
            makeStrict(object);
        }
        if (schema.has("properties") && !schema.path("type").asText().equals("object")) {
            throw new UnsupportedSchema("properties require type object");
        }
        if (!schema.path("type").asText().equals("object")) {
            return;
        }
        if (schema.has("additionalProperties")
                && !schema.path("additionalProperties").isBoolean()
                || schema.has("additionalProperties")
                && schema.path("additionalProperties").asBoolean()) {
            throw new UnsupportedSchema(
                    "schema-valued or true additionalProperties is unsupported"
            );
        }
        JsonNode propertiesNode = schema.path("properties");
        if (!propertiesNode.isMissingNode() && !(propertiesNode instanceof ObjectNode)) {
            throw new UnsupportedSchema("object properties must be a schema map");
        }
        Set<String> required = new HashSet<>();
        if (schema.has("required")) {
            JsonNode requiredNode = schema.path("required");
            if (!requiredNode.isArray()) {
                throw new UnsupportedSchema("object required must be a string array");
            }
            for (JsonNode value : requiredNode) {
                if (!value.isTextual()) {
                    throw new UnsupportedSchema("object required must be a string array");
                }
                required.add(value.asText());
            }
        }
        ObjectNode properties = propertiesNode instanceof ObjectNode object
                ? object
                : schema.putObject("properties");
        Set<String> propertyNames = new HashSet<>();
        properties.fieldNames().forEachRemaining(propertyNames::add);
        if (!propertyNames.containsAll(required)) {
            throw new UnsupportedSchema("required contains an unknown property");
        }
        properties.properties().forEach(entry -> {
            if (!(entry.getValue() instanceof ObjectNode property)) {
                throw new UnsupportedSchema("boolean schemas are unsupported");
            }
            makeStrict(property);
            if (!required.contains(entry.getKey()) && !allowsNull(property)) {
                ObjectNode nullable = property.objectNode();
                ArrayNode variants = nullable.putArray("anyOf");
                variants.add(property.deepCopy());
                variants.addObject().put("type", "null");
                entry.setValue(nullable);
            }
        });
        ArrayNode allRequired = schema.putArray("required");
        properties.fieldNames().forEachRemaining(allRequired::add);
        schema.put("additionalProperties", false);
    }

    private static boolean isStructured(ObjectNode schema) {
        JsonNode type = schema.path("type");
        if (type.isTextual()
                && (type.asText().equals("object") || type.asText().equals("array"))) {
            return true;
        }
        if (type.isArray()) {
            for (JsonNode value : type) {
                if (value.asText().equals("object") || value.asText().equals("array")) {
                    return true;
                }
            }
        }
        return schema.has("properties") || schema.has("items");
    }

    private static boolean allowsNull(ObjectNode schema) {
        JsonNode type = schema.path("type");
        if (type.asText().equals("null") || schema.path("const").isNull()) {
            return true;
        }
        if (type.isArray()) {
            for (JsonNode value : type) {
                if (value.asText().equals("null")) {
                    return true;
                }
            }
        }
        for (String field : List.of("enum", "anyOf")) {
            JsonNode values = schema.path(field);
            if (values.isArray()) {
                for (JsonNode value : values) {
                    if (value.isNull()
                            || value instanceof ObjectNode object && allowsNull(object)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    record Resolution(JsonNode parameters, boolean strict, boolean includeStrict) {
    }

    private static final class UnsupportedSchema extends IllegalArgumentException {
        private UnsupportedSchema(String message) {
            super(message);
        }
    }
}
