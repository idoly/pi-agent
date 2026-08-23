package io.github.idoly.pi.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Validates tool arguments against the JSON Schema exposed to the model. */
public final class ToolArgumentValidator {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonSchemaFactory FACTORY =
            JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    private static final ConcurrentHashMap<JsonNode, JsonSchema> CACHE = new ConcurrentHashMap<>();

    private ToolArgumentValidator() {
    }

    public static void validate(Map<String, Object> schemaValue, Map<String, Object> arguments) {
        if (schemaValue.isEmpty()) {
            return;
        }
        JsonNode schemaNode = MAPPER.valueToTree(schemaValue);
        JsonSchema schema = CACHE.computeIfAbsent(schemaNode, FACTORY::getSchema);
        JsonNode input = MAPPER.valueToTree(arguments);
        Set<ValidationMessage> errors = schema.validate(input);
        if (errors.isEmpty()) {
            return;
        }
        String message = errors.stream()
                .map(ValidationMessage::getMessage)
                .sorted(Comparator.naturalOrder())
                .reduce((left, right) -> left + "; " + right)
                .orElse("Invalid tool arguments");
        throw new IllegalArgumentException(message);
    }
}
