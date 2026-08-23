package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayDeque;

final class SessionJson {
    private SessionJson() {
    }

    static JsonNode copy(JsonNode value, String label) {
        if (value == null) return null;
        validate(value, label);
        return value.deepCopy();
    }

    static void validate(JsonNode value, String label) {
        ArrayDeque<JsonNode> pending = new ArrayDeque<>();
        pending.add(value);
        while (!pending.isEmpty()) {
            JsonNode node = pending.removeLast();
            if (node.isObject()) {
                node.elements().forEachRemaining(pending::addLast);
            } else if (node.isArray()) {
                node.elements().forEachRemaining(pending::addLast);
            } else if (node.isNull() || node.isTextual() || node.isBoolean()
                    || node.isIntegralNumber()) {
                // Supported JSON scalar.
            } else if (node.isFloatingPointNumber()) {
                if (!Double.isFinite(node.doubleValue())) {
                    throw invalid(label + " contains a non-finite number");
                }
            } else {
                throw invalid(label + " contains a non-JSON node: " + node.getNodeType());
            }
        }
    }

    private static SessionError invalid(String message) {
        return new SessionError(SessionError.Code.INVALID_PAYLOAD, message);
    }
}
