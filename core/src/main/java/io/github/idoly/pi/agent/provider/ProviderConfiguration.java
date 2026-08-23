package io.github.idoly.pi.agent.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.ConfigValueResolver;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ProviderDefinition;
import io.github.idoly.pi.ai.ModelPricing;
import io.github.idoly.pi.ai.ThinkingLevelMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Structured pi-style models.json loader for headless JVM hosts. */
public final class ProviderConfiguration {
    private static final Pattern ENV = Pattern.compile(
            "\\$\\{([A-Za-z_][A-Za-z0-9_]*)}|\\$([A-Za-z_][A-Za-z0-9_]*)"
    );
    private static final List<String> APIS = List.of(
            "anthropic-messages", "openai-completions", "openai-responses",
            "azure-openai-responses", "openai-codex-responses",
            "mistral-conversations", "google-generative-ai",
            "google-vertex", "bedrock-converse-stream"
    );

    private final List<ProviderDefinition> providers;

    private ProviderConfiguration(List<ProviderDefinition> providers) {
        this.providers = List.copyOf(providers);
    }

    public static ProviderConfiguration read(Path path) {
        Objects.requireNonNull(path, "path");
        try {
            return parse(Files.readString(path, StandardCharsets.UTF_8));
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Unable to read provider configuration " + path, failure
            );
        }
    }

    public static ProviderConfiguration parse(String json) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;
        try {
            root = mapper.readTree(json);
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Invalid provider configuration JSON", failure
            );
        }
        JsonNode values = root.path("providers");
        if (!values.isObject()) {
            throw new IllegalArgumentException("providers must be an object");
        }
        ArrayList<ProviderDefinition> providers = new ArrayList<>();
        values.fields().forEachRemaining(entry -> providers.add(parseProvider(
                mapper, entry.getKey(), entry.getValue()
        )));
        return new ProviderConfiguration(providers);
    }

    public List<ProviderDefinition> providers() {
        return providers;
    }

    public ProviderDefinition provider(String id) {
        return providers.stream().filter(value -> value.id().equals(id))
                .findFirst().orElseThrow(() ->
                        new IllegalArgumentException("Unknown provider " + id)
                );
    }

    public static ConfigValueResolver environmentResolver() {
        return expression -> java.util.concurrent.CompletableFuture
                .completedFuture(resolveEnvironment(expression));
    }

    public static String resolveEnvironment(String expression) {
        if (expression == null) return null;
        if (expression.startsWith("!")) {
            throw new IllegalArgumentException(
                    "Command secrets require an explicit ConfigValueResolver"
            );
        }
        String escaped = expression.replace("$$", "\u0000")
                .replace("$!", "\u0001");
        Matcher matcher = ENV.matcher(escaped);
        StringBuffer output = new StringBuffer();
        while (matcher.find()) {
            String name = matcher.group(1) == null
                    ? matcher.group(2) : matcher.group(1);
            String value = System.getenv(name);
            if (value == null) return null;
            matcher.appendReplacement(output, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(output);
        return output.toString().replace('\u0000', '$').replace('\u0001', '!');
    }

    private static ProviderDefinition parseProvider(
            ObjectMapper mapper,
            String id,
            JsonNode node
    ) {
        if (!node.isObject()) throw new IllegalArgumentException(
                "Provider " + id + " must be an object"
        );
        String baseUrl = optionalText(node, "baseUrl");
        String providerApi = optionalText(node, "api");
        JsonNode modelValues = node.path("models");
        ArrayList<Model> models = new ArrayList<>();
        if (modelValues.isArray()) {
            if (baseUrl == null || baseUrl.isBlank()) {
                throw new IllegalArgumentException(
                        "Provider " + id + " with models requires baseUrl"
                );
            }
            for (JsonNode model : modelValues) {
                String api = optionalText(model, "api");
                if (api == null) api = providerApi;
                if (api == null || !APIS.contains(api)) {
                    throw new IllegalArgumentException(
                            "Provider " + id + " has unsupported API " + api
                    );
                }
                String modelId = requiredText(model, "id");
                String modelBaseUrl = optionalText(model, "baseUrl");
                ThinkingLevelMap thinking = thinking(
                        model.path("thinkingLevelMap")
                );
                models.add(new Model(
                        modelId,
                        defaultText(model, "name", modelId), api, id,
                        modelBaseUrl == null ? baseUrl : modelBaseUrl,
                        model.path("reasoning").asBoolean(false),
                        strings(model.path("input"), List.of("text")),
                        positive(model, "contextWindow", 128_000),
                        positive(model, "maxTokens", 16_384),
                        thinking, pricing(model.path("cost"))
                ));
            }
        }
        return new ProviderDefinition(
                id, optionalText(node, "name"), baseUrl, providerApi,
                optionalText(node, "apiKey"), stringsMap(node.path("headers")),
                node.path("authHeader").asBoolean(false), models,
                objectMap(mapper, node.path("compat"))
        );
    }

    private static ModelPricing pricing(JsonNode node) {
        return new ModelPricing(
                node.path("input").asDouble(), node.path("output").asDouble(),
                node.path("cacheRead").asDouble(),
                node.path("cacheWrite").asDouble()
        );
    }

    private static ThinkingLevelMap thinking(JsonNode node) {
        if (!node.isObject()) return null;
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> result.put(
                entry.getKey(), entry.getValue().isNull()
                        ? null : entry.getValue().asText()
        ));
        return new ThinkingLevelMap(result);
    }

    private static List<String> strings(JsonNode node, List<String> defaults) {
        if (!node.isArray()) return defaults;
        ArrayList<String> values = new ArrayList<>();
        node.forEach(value -> values.add(value.asText()));
        return List.copyOf(values);
    }

    private static Map<String, String> stringsMap(JsonNode node) {
        if (!node.isObject()) return Map.of();
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> values.put(
                entry.getKey(), entry.getValue().asText()
        ));
        return Map.copyOf(values);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectMap(
            ObjectMapper mapper,
            JsonNode node
    ) {
        return node.isObject()
                ? Map.copyOf(mapper.convertValue(node, LinkedHashMap.class))
                : Map.of();
    }

    private static int positive(JsonNode node, String field, int fallback) {
        int value = node.path(field).asInt(fallback);
        if (value < 1) throw new IllegalArgumentException(
                field + " must be positive"
        );
        return value;
    }

    private static String requiredText(JsonNode node, String field) {
        String value = optionalText(node, field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static String defaultText(
            JsonNode node,
            String field,
            String fallback
    ) {
        String value = optionalText(node, field);
        return value == null ? fallback : value;
    }

    private static String optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }
}
