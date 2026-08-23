package io.github.idoly.pi.vertx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelPricing;
import io.github.idoly.pi.ai.ThinkingLevelMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Versioned catalog for Anthropic, Google, Vertex, Bedrock, and Mistral. */
public final class ProviderModelCatalog {
    public static final String UPSTREAM_VERSION = "0.84.2";
    private static final String RESOURCE =
            "/io/github/idoly/pi/vertx/provider-model-catalog-0.84.2.json";

    private final List<Entry> entries;
    private final Map<Key, Entry> byKey;

    private ProviderModelCatalog(List<Entry> entries) {
        this.entries = List.copyOf(entries);
        LinkedHashMap<Key, Entry> indexed = new LinkedHashMap<>();
        for (Entry entry : entries) {
            Key key = new Key(entry.model().provider(), entry.model().id());
            if (indexed.putIfAbsent(key, entry) != null) {
                throw new IllegalArgumentException(
                        "Duplicate provider model " + key
                );
            }
        }
        byKey = Map.copyOf(indexed);
    }

    public static ProviderModelCatalog bundled() {
        return Holder.INSTANCE;
    }

    public static ProviderModelCatalog load(
            InputStream input,
            ObjectMapper mapper
    ) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(mapper, "mapper");
        JsonNode root;
        try (input) {
            root = mapper.readTree(input);
        } catch (IOException failure) {
            throw new IllegalArgumentException(
                    "Unable to read provider model catalog", failure
            );
        }
        require(root.path("schemaVersion").asInt() == 1,
                "unsupported catalog schemaVersion");
        require(UPSTREAM_VERSION.equals(root.path("upstreamVersion").asText()),
                "catalog upstreamVersion must be " + UPSTREAM_VERSION);
        ArrayList<Entry> entries = new ArrayList<>();
        for (JsonNode entry : root.path("entries")) {
            JsonNode node = entry.path("model");
            ThinkingLevelMap thinking = thinking(node.path("thinkingLevelMap"));
            Model model = new Model(
                    required(node, "id"), required(node, "name"),
                    required(node, "api"), required(node, "provider"),
                    text(node, "baseUrl"), node.path("reasoning").asBoolean(),
                    strings(node.path("input")),
                    node.path("contextWindow").asInt(),
                    node.path("maxTokens").asInt(), thinking,
                    pricing(entry.path("cost"))
            );
            entries.add(new Entry(
                    model, jsonMap(mapper, entry.path("compat"))
            ));
        }
        return new ProviderModelCatalog(entries);
    }

    public List<Entry> entries() {
        return entries;
    }

    public List<Model> models() {
        return entries.stream().map(Entry::model).toList();
    }

    public List<Model> models(String provider) {
        return entries.stream().map(Entry::model)
                .filter(model -> model.provider().equals(provider)).toList();
    }

    public Optional<Entry> find(String provider, String id) {
        return Optional.ofNullable(byKey.get(new Key(provider, id)));
    }

    private static ModelPricing pricing(JsonNode node) {
        ArrayList<ModelPricing.Tier> tiers = new ArrayList<>();
        for (JsonNode tier : node.path("tiers")) {
            tiers.add(new ModelPricing.Tier(
                    tier.path("inputTokensAbove").asLong(),
                    tier.path("input").asDouble(),
                    tier.path("output").asDouble(),
                    tier.path("cacheRead").asDouble(),
                    tier.path("cacheWrite").asDouble()
            ));
        }
        return new ModelPricing(
                node.path("input").asDouble(),
                node.path("output").asDouble(),
                node.path("cacheRead").asDouble(),
                node.path("cacheWrite").asDouble(), tiers
        );
    }

    private static ThinkingLevelMap thinking(JsonNode node) {
        if (!node.isObject()) return null;
        LinkedHashMap<String, String> mappings = new LinkedHashMap<>();
        for (String level : List.of(
                "off", "minimal", "low", "medium", "high", "xhigh", "max"
        )) {
            if (!node.has(level)) continue;
            mappings.put(level, node.path(level).isNull()
                    ? null : node.path(level).asText());
        }
        return new ThinkingLevelMap(mappings);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException(
                    "model " + field + " must be a string"
            );
        }
        return value.asText();
    }

    private static String required(JsonNode node, String field) {
        String value = node.path(field).asText();
        require(!value.isBlank(), "model " + field + " must not be blank");
        return value;
    }

    private static List<String> strings(JsonNode values) {
        ArrayList<String> result = new ArrayList<>();
        values.forEach(value -> result.add(value.asText()));
        return List.copyOf(result);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> jsonMap(
            ObjectMapper mapper,
            JsonNode node
    ) {
        return node.isObject()
                ? Map.copyOf(mapper.convertValue(node, LinkedHashMap.class))
                : Map.of();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    public record Entry(Model model, Map<String, Object> compatibility) {
        public Entry {
            Objects.requireNonNull(model, "model");
            compatibility = Map.copyOf(compatibility);
        }
    }

    private record Key(String provider, String id) {
    }

    private static final class Holder {
        private static final ProviderModelCatalog INSTANCE;

        static {
            InputStream input = ProviderModelCatalog.class
                    .getResourceAsStream(RESOURCE);
            if (input == null) throw new IllegalStateException(
                    "Missing bundled provider model catalog " + RESOURCE
            );
            INSTANCE = load(input, new ObjectMapper());
        }
    }
}
