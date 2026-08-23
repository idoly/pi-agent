package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ThinkingLevelMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Immutable, versioned catalog of OpenAI-family models supported by this transport. */
public final class OpenAiModelCatalog {
    public static final String UPSTREAM_VERSION = "0.84.2";
    private static final String RESOURCE =
            "/io/github/idoly/pi/vertx/openai/model-catalog-0.84.2.json";
    private static final Set<String> SUPPORTED_APIS = Set.of(
            "openai-completions",
            "openai-responses",
            "azure-openai-responses",
            "openai-codex-responses"
    );

    private final List<Entry> entries;
    private final Map<Key, Entry> byKey;
    private final Map<ProviderModelKey, Entry> byProviderModel;

    private OpenAiModelCatalog(List<Entry> entries) {
        this.entries = List.copyOf(entries);
        LinkedHashMap<Key, Entry> exact = new LinkedHashMap<>();
        LinkedHashMap<ProviderModelKey, Entry> providerModels = new LinkedHashMap<>();
        for (Entry entry : entries) {
            Model model = entry.model();
            Key key = new Key(model.provider(), model.api(), model.id());
            if (exact.putIfAbsent(key, entry) != null) {
                throw new IllegalArgumentException("Duplicate model catalog key: " + key);
            }
            ProviderModelKey providerKey = new ProviderModelKey(model.provider(), model.id());
            if (providerModels.putIfAbsent(providerKey, entry) != null) {
                throw new IllegalArgumentException(
                        "Model appears under multiple APIs: " + model.provider() + "/" + model.id()
                );
            }
        }
        byKey = Collections.unmodifiableMap(exact);
        byProviderModel = Collections.unmodifiableMap(providerModels);
    }

    public static OpenAiModelCatalog bundled() {
        return BundledHolder.INSTANCE;
    }

    public static OpenAiModelCatalog load(InputStream input, ObjectMapper mapper) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(mapper, "mapper");
        JsonNode root;
        try (input) {
            root = mapper.readTree(input);
        } catch (IOException failure) {
            throw new IllegalArgumentException("Unable to read OpenAI model catalog", failure);
        }
        require(root.isObject(), "catalog root must be an object");
        require(root.path("schemaVersion").asInt(-1) == 1, "unsupported catalog schemaVersion");
        require(
                UPSTREAM_VERSION.equals(root.path("upstreamVersion").asText()),
                "catalog upstreamVersion must be " + UPSTREAM_VERSION
        );
        JsonNode values = root.path("entries");
        require(values.isArray() && !values.isEmpty(), "catalog entries must be a non-empty array");
        List<Entry> entries = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            entries.add(parseEntry(values.get(index), index));
        }
        return new OpenAiModelCatalog(entries);
    }

    public List<Entry> entries() {
        return entries;
    }

    public List<Model> models() {
        return entries.stream().map(Entry::model).toList();
    }

    public List<Model> models(String provider) {
        Objects.requireNonNull(provider, "provider");
        return entries.stream()
                .map(Entry::model)
                .filter(model -> model.provider().equals(provider))
                .toList();
    }

    public Optional<Entry> find(String provider, String id) {
        return Optional.ofNullable(byProviderModel.get(new ProviderModelKey(provider, id)));
    }

    public Optional<Entry> find(String provider, String api, String id) {
        return Optional.ofNullable(byKey.get(new Key(provider, api, id)));
    }

    public Optional<Entry> find(Model model) {
        Objects.requireNonNull(model, "model");
        return find(model.provider(), model.api(), model.id());
    }

    private static Entry parseEntry(JsonNode value, int index) {
        require(value.isObject(), "entry " + index + " must be an object");
        JsonNode node = value.path("model");
        require(node.isObject(), "entry " + index + " model must be an object");
        String label = "entry " + index + " model";
        String id = requiredText(node, "id", label, false);
        String name = requiredText(node, "name", label, false);
        String api = requiredText(node, "api", label, false);
        require(SUPPORTED_APIS.contains(api), label + " has unsupported api: " + api);
        String provider = requiredText(node, "provider", label, false);
        String baseUrl = requiredText(node, "baseUrl", label, api.equals("azure-openai-responses"));
        require(node.path("reasoning").isBoolean(), label + " reasoning must be boolean");
        JsonNode inputs = node.path("input");
        require(inputs.isArray() && !inputs.isEmpty(), label + " input must be a non-empty array");
        List<String> input = new ArrayList<>(inputs.size());
        for (JsonNode modality : inputs) {
            require(modality.isTextual(), label + " input modalities must be strings");
            require(
                    modality.asText().equals("text") || modality.asText().equals("image"),
                    label + " has unsupported input modality: " + modality.asText()
            );
            input.add(modality.asText());
        }
        int contextWindow = positiveInt(node, "contextWindow", label);
        int maxTokens = positiveInt(node, "maxTokens", label);
        ThinkingLevelMap thinkingLevelMap = parseThinkingLevelMap(node.path("thinkingLevelMap"), label);
        Model model = new Model(
                id, name, api, provider, baseUrl, node.path("reasoning").asBoolean(),
                input, contextWindow, maxTokens, thinkingLevelMap
        );

        JsonNode compat = value.path("compat");
        require(compat.isObject(), "entry " + index + " compat must be an object");
        Capabilities capabilities = new Capabilities(
                optionalBoolean(compat, "supportsStrictMode"),
                optionalBoolean(compat, "supportsOpenAIGrammarTools"),
                optionalBoolean(compat, "supportsAdditionalTools"),
                optionalBoolean(compat, "supportsToolSearch"),
                optionalBoolean(compat, "supportsExplicitPromptCacheMode")
        );
        OpenAiCompatibility completions = new OpenAiCompatibility(
                OpenAiCompatibility.MaxTokensField.MAX_COMPLETION_TOKENS,
                true, OpenAiCompatibility.ReasoningFormat.STANDARD, true, true,
                capabilities.supportsStrictMode() == null || capabilities.supportsStrictMode(),
                Boolean.TRUE.equals(capabilities.supportsGrammarTools())
        );
        boolean strict = switch (api) {
            case "openai-responses" -> Boolean.TRUE.equals(capabilities.supportsStrictMode());
            case "azure-openai-responses", "openai-codex-responses" ->
                    capabilities.supportsStrictMode() == null || capabilities.supportsStrictMode();
            default -> Boolean.TRUE.equals(capabilities.supportsStrictMode());
        };
        OpenAiResponsesCompatibility responses = new OpenAiResponsesCompatibility(
                true, "none", OpenAiResponsesCompatibility.SessionAffinityFormat.AUTO,
                true, strict, Boolean.TRUE.equals(capabilities.supportsGrammarTools())
        );
        return new Entry(model, completions, responses, capabilities);
    }

    private static ThinkingLevelMap parseThinkingLevelMap(JsonNode node, String label) {
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        require(node.isObject(), label + " thinkingLevelMap must be an object");
        LinkedHashMap<String, String> mappings = new LinkedHashMap<>();
        node.fields().forEachRemaining(field -> {
            JsonNode value = field.getValue();
            require(
                    value.isNull() || value.isTextual(),
                    label + " thinkingLevelMap values must be strings or null"
            );
            mappings.put(field.getKey(), value.isNull() ? null : value.asText());
        });
        return new ThinkingLevelMap(mappings);
    }

    private static Boolean optionalBoolean(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isMissingNode()) {
            return null;
        }
        require(value.isBoolean(), "compat." + field + " must be boolean");
        return value.asBoolean();
    }

    private static String requiredText(
            JsonNode node,
            String field,
            String label,
            boolean allowEmpty
    ) {
        JsonNode value = node.path(field);
        require(value.isTextual(), label + " " + field + " must be a string");
        require(allowEmpty || !value.asText().isBlank(), label + " " + field + " must not be blank");
        return value.asText();
    }

    private static int positiveInt(JsonNode node, String field, String label) {
        JsonNode value = node.path(field);
        require(value.canConvertToInt() && value.asInt() > 0, label + " " + field + " must be a positive int");
        return value.asInt();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid OpenAI model catalog: " + message);
        }
    }

    public record Entry(
            Model model,
            OpenAiCompatibility completionsCompatibility,
            OpenAiResponsesCompatibility responsesCompatibility,
            Capabilities capabilities
    ) {
        public Entry {
            Objects.requireNonNull(model, "model");
            Objects.requireNonNull(completionsCompatibility, "completionsCompatibility");
            Objects.requireNonNull(responsesCompatibility, "responsesCompatibility");
            Objects.requireNonNull(capabilities, "capabilities");
        }

        /** Returns a copy suitable for provider-specific or Azure deployment endpoints. */
        public Model withBaseUrl(String baseUrl) {
            Objects.requireNonNull(baseUrl, "baseUrl");
            if (baseUrl.isBlank()) {
                throw new IllegalArgumentException("baseUrl must not be blank");
            }
            return new Model(
                    model.id(), model.name(), model.api(), model.provider(), baseUrl,
                    model.reasoning(), model.input(), model.contextWindow(), model.maxTokens(),
                    model.thinkingLevelMap()
            );
        }
    }

    public record Capabilities(
            Boolean supportsStrictMode,
            Boolean supportsGrammarTools,
            Boolean supportsAdditionalTools,
            Boolean supportsToolSearch,
            Boolean supportsExplicitPromptCacheMode
    ) {
    }

    private record Key(String provider, String api, String id) {
        private Key {
            Objects.requireNonNull(provider, "provider");
            Objects.requireNonNull(api, "api");
            Objects.requireNonNull(id, "id");
        }
    }

    private record ProviderModelKey(String provider, String id) {
        private ProviderModelKey {
            Objects.requireNonNull(provider, "provider");
            Objects.requireNonNull(id, "id");
        }
    }

    private static final class BundledHolder {
        private static final OpenAiModelCatalog INSTANCE = loadBundled();

        private static OpenAiModelCatalog loadBundled() {
            InputStream input = OpenAiModelCatalog.class.getResourceAsStream(RESOURCE);
            if (input == null) {
                throw new IllegalStateException("Missing bundled OpenAI model catalog: " + RESOURCE);
            }
            try {
                return load(input, new ObjectMapper());
            } catch (IllegalArgumentException failure) {
                throw new IllegalStateException("Invalid bundled OpenAI model catalog", failure);
            }
        }
    }
}
