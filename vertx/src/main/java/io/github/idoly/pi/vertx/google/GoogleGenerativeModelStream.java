package io.github.idoly.pi.vertx.google;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.internal.ProviderHttpHooks;
import io.github.idoly.pi.vertx.SseHttpRequest;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.smallrye.mutiny.Multi;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

/** Google AI Studio and Vertex generateContent streaming over Vert.x SSE. */
public final class GoogleGenerativeModelStream
        implements ModelProvider, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final ObjectMapper mapper;
    private final GoogleGenerativeCodec codec;
    private final boolean ownsTransport;

    public GoogleGenerativeModelStream() {
        this(new VertxSseHttpClient(), new ObjectMapper(), true);
    }

    public GoogleGenerativeModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper
    ) {
        this(transport, mapper, false);
    }

    private GoogleGenerativeModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.codec = new GoogleGenerativeCodec(mapper);
        this.ownsTransport = ownsTransport;
    }

    @Override
    public String id() {
        return "google-generative";
    }

    @Override
    public boolean supports(Model model) {
        return model.api().equals("google-generative-ai")
                || model.api().equals("google-vertex");
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        if (!supports(model)) {
            return Multi.createFrom().failure(new IllegalArgumentException(
                    "Unsupported Google API " + model.api()
            ));
        }
        Map<String, String> headers;
        try {
            headers = authenticationHeaders(model, options);
        } catch (IllegalArgumentException failure) {
            return Multi.createFrom().failure(failure);
        }
        return ProviderHttpHooks.prepare(
                mapper, model,
                codec.encodeRequest(model, context, options.thinkingLevel()),
                headers, options
        ).toMulti().onItem().transformToMultiAndConcatenate(prepared -> {
            byte[] body;
            try {
                body = mapper.writeValueAsBytes(prepared.payload());
            } catch (JsonProcessingException failure) {
                return Multi.createFrom().failure(failure);
            }
            return ProviderHttpHooks.observeSse(transport.execute(
                    SseHttpRequest.post(uri(model), prepared.headers(), body),
                    options.cancellation()
            ), model, options).toMulti()
                    .onItem().transformToMultiAndConcatenate(response ->
                            codec.decode(response.events(), model)
                    );
        });
    }

    static Map<String, String> authenticationHeaders(
            Model model,
            StreamOptions options
    ) {
        LinkedHashMap<String, String> headers =
                new LinkedHashMap<>(options.headers());
        headers.put("content-type", "application/json");
        headers.put("accept", "text/event-stream");
        String credential = options.apiKey() == null
                ? null : options.apiKey().trim();
        String suppliedAuthorization = headers.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase("authorization"))
                .map(Map.Entry::getValue)
                .filter(value -> value != null && !value.isBlank())
                .findFirst().orElse(null);
        if (model.api().equals("google-vertex")
                && suppliedAuthorization != null) {
            return Map.copyOf(headers);
        }
        if (credential == null || credential.isBlank()) {
            throw new IllegalArgumentException(
                    "No API key or access token for provider: "
                            + model.provider()
            );
        }
        if (model.api().equals("google-vertex")
                && isBearerCredential(credential)) {
            String token = credential.substring(
                    credential.indexOf(' ') + 1
            ).trim();
            headers.put("authorization", "Bearer " + token);
        } else if (model.api().equals("google-vertex")
                && (credential.startsWith("ya29.")
                || credential.startsWith("eyJ"))) {
            headers.put("authorization", "Bearer " + credential);
        } else {
            headers.put("x-goog-api-key", credential);
        }
        return Map.copyOf(headers);
    }

    private static boolean isBearerCredential(String value) {
        int separator = value.indexOf(' ');
        return separator > 0
                && value.substring(0, separator).equalsIgnoreCase("Bearer")
                && !value.substring(separator + 1).isBlank();
    }

    static URI uri(Model model) {
        return uri(
                model, System.getenv("GOOGLE_CLOUD_PROJECT"),
                System.getenv("GOOGLE_CLOUD_LOCATION")
        );
    }

    static URI uri(Model model, String project, String location) {
        if (model.baseUrl().isBlank()) {
            throw new IllegalArgumentException(
                    "Google Vertex model requires a baseUrl containing project and location"
            );
        }
        String baseUrl = model.baseUrl();
        if (baseUrl.contains("{location}")) {
            if (location == null || location.isBlank()
                    || project == null || project.isBlank()) {
                throw new IllegalArgumentException(
                        "Google Vertex requires GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION"
                );
            }
            baseUrl = baseUrl.replace("{location}", location)
                    + "/v1/projects/" + encodePath(project)
                    + "/locations/" + encodePath(location);
        }
        String normalized = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        String encodedId = encodePath(model.id());
        if (normalized.endsWith(":streamGenerateContent")) {
            return URI.create(normalized + "?alt=sse");
        }
        if (normalized.matches(".*/models/[^/]+$")) {
            return URI.create(normalized + ":streamGenerateContent?alt=sse");
        }
        if (model.api().equals("google-vertex")) {
            return URI.create(normalized + "/publishers/google/models/"
                    + encodedId + ":streamGenerateContent?alt=sse");
        }
        return URI.create(normalized + "/models/" + encodedId
                + ":streamGenerateContent?alt=sse");
    }

    private static String encodePath(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    @Override
    public void close() {
        if (ownsTransport) transport.close();
    }
}
