package io.github.idoly.pi.vertx.internal;

import io.github.idoly.pi.vertx.BinaryHttpResponse;
import io.github.idoly.pi.vertx.HttpResponseException;
import io.github.idoly.pi.vertx.SseHttpResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StreamOptions;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Internal bridge from framework-neutral provider hooks to Jackson/Mutiny. */
public final class ProviderHttpHooks {
    private ProviderHttpHooks() { }

    public static Uni<Prepared> prepare(
            ObjectMapper mapper,
            Model model,
            ObjectNode payload,
            Map<String, String> headers,
            StreamOptions options
    ) {
        Objects.requireNonNull(payload, "payload");
        Object value = mapper.convertValue(payload, Object.class);
        return Uni.createFrom().completionStage(() ->
                options.requestHooks().beforeHeaders(
                        model, Map.copyOf(headers), options.cancellation()
                )
        ).chain(updatedHeaders -> Uni.createFrom().completionStage(() ->
                options.requestHooks().beforeRequest(
                        model, value, options.cancellation()
                )
        ).map(updatedPayload -> {
            JsonNode tree = mapper.valueToTree(updatedPayload);
            if (!(tree instanceof ObjectNode object)) {
                throw new IllegalArgumentException(
                        "Provider request hook must return a JSON object"
                );
            }
            return new Prepared(object, Map.copyOf(updatedHeaders));
        }));
    }

    public static Uni<SseHttpResponse> observeSse(
            Uni<SseHttpResponse> response,
            Model model,
            StreamOptions options
    ) {
        return observe(response, model, options,
                SseHttpResponse::status, SseHttpResponse::headers);
    }

    public static Uni<BinaryHttpResponse> observeBinary(
            Uni<BinaryHttpResponse> response,
            Model model,
            StreamOptions options
    ) {
        return observe(response, model, options,
                BinaryHttpResponse::status, BinaryHttpResponse::headers);
    }

    private static <T> Uni<T> observe(
            Uni<T> response,
            Model model,
            StreamOptions options,
            java.util.function.ToIntFunction<T> status,
            java.util.function.Function<T, Map<String, List<String>>> headers
    ) {
        return response.onItem().call(value -> afterResponse(
                model, status.applyAsInt(value), headers.apply(value), options
        )).onFailure(HttpResponseException.class).call(failure -> {
            HttpResponseException responseFailure =
                    (HttpResponseException) failure;
            return afterResponse(
                    model, responseFailure.status(), responseFailure.headers(),
                    options
            );
        });
    }

    public static Uni<Void> afterResponse(
            Model model,
            int status,
            Map<String, List<String>> headers,
            StreamOptions options
    ) {
        return Uni.createFrom().completionStage(() ->
                options.requestHooks().afterResponse(
                        model, status, immutableHeaders(headers),
                        options.cancellation()
                )
        );
    }

    private static Map<String, List<String>> immutableHeaders(
            Map<String, List<String>> headers
    ) {
        java.util.LinkedHashMap<String, List<String>> copy =
                new java.util.LinkedHashMap<>();
        headers.forEach((name, values) -> copy.put(
                name, values == null ? List.of() : List.copyOf(values)
        ));
        return Map.copyOf(copy);
    }

    public record Prepared(ObjectNode payload, Map<String, String> headers) {
        public Prepared {
            Objects.requireNonNull(payload, "payload");
            headers = Map.copyOf(headers);
        }
    }
}
