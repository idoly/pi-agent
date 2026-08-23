package io.github.idoly.pi.vertx;

import io.smallrye.mutiny.Multi;

import java.util.List;
import java.util.Map;

public record SseHttpResponse(
        int status,
        Map<String, List<String>> headers,
        Multi<SseEvent> events
) {
}
