package io.github.idoly.pi.vertx;

public record SseEvent(
        String event,
        String data,
        String id,
        Long retryMillis
) {
}
