package io.github.idoly.pi.vertx;

import io.smallrye.mutiny.Multi;

import java.util.List;
import java.util.Map;

/** Streaming binary HTTP response without exposing Vert.x Buffer publicly. */
public record BinaryHttpResponse(
        int status,
        Map<String, List<String>> headers,
        Multi<byte[]> chunks
) {
    public BinaryHttpResponse {
        headers = Map.copyOf(headers);
    }
}
