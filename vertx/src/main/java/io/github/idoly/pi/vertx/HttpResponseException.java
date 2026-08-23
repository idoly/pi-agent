package io.github.idoly.pi.vertx;

import java.util.List;
import java.util.Map;

public final class HttpResponseException extends RuntimeException {
    private final int status;
    private final Map<String, List<String>> headers;

    public HttpResponseException(int status, String reason, Map<String, List<String>> headers) {
        super("HTTP " + status + (reason == null || reason.isBlank() ? "" : " " + reason));
        this.status = status;
        java.util.LinkedHashMap<String, List<String>> copied =
                new java.util.LinkedHashMap<>();
        headers.forEach((name, values) -> copied.put(
                name, values == null ? List.of() : List.copyOf(values)
        ));
        this.headers = Map.copyOf(copied);
    }

    public int status() {
        return status;
    }

    public Map<String, List<String>> headers() {
        return headers;
    }
}
