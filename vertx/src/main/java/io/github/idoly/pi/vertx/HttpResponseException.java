package io.github.idoly.pi.vertx;

import java.util.List;
import java.util.Map;

public final class HttpResponseException extends RuntimeException {
    private final int status;
    private final Map<String, List<String>> headers;

    public HttpResponseException(int status, String reason, Map<String, List<String>> headers) {
        super("HTTP " + status + (reason == null || reason.isBlank() ? "" : " " + reason));
        this.status = status;
        this.headers = Map.copyOf(headers);
    }

    public int status() {
        return status;
    }

    public Map<String, List<String>> headers() {
        return headers;
    }
}
