package io.github.idoly.pi.vertx;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record SseHttpRequest(
        URI uri,
        String method,
        Map<String, String> headers,
        byte[] body
) {
    public SseHttpRequest {
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(method, "method");
        headers = Map.copyOf(new LinkedHashMap<>(Objects.requireNonNull(headers, "headers")));
        body = Objects.requireNonNull(body, "body").clone();
        if (method.isBlank()) {
            throw new IllegalArgumentException("method must not be blank");
        }
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("Only http and https URIs are supported: " + uri);
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("URI must include a host: " + uri);
        }
        if (uri.getUserInfo() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("URI must not contain user info or a fragment: " + uri);
        }
    }

    @Override
    public byte[] body() {
        return body.clone();
    }

    public static SseHttpRequest post(URI uri, Map<String, String> headers, byte[] body) {
        return new SseHttpRequest(uri, "POST", headers, body);
    }
}
