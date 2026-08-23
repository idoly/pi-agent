package io.github.idoly.pi.vertx;

import java.time.Duration;
import java.util.Objects;

public record VertxSseClientOptions(
        int maxHttp1Connections,
        int maxHttp2Connections,
        int http2MultiplexingLimit,
        int maxWaitQueueSize,
        Duration connectTimeout,
        Duration requestTimeout,
        Duration readIdleTimeout,
        int maxSseLineLength,
        int maxPendingResponseBuffers,
        boolean preferHttp2,
        boolean trustAll
) {
    public static final VertxSseClientOptions DEFAULT = new VertxSseClientOptions(
            64,
            8,
            100,
            1_024,
            Duration.ofSeconds(10),
            Duration.ofMinutes(5),
            Duration.ofSeconds(60),
            1_048_576,
            32,
            true,
            false
    );

    public VertxSseClientOptions(
            int maxHttp1Connections,
            int maxHttp2Connections,
            int http2MultiplexingLimit,
            int maxWaitQueueSize,
            Duration connectTimeout,
            Duration requestTimeout,
            Duration readIdleTimeout,
            int maxSseLineLength,
            int maxPendingResponseBuffers,
            boolean preferHttp2
    ) {
        this(
                maxHttp1Connections, maxHttp2Connections, http2MultiplexingLimit,
                maxWaitQueueSize, connectTimeout, requestTimeout, readIdleTimeout,
                maxSseLineLength, maxPendingResponseBuffers, preferHttp2, false
        );
    }

    public VertxSseClientOptions {
        requirePositive(maxHttp1Connections, "maxHttp1Connections");
        requirePositive(maxHttp2Connections, "maxHttp2Connections");
        requirePositive(http2MultiplexingLimit, "http2MultiplexingLimit");
        requirePositive(maxWaitQueueSize, "maxWaitQueueSize");
        requirePositive(maxSseLineLength, "maxSseLineLength");
        requirePositive(maxPendingResponseBuffers, "maxPendingResponseBuffers");
        requirePositive(connectTimeout, "connectTimeout");
        requirePositive(requestTimeout, "requestTimeout");
        requirePositive(readIdleTimeout, "readIdleTimeout");
    }

    private static void requirePositive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.isZero() || value.toMillis() < 1) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
    }
}
