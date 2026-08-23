package io.github.idoly.pi.vertx;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.PoolOptions;
import io.vertx.core.http.RequestOptions;
import io.github.idoly.pi.ai.CancellationSignal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Pooled HTTP/1.1 and HTTP/2 SSE transport backed by Vert.x 5 and exposed through Mutiny. */
public final class VertxSseHttpClient implements AutoCloseable {
    private final Vertx vertx;
    private final HttpClientAgent client;
    private final VertxSseClientOptions options;
    private final boolean ownsVertx;
    private final AtomicBoolean closed = new AtomicBoolean();

    public VertxSseHttpClient() {
        this(Vertx.vertx(), VertxSseClientOptions.DEFAULT, true);
    }

    public VertxSseHttpClient(Vertx vertx, VertxSseClientOptions options) {
        this(vertx, options, false);
    }

    private VertxSseHttpClient(
            Vertx vertx,
            VertxSseClientOptions options,
            boolean ownsVertx
    ) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.options = Objects.requireNonNull(options, "options");
        this.ownsVertx = ownsVertx;
        HttpClientOptions clientOptions = new HttpClientOptions()
                .setConnectTimeout(durationMillis(options.connectTimeout()))
                .setKeepAlive(true)
                .setDecompressionSupported(true)
                .setTrustAll(options.trustAll())
                .setHttp2MultiplexingLimit(options.http2MultiplexingLimit());
        if (options.preferHttp2()) {
            clientOptions
                    .setProtocolVersion(HttpVersion.HTTP_2)
                    .setUseAlpn(true)
                    .setAlpnVersions(List.of(HttpVersion.HTTP_2, HttpVersion.HTTP_1_1))
                    .setHttp2ClearTextUpgrade(true);
        }
        PoolOptions poolOptions = new PoolOptions()
                .setHttp1MaxSize(options.maxHttp1Connections())
                .setHttp2MaxSize(options.maxHttp2Connections())
                .setMaxWaitQueueSize(options.maxWaitQueueSize());
        this.client = vertx.createHttpClient(clientOptions, poolOptions);
    }

    public Uni<SseHttpResponse> execute(
            SseHttpRequest request,
            CancellationSignal cancellation
    ) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(cancellation, "cancellation");
        return Uni.createFrom().deferred(() -> executeDeferred(request, cancellation));
    }

    private Uni<SseHttpResponse> executeDeferred(
            SseHttpRequest request,
            CancellationSignal cancellation
    ) {
        if (closed.get()) {
            return Uni.createFrom().failure(new IllegalStateException("Client is closed"));
        }
        try {
            cancellation.throwIfCancelled();
        } catch (Throwable failure) {
            return Uni.createFrom().failure(failure);
        }

        RequestOptions requestOptions = new RequestOptions()
                .setAbsoluteURI(request.uri().toASCIIString())
                .setMethod(HttpMethod.valueOf(request.method()))
                .setTimeout(options.requestTimeout().toMillis())
                .setIdleTimeout(options.readIdleTimeout().toMillis())
                .putHeader("accept", "text/event-stream");
        request.headers().forEach(requestOptions::putHeader);

        return uni(client.request(requestOptions))
                .chain(outbound -> send(outbound, request, cancellation));
    }

    private Uni<SseHttpResponse> send(
            HttpClientRequest outbound,
            SseHttpRequest request,
            CancellationSignal cancellation
    ) {
        AtomicReference<AutoCloseable> cancellationRegistration = new AtomicReference<>();
        try {
            cancellationRegistration.set(cancellation.onCancel(outbound::cancel));
        } catch (Throwable failure) {
            outbound.reset();
            return Uni.createFrom().failure(failure);
        }
        if (cancellation.isCancelled()) {
            outbound.reset();
            closeRegistration(cancellationRegistration);
            return Uni.createFrom().failure(new CancellationException("HTTP request cancelled"));
        }

        return uni(outbound.send(Buffer.buffer(request.body())))
                .onCancellation().invoke(() -> {
                    outbound.cancel();
                    closeRegistration(cancellationRegistration);
                })
                .onFailure().invoke(ignored -> closeRegistration(cancellationRegistration))
                .map(response -> mapResponse(
                        response,
                        outbound,
                        cancellation,
                        cancellationRegistration
                ));
    }

    private SseHttpResponse mapResponse(
            HttpClientResponse response,
            HttpClientRequest request,
            CancellationSignal cancellation,
            AtomicReference<AutoCloseable> cancellationRegistration
    ) {
        Map<String, List<String>> headers = copyHeaders(response.headers());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            request.cancel();
            closeRegistration(cancellationRegistration);
            throw new HttpResponseException(
                    response.statusCode(), response.statusMessage(), headers
            );
        }
        String contentType = response.getHeader("content-type");
        if (contentType != null
                && !contentType.toLowerCase(Locale.ROOT).startsWith("text/event-stream")) {
            request.cancel();
            closeRegistration(cancellationRegistration);
            throw new IllegalStateException(
                    "Expected text/event-stream response but received " + contentType
            );
        }

        SseDecoder decoder = new SseDecoder(options.maxSseLineLength());
        Multi<SseEvent> events = buffers(
                response,
                request,
                cancellation,
                options.maxPendingResponseBuffers()
        )
                .onItem().transformToMultiAndConcatenate(buffer ->
                        Multi.createFrom().iterable(decoder.decode(buffer))
                )
                .ifNoItem().after(options.readIdleTimeout()).fail()
                .onTermination().invoke(() -> closeRegistration(cancellationRegistration));
        return new SseHttpResponse(response.statusCode(), headers, events);
    }

    private static Multi<Buffer> buffers(
            HttpClientResponse response,
            HttpClientRequest request,
            CancellationSignal cancellation,
            int maxPendingBuffers
    ) {
        UnicastProcessor<Buffer> processor = UnicastProcessor.create(
                new ArrayBlockingQueue<>(maxPendingBuffers),
                () -> { }
        );
        response.exceptionHandler(failure -> {
            if (cancellation.isCancelled()) {
                processor.onError(new CancellationException("HTTP request cancelled"));
            } else {
                processor.onError(failure);
            }
        });
        response.handler(processor::onNext);
        response.endHandler(ignored -> processor.onComplete());
        response.resume();
        return processor
                .onFailure().invoke(ignored -> request.cancel())
                .onCancellation().invoke(request::cancel);
    }

    private static Map<String, List<String>> copyHeaders(MultiMap source) {
        Map<String, List<String>> headers = new LinkedHashMap<>();
        source.names().forEach(name -> headers.put(
                name,
                List.copyOf(new ArrayList<>(source.getAll(name)))
        ));
        return Map.copyOf(headers);
    }

    private static <T> Uni<T> uni(io.vertx.core.Future<T> future) {
        return Uni.createFrom().completionStage(future.toCompletionStage());
    }

    private static void closeRegistration(AtomicReference<AutoCloseable> reference) {
        AutoCloseable registration = reference.getAndSet(null);
        if (registration == null) {
            return;
        }
        try {
            registration.close();
        } catch (Exception ignored) {
            // Callback cleanup must not replace the HTTP or stream result.
        }
    }

    private static int durationMillis(java.time.Duration duration) {
        return Math.toIntExact(Math.min(Integer.MAX_VALUE, duration.toMillis()));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        client.close();
        if (ownsVertx) {
            vertx.close();
        }
    }
}
