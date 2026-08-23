package io.github.idoly.pi.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.SelfSignedCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.CancellationSignal;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VertxSseHttpClientTest {
    static {
        Logger.getLogger("io.vertx.core.http.impl").setLevel(Level.OFF);
        Logger.getLogger("io.vertx.core.http.impl.HttpClientResponseImpl").setLevel(Level.OFF);
    }

    private final Duration wait = Duration.ofSeconds(3);
    private Vertx vertx;
    private HttpServer server;
    private VertxSseHttpClient client;
    private URI baseUri;
    private AtomicInteger connections;
    private List<HttpVersion> requestVersions;

    @BeforeEach
    void startServer() throws Exception {
        vertx = Vertx.vertx();
        connections = new AtomicInteger();
        requestVersions = new CopyOnWriteArrayList<>();
        server = vertx.createHttpServer(new HttpServerOptions().setHttp2ClearTextEnabled(true))
                .connectionHandler(ignored -> connections.incrementAndGet())
                .requestHandler(request -> request.body().onComplete(ignored -> {
                    requestVersions.add(request.version());
                    String path = request.path();
                    if (path.equals("/unavailable")) {
                        request.response().setStatusCode(503).end("unavailable");
                    } else if (path.equals("/idle")) {
                        request.response()
                                .setChunked(true)
                                .putHeader("content-type", "text/event-stream")
                                .writeHead();
                    } else {
                        request.response()
                                .putHeader("content-type", "text/event-stream; charset=utf-8")
                                .end("data: one\n\ndata: two\n\n");
                    }
                }));
        server.listen(0, "127.0.0.1").await(3, TimeUnit.SECONDS);
        baseUri = URI.create("http://127.0.0.1:" + server.actualPort());
        client = new VertxSseHttpClient(vertx, options(Duration.ofSeconds(2), Duration.ofSeconds(2)));
    }

    @AfterEach
    void stopServer() throws Exception {
        client.close();
        server.close().await(3, TimeUnit.SECONDS);
        vertx.close().await(3, TimeUnit.SECONDS);
    }

    @Test
    void streamsEventsAndReusesTheHttp1Connection() {
        List<SseEvent> first = executeAndCollect("/events");
        List<SseEvent> second = executeAndCollect("/events");

        assertEquals(List.of("one", "two"), first.stream().map(SseEvent::data).toList());
        assertEquals(List.of("one", "two"), second.stream().map(SseEvent::data).toList());
        assertEquals(1, connections.get());
    }

    @Test
    void multiplexesConcurrentStreamsOverOneHttp2Connection() {
        client.close();
        client = new VertxSseHttpClient(
                vertx,
                options(Duration.ofSeconds(2), Duration.ofSeconds(2), true)
        );

        var first = client.execute(request("/events"), NeverCancelled.INSTANCE)
                .chain(response -> response.events().collect().asList())
                .subscribeAsCompletionStage();
        var second = client.execute(request("/events"), NeverCancelled.INSTANCE)
                .chain(response -> response.events().collect().asList())
                .subscribeAsCompletionStage();
        java.util.concurrent.CompletableFuture.allOf(
                first.toCompletableFuture(), second.toCompletableFuture()
        ).orTimeout(3, TimeUnit.SECONDS).join();

        assertEquals(1, connections.get());
        assertEquals(List.of(HttpVersion.HTTP_2, HttpVersion.HTTP_2), requestVersions);
    }

    @Test
    void negotiatesAndMultiplexesHttp2OverTlsAlpn() throws Exception {
        SelfSignedCertificate certificate = SelfSignedCertificate.create("localhost");
        HttpServer tlsServer = vertx.createHttpServer(new HttpServerOptions()
                        .setSsl(true)
                        .setUseAlpn(true)
                        .setAlpnVersions(List.of(HttpVersion.HTTP_2, HttpVersion.HTTP_1_1))
                        .setKeyCertOptions(certificate.keyCertOptions()))
                .connectionHandler(ignored -> connections.incrementAndGet())
                .requestHandler(request -> request.body().onComplete(ignored -> {
                    requestVersions.add(request.version());
                    request.response()
                            .putHeader("content-type", "text/event-stream")
                            .end("data: secure\n\n");
                }));
        try {
            tlsServer.listen(0, "localhost").await(3, TimeUnit.SECONDS);
            client.close();
            client = new VertxSseHttpClient(vertx, new VertxSseClientOptions(
                    1, 1, 100, 8,
                    Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(2),
                    16 * 1024, 8, true, true
            ));
            URI uri = URI.create("https://localhost:" + tlsServer.actualPort() + "/events");
            SseHttpRequest secureRequest = SseHttpRequest.post(
                    uri, Map.of("content-type", "application/json"),
                    "{}".getBytes(StandardCharsets.UTF_8)
            );

            var first = client.execute(secureRequest, NeverCancelled.INSTANCE)
                    .chain(response -> response.events().collect().asList())
                    .subscribeAsCompletionStage();
            var second = client.execute(secureRequest, NeverCancelled.INSTANCE)
                    .chain(response -> response.events().collect().asList())
                    .subscribeAsCompletionStage();
            java.util.concurrent.CompletableFuture.allOf(
                    first.toCompletableFuture(), second.toCompletableFuture()
            ).orTimeout(3, TimeUnit.SECONDS).join();

            assertEquals(List.of("secure"), first.toCompletableFuture().join().stream()
                    .map(SseEvent::data).toList());
            assertEquals(1, connections.get());
            assertEquals(List.of(HttpVersion.HTTP_2, HttpVersion.HTTP_2), requestVersions);
        } finally {
            tlsServer.close().await(3, TimeUnit.SECONDS);
            certificate.delete();
        }
    }

    @Test
    void rejectsNonSuccessfulResponsesBeforePublishing() {
        Throwable failure = assertThrows(Throwable.class, () -> client.execute(
                request("/unavailable"), NeverCancelled.INSTANCE
        ).await().atMost(wait));

        HttpResponseException responseFailure = assertInstanceOf(
                HttpResponseException.class, unwrap(failure)
        );
        assertEquals(503, responseFailure.status());
    }

    @Test
    void readIdleTimeoutFailsAnOpenStream() {
        client.close();
        client = new VertxSseHttpClient(
                vertx,
                options(Duration.ofMillis(100), Duration.ofSeconds(2))
        );
        SseHttpResponse response = client.execute(
                request("/idle"), NeverCancelled.INSTANCE
        ).await().atMost(wait);

        Throwable failure = assertThrows(Throwable.class, () ->
                response.events().collect().asList().await().atMost(wait)
        );
        assertInstanceOf(io.smallrye.mutiny.TimeoutException.class, unwrap(failure));
    }

    @Test
    void cancellationFailsAnOpenStream() {
        TestCancellation cancellation = new TestCancellation();
        SseHttpResponse response = client.execute(
                request("/idle"), cancellation
        ).await().atMost(wait);
        var result = response.events().collect().asList().subscribeAsCompletionStage();

        cancellation.cancel();

        Throwable failure = assertThrows(Throwable.class, () -> result.toCompletableFuture().join());
        assertInstanceOf(CancellationException.class, unwrap(failure));
    }

    private List<SseEvent> executeAndCollect(String path) {
        SseHttpResponse response = client.execute(
                request(path), NeverCancelled.INSTANCE
        ).await().atMost(wait);
        assertEquals(200, response.status());
        return response.events().collect().asList().await().atMost(wait);
    }

    private SseHttpRequest request(String path) {
        return SseHttpRequest.post(
                baseUri.resolve(path),
                Map.of("content-type", "application/json"),
                "{}".getBytes(StandardCharsets.UTF_8)
        );
    }

    private static VertxSseClientOptions options(Duration idle, Duration request) {
        return options(idle, request, false);
    }

    private static VertxSseClientOptions options(
            Duration idle,
            Duration request,
            boolean preferHttp2
    ) {
        return new VertxSseClientOptions(
                1, 1, 100, 8,
                Duration.ofSeconds(2), request, idle,
                16 * 1024,
                8,
                preferHttp2
        );
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null
                && (current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)) {
            current = current.getCause();
        }
        return current;
    }

    private enum NeverCancelled implements CancellationSignal {
        INSTANCE;

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void throwIfCancelled() {
        }

        @Override
        public AutoCloseable onCancel(Runnable callback) {
            return () -> { };
        }
    }

    private static final class TestCancellation implements CancellationSignal {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final List<Runnable> callbacks = new CopyOnWriteArrayList<>();

        private void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                callbacks.forEach(Runnable::run);
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void throwIfCancelled() {
            if (isCancelled()) {
                throw new CancellationException();
            }
        }

        @Override
        public AutoCloseable onCancel(Runnable callback) {
            callbacks.add(callback);
            return () -> callbacks.remove(callback);
        }
    }
}
