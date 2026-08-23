package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.VertxSseClientOptions;
import io.github.idoly.pi.vertx.VertxSseHttpClient;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiCompatibleModelStreamTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<JsonNode> requestBody = new AtomicReference<>();
    private Vertx vertx;
    private HttpServer server;
    private VertxSseHttpClient transport;
    private OpenAiCompatibleModelStream modelStream;
    private String baseUrl;

    @BeforeEach
    void startServer() throws Exception {
        vertx = Vertx.vertx();
        server = vertx.createHttpServer().requestHandler(request -> request.body().onSuccess(body -> {
            path.set(request.path());
            authorization.set(request.getHeader("authorization"));
            try {
                requestBody.set(mapper.readTree(body.getBytes()));
            } catch (Exception failure) {
                request.response().setStatusCode(400).end(failure.getMessage());
                return;
            }
            var response = request.response()
                    .setChunked(true)
                    .putHeader("content-type", "text/event-stream");
            response.write("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\","
                            + "\"content\":\"hel\"},\"finish_reason\":null}]}\n\n")
                    .compose(ignored -> response.write(
                            "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"},"
                                    + "\"finish_reason\":\"stop\"}]}\n\n"
                    ))
                    .compose(ignored -> response.write(
                            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,"
                                    + "\"completion_tokens\":2,\"total_tokens\":7}}\n\n"
                    ))
                    .compose(ignored -> response.end("data: [DONE]\n\n"));
        }));
        server.listen(0, "127.0.0.1").await(3, TimeUnit.SECONDS);
        baseUrl = "http://127.0.0.1:" + server.actualPort() + "/v1";
        transport = new VertxSseHttpClient(vertx, new VertxSseClientOptions(
                2, 1, 100, 8,
                Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(2),
                16 * 1024, 8, false
        ));
        modelStream = new OpenAiCompatibleModelStream(transport, mapper);
    }

    @AfterEach
    void stopServer() throws Exception {
        modelStream.close();
        transport.close();
        server.close().await(3, TimeUnit.SECONDS);
        vertx.close().await(3, TimeUnit.SECONDS);
    }

    @Test
    void sendsACompatibleRequestAndMapsTheCompleteStream() {
        Model model = new Model(
                "fixture-model", "Fixture", "openai-completions", "local",
                baseUrl, false, List.of("text"), 8_192, 1_024
        );
        ModelContext context = new ModelContext(
                "Be concise",
                List.of(UserMessage.text("Hi", 1L))
        );
        StreamOptions options = new StreamOptions(
                "session-1", "secret-key", "off", NeverCancelled.INSTANCE
        );

        List<AssistantStreamEvent> events = Multi.createFrom().publisher(
                modelStream.stream(model, context, options)
        ).collect().asList().await().atMost(Duration.ofSeconds(3));

        assertEquals("/v1/chat/completions", path.get());
        assertEquals("Bearer secret-key", authorization.get());
        assertEquals("fixture-model", requestBody.get().path("model").asText());
        assertEquals("Be concise", requestBody.get().path("messages").get(0)
                .path("content").asText());
        assertEquals("Hi", requestBody.get().path("messages").get(1)
                .path("content").asText());
        assertTrue(requestBody.get().path("stream_options").path("include_usage").asBoolean());

        AssistantStreamEvent.Done doneEvent = assertInstanceOf(
                AssistantStreamEvent.Done.class, events.getLast()
        );
        assertEquals(StopReason.STOP, doneEvent.message().stopReason());
        assertEquals("hello", assertInstanceOf(
                TextContent.class, doneEvent.message().content().getFirst()
        ).text());
        assertEquals(5, doneEvent.message().usage().input());
        assertEquals(2, doneEvent.message().usage().output());
        assertEquals(7, doneEvent.message().usage().totalTokens());
    }

    @Test
    void doesNotAppendTheEndpointTwice() {
        assertEquals(
                URI.create("https://example.com/v1/chat/completions"),
                OpenAiCompatibleModelStream.chatCompletionsUri(
                        "https://example.com/v1/chat/completions/"
                )
        );
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
}
