package io.github.idoly.pi.vertx.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiResponsesModelStreamTest {
    @Test
    void resolvesSessionAffinityCompatibilityFormats() {
        Model openAi = model("openai", "https://api.openai.com/v1");
        Model openRouter = model("proxy", "https://openrouter.ai/api/v1");
        Model openCode = model("opencode", "https://opencode.ai/v1");

        assertEquals(
                Map.of("session_id", "s", "x-client-request-id", "s"),
                OpenAiResponsesModelStream.sessionAffinityHeaders(
                        openAi, OpenAiResponsesCompatibility.DEFAULT, "s"
                )
        );
        assertEquals(
                Map.of("x-session-id", "s"),
                OpenAiResponsesModelStream.sessionAffinityHeaders(
                        openRouter, OpenAiResponsesCompatibility.DEFAULT, "s"
                )
        );
        assertEquals(
                Map.of("x-client-request-id", "s"),
                OpenAiResponsesModelStream.sessionAffinityHeaders(
                        openCode, OpenAiResponsesCompatibility.DEFAULT, "s"
                )
        );
        OpenAiResponsesCompatibility forcedOpenRouter = new OpenAiResponsesCompatibility(
                true, "none",
                OpenAiResponsesCompatibility.SessionAffinityFormat.OPENROUTER,
                true
        );
        assertEquals(
                Map.of("x-session-id", "s"),
                OpenAiResponsesModelStream.sessionAffinityHeaders(
                        openAi, forcedOpenRouter, "s"
                )
        );
    }

    @Test
    void sendsResponsesRequestAndMapsItsStream() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> sessionId = new AtomicReference<>();
        AtomicReference<String> clientRequestId = new AtomicReference<>();
        AtomicReference<JsonNode> body = new AtomicReference<>();
        Vertx vertx = Vertx.vertx();
        HttpServer server = vertx.createHttpServer().requestHandler(request ->
                request.body().onSuccess(content -> {
                    path.set(request.path());
                    authorization.set(request.getHeader("authorization"));
                    sessionId.set(request.getHeader("session_id"));
                    clientRequestId.set(request.getHeader("x-client-request-id"));
                    try {
                        body.set(mapper.readTree(content.getBytes()));
                    } catch (Exception failure) {
                        request.response().setStatusCode(400).end(failure.getMessage());
                        return;
                    }
                    request.response()
                            .setChunked(true)
                            .putHeader("content-type", "text/event-stream")
                            .end("data: {\"type\":\"response.output_item.added\",\"output_index\":0,"
                                    + "\"sequence_number\":1,\"item\":{\"id\":\"msg_1\","
                                    + "\"type\":\"message\",\"status\":\"in_progress\","
                                    + "\"role\":\"assistant\",\"content\":[]}}\n\n"
                                    + "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,"
                                    + "\"content_index\":0,\"item_id\":\"msg_1\",\"sequence_number\":2,"
                                    + "\"delta\":\"ok\",\"logprobs\":[]}\n\n"
                                    + "data: {\"type\":\"response.output_item.done\",\"output_index\":0,"
                                    + "\"sequence_number\":3,\"item\":{\"id\":\"msg_1\","
                                    + "\"type\":\"message\",\"status\":\"completed\","
                                    + "\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\","
                                    + "\"text\":\"ok\",\"annotations\":[],\"logprobs\":[]}]}}\n\n"
                                    + "data: " + completedResponse() + "\n\n");
                })
        );
        VertxSseHttpClient transport = null;
        try {
            server.listen(0, "127.0.0.1").await(3, TimeUnit.SECONDS);
            transport = new VertxSseHttpClient(vertx, options());
            OpenAiResponsesModelStream stream = new OpenAiResponsesModelStream(transport, mapper);
            Model model = new Model(
                    "gpt-5", "GPT-5", "openai-responses", "openai",
                    "http://127.0.0.1:" + server.actualPort() + "/v1",
                    true, List.of("text"), 128_000, 1_024
            );

            List<AssistantStreamEvent> events = Multi.createFrom().publisher(stream.stream(
                    model,
                    new ModelContext("system", List.of(UserMessage.text("hello", 1L))),
                    new StreamOptions("session", "key", "high", NeverCancelled.INSTANCE)
            )).collect().asList().await().atMost(Duration.ofSeconds(3));

            assertEquals("/v1/responses", path.get());
            assertEquals("Bearer key", authorization.get());
            assertEquals("session", sessionId.get());
            assertEquals("session", clientRequestId.get());
            assertTrue(body.get().path("stream").asBoolean());
            assertEquals("gpt-5", body.get().path("model").asText());
            assertEquals("session", body.get().path("prompt_cache_key").asText());
            AssistantStreamEvent.Done done = assertInstanceOf(
                    AssistantStreamEvent.Done.class, events.getLast()
            );
            assertEquals(StopReason.STOP, done.message().stopReason());
            assertEquals("ok", assertInstanceOf(
                    TextContent.class, done.message().content().getFirst()
            ).text());
        } finally {
            if (transport != null) {
                transport.close();
            }
            server.close().await(3, TimeUnit.SECONDS);
            vertx.close().await(3, TimeUnit.SECONDS);
        }
    }

    private static Model model(String provider, String baseUrl) {
        return new Model(
                "gpt-5", "GPT-5", "openai-responses", provider, baseUrl,
                true, List.of("text"), 128_000, 1_024
        );
    }

    private static VertxSseClientOptions options() {
        return new VertxSseClientOptions(
                2, 1, 100, 8,
                Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(2),
                16 * 1024, 8, false
        );
    }

    private static String completedResponse() {
        return "{\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{"
                + "\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,"
                + "\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[],"
                + "\"parallel_tool_calls\":true,\"tool_choice\":\"auto\",\"tools\":[],"
                + "\"usage\":{\"input_tokens\":2,\"input_tokens_details\":{"
                + "\"cached_tokens\":0,\"cache_write_tokens\":0},\"output_tokens\":1,"
                + "\"output_tokens_details\":{\"reasoning_tokens\":0},\"total_tokens\":3}}}";
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
