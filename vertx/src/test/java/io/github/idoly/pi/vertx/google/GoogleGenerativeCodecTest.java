package io.github.idoly.pi.vertx.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.SseEvent;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GoogleGenerativeCodecTest {
    private final GoogleGenerativeCodec codec =
            new GoogleGenerativeCodec(new ObjectMapper());

    @Test
    void encodesMultimodalMessagesToolsAndSameModelSignatures() {
        Model model = model("google-generative-ai");
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new ThinkingContent("why", "c2ln"),
                        new TextContent("answer", "dGV4dA=="),
                        new ToolCallContent(
                                "call.id", "lookup", Map.of("q", "x"), "dG9vbA=="
                        )
                ), model.api(), model.provider(), model.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        ModelContext context = new ModelContext(
                "system",
                List.of(
                        new UserMessage(List.of(
                                new TextContent("hello"),
                                new ImageContent("base64", "image/png")
                        ), 0),
                        assistant,
                        new ToolResultMessage(
                                "call.id", "lookup",
                                List.of(new TextContent("result")),
                                Map.of(), null, false, 2
                        )
                ),
                List.of(new ToolDefinition(
                        "lookup", "Lookup", Map.of("type", "object")
                ))
        );
        var request = codec.encodeRequest(model, context, "high");
        assertEquals("system", request.path("systemInstruction")
                .path("parts").get(0).path("text").asText());
        assertEquals("HIGH", request.path("generationConfig")
                .path("thinkingConfig").path("thinkingLevel").asText());
        assertEquals("c2ln", request.path("contents").get(1)
                .path("parts").get(0).path("thoughtSignature").asText());
        assertEquals("call.id", request.path("contents").get(1)
                .path("parts").get(2).path("functionCall").path("id").asText());
        assertEquals("object", request.path("tools").get(0)
                .path("functionDeclarations").get(0)
                .path("parametersJsonSchema").path("type").asText());
    }

    @Test
    void normalizesForeignToolIdsAndReusesThemForResults() {
        Model model = model("google-generative-ai");
        AssistantMessage foreign = new AssistantMessage(
                List.of(new ToolCallContent(
                        "call.foreign/id", "lookup", Map.of()
                )), "openai-responses", "openai", "other",
                Usage.ZERO, StopReason.TOOL_USE, null, 1
        );
        ModelContext context = new ModelContext(
                "", List.of(
                        foreign,
                        new ToolResultMessage(
                                "call.foreign/id", "lookup",
                                List.of(new TextContent("result")),
                                Map.of(), null, false, 2
                        )
                ), List.of()
        );
        var contents = codec.encodeRequest(model, context, "off")
                .path("contents");
        assertEquals("call_foreign_id", contents.get(0).path("parts").get(0)
                .path("functionCall").path("id").asText());
        assertEquals("call_foreign_id", contents.get(1).path("parts").get(0)
                .path("functionResponse").path("id").asText());
    }

    @Test
    void decodesThinkingTextToolUsageAndFinish() {
        Multi<SseEvent> source = Multi.createFrom().items(
                event("""
                        {"responseId":"response","candidates":[{"content":{"parts":[{"thought":true,"text":"why","thoughtSignature":"c2ln"},{"text":"answer","thoughtSignature":"dGV4dA=="},{"functionCall":{"id":"call","name":"lookup","args":{"q":"x"}},"thoughtSignature":"dG9vbA=="}]}}],"usageMetadata":{"promptTokenCount":12,"cachedContentTokenCount":2,"candidatesTokenCount":4,"thoughtsTokenCount":3,"totalTokenCount":19}}
                        """),
                event("""
                        {"candidates":[{"finishReason":"STOP"}]}
                        """)
        );
        List<AssistantStreamEvent> events = codec.decode(source, model(
                "google-generative-ai"
        )).collect().asList().await().indefinitely();
        AssistantMessage done = ((AssistantStreamEvent.Done)
                events.getLast()).message();
        assertEquals("response", done.responseId());
        assertEquals(StopReason.TOOL_USE, done.stopReason());
        assertEquals(new ThinkingContent("why", "c2ln"), done.content().get(0));
        assertEquals(new TextContent("answer", "dGV4dA=="), done.content().get(1));
        assertEquals(new ToolCallContent(
                "call", "lookup", Map.of("q", "x"), "dG9vbA=="
        ), done.content().get(2));
        assertEquals(10, done.usage().input());
        assertEquals(7, done.usage().output());
        assertEquals(3, done.usage().reasoning());
        assertEquals(2, done.usage().cacheRead());
    }

    @Test
    void supportsExplicitOpaqueVertexBearerTokens() {
        Model vertex = model("google-vertex");
        Map<String, String> headers = GoogleGenerativeModelStream
                .authenticationHeaders(vertex, new StreamOptions(
                        "session", "Bearer opaque-access-token", "off",
                        CancellationSignal.NONE
                ));
        assertEquals("Bearer opaque-access-token", headers.get("authorization"));
        assertFalse(headers.containsKey("x-goog-api-key"));
    }

    @Test
    void preservesHostSuppliedVertexAuthorizationWithoutAnApiKey() {
        Model vertex = model("google-vertex");
        Map<String, String> headers = GoogleGenerativeModelStream
                .authenticationHeaders(vertex, new StreamOptions(
                        "session", null, "off", CancellationSignal.NONE,
                        Map.of("Authorization", "Bearer refreshed-token")
                ));
        assertEquals("Bearer refreshed-token", headers.get("Authorization"));
        assertFalse(headers.containsKey("x-goog-api-key"));
    }

    @Test
    void preservesLegacyVertexAccessTokenDetection() {
        Model vertex = model("google-vertex");
        Map<String, String> headers = GoogleGenerativeModelStream
                .authenticationHeaders(vertex, new StreamOptions(
                        "session", "ya29.legacy-token", "off",
                        CancellationSignal.NONE
                ));
        assertEquals("Bearer ya29.legacy-token", headers.get("authorization"));
        assertFalse(headers.containsKey("x-goog-api-key"));
    }

    @Test
    void keepsOpaqueVertexApiKeysAsApiKeys() {
        Model vertex = model("google-vertex");
        Map<String, String> headers = GoogleGenerativeModelStream
                .authenticationHeaders(vertex, new StreamOptions(
                        "session", "opaque-api-key", "off",
                        CancellationSignal.NONE
                ));
        assertEquals("opaque-api-key", headers.get("x-goog-api-key"));
        assertFalse(headers.containsKey("authorization"));
    }

    @Test
    void buildsAiStudioAndVertexUris() {
        assertEquals(
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro:streamGenerateContent?alt=sse",
                GoogleGenerativeModelStream.uri(model(
                        "google-generative-ai"
                )).toString()
        );
        Model vertex = new Model(
                "gemini-3-pro", "Gemini", "google-vertex", "google-vertex",
                "https://{location}-aiplatform.googleapis.com",
                true, List.of("text"), 1000, 100
        );
        String vertexUri = GoogleGenerativeModelStream.uri(
                vertex, "project", "us-central1"
        ).toString();
        assertTrue(vertexUri.contains(
                "/v1/projects/project/locations/us-central1/publishers/google/models/gemini-3-pro:streamGenerateContent"
        ));
        assertThrows(IllegalArgumentException.class, () ->
                GoogleGenerativeModelStream.uri(vertex, null, null));
    }

    private static SseEvent event(String data) {
        return new SseEvent(null, data.strip(), null, null);
    }

    private static Model model(String api) {
        return new Model(
                "gemini-3-pro", "Gemini", api, "google",
                "https://generativelanguage.googleapis.com/v1beta",
                true, List.of("text", "image"), 1_000_000, 64_000
        );
    }
}
