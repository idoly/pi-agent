package io.github.idoly.pi.vertx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.anthropic.AnthropicMessagesCodec;
import io.github.idoly.pi.vertx.bedrock.BedrockConverseCodec;
import io.github.idoly.pi.vertx.google.GoogleGenerativeCodec;
import io.github.idoly.pi.vertx.SseEvent;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProviderProtocolsUpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void anthropicRequestMatchesTypeScriptOracle() throws Exception {
        Model model = model(
                "claude-fixture", "anthropic-messages", "anthropic",
                "https://api.anthropic.com", 200_000, 16_384
        );
        AssistantMessage assistant = assistant(
                model,
                List.of(
                        new ThinkingContent("old thought", "old-signature"),
                        new ToolCallContent(
                                "old-call", "lookup", Map.of("q", "old")
                        )
                )
        );
        ModelContext context = new ModelContext(
                "system",
                List.of(
                        UserMessage.text("hello", 1),
                        assistant,
                        new ToolResultMessage(
                                "old-call", "lookup",
                                List.of(new TextContent("old result")),
                                Map.of(), null, false, 3
                        )
                ),
                List.of(tool())
        );
        JsonNode actual = new AnthropicMessagesCodec(MAPPER)
                .encodeRequest(model, context, "medium");
        assertEquals(fixture().path("anthropic").path("request"), actual);
    }

    @Test
    void anthropicStreamErrorMatchesTypeScriptOracle() throws Exception {
        JsonNode expected = fixture().path("anthropic").path("streamError");
        String payload = MAPPER.writeValueAsString(expected.path("payload"));
        List<AssistantStreamEvent> events = new AnthropicMessagesCodec(MAPPER)
                .decode(Multi.createFrom().item(
                        new SseEvent("error", payload, null, null)
                ), model(
                        "claude-fixture", "anthropic-messages", "anthropic",
                        "https://api.anthropic.com", 200_000, 16_384
                ))
                .collect().asList().await().indefinitely();
        AssistantMessage actual = ((AssistantStreamEvent.Error)
                events.getLast()).message();

        assertEquals(List.of("start", "error"), events.stream().map(event ->
                event instanceof AssistantStreamEvent.Start ? "start"
                        : event instanceof AssistantStreamEvent.Error
                        ? "error" : event.getClass().getSimpleName()
        ).toList());
        assertEquals(expected.path("message").path("errorMessage").asText(),
                actual.errorMessage());
        assertEquals(StopReason.ERROR, actual.stopReason());
    }

    @Test
    void googleMessagesAndToolsMatchTypeScriptOracle() throws Exception {
        Model model = model(
                "gemini-3-pro", "google-generative-ai", "google",
                "https://generativelanguage.googleapis.com/v1beta",
                1_000_000, 64_000
        );
        AssistantMessage assistant = assistant(
                model,
                List.of(
                        new ThinkingContent("why", "c2ln"),
                        new TextContent("answer", "dGV4dA=="),
                        new ToolCallContent(
                                "call.id", "lookup", Map.of("q", "x"),
                                "dG9vbA=="
                        )
                )
        );
        ModelContext context = new ModelContext(
                "system",
                List.of(
                        new UserMessage(List.of(
                                new TextContent("hello"),
                                new ImageContent("aGVsbG8=", "image/png")
                        ), 1),
                        assistant,
                        new ToolResultMessage(
                                "call.id", "lookup",
                                List.of(new TextContent("result")),
                                Map.of(), null, false, 3
                        )
                ),
                List.of(tool())
        );
        JsonNode actual = new GoogleGenerativeCodec(MAPPER)
                .encodeRequest(model, context, "high");
        JsonNode expected = fixture().path("google");
        assertEquals(expected.path("contents"), actual.path("contents"));
        assertEquals(expected.path("tools"), actual.path("tools"));
    }

    @Test
    void bedrockCommandInputMatchesTypeScriptOracle() throws Exception {
        Model model = model(
                "anthropic.claude-fixture", "bedrock-converse-stream",
                "amazon-bedrock",
                "https://bedrock-runtime.us-east-1.amazonaws.com",
                200_000, 16_000
        );
        JsonNode actual = new BedrockConverseCodec(MAPPER).encodeRequest(
                model,
                new ModelContext(
                        "system", List.of(UserMessage.text("hello", 1)),
                        List.of(tool())
                ),
                "medium"
        );
        ObjectNode expected = (ObjectNode) fixture().path("bedrock")
                .path("request").deepCopy();
        expected.remove("modelId");
        assertEquals(expected, actual);
    }

    @Test
    void bedrockModeledErrorMatchesTypeScriptOracle() throws Exception {
        JsonNode expected = fixture().path("bedrock").path("modeledError");
        Model model = model(
                "anthropic.claude-fixture", "bedrock-converse-stream",
                "amazon-bedrock",
                "https://bedrock-runtime.us-east-1.amazonaws.com",
                200_000, 16_000
        );
        byte[] frame = bedrockFrame(
                expected.path("eventType").asText(),
                MAPPER.writeValueAsString(expected.path("payload"))
        );
        List<AssistantStreamEvent> events = new BedrockConverseCodec(MAPPER)
                .decode(Multi.createFrom().item(frame), model)
                .collect().asList().await().indefinitely();
        AssistantMessage actual = ((AssistantStreamEvent.Error)
                events.getLast()).message();

        assertEquals(List.of("error"), events.stream().map(event ->
                event instanceof AssistantStreamEvent.Error
                        ? "error" : event.getClass().getSimpleName()
        ).toList());
        assertEquals(expected.path("message").path("stopReason").asText(),
                actual.stopReason().name().toLowerCase());
        assertEquals(expected.path("message").path("errorMessage").asText(),
                actual.errorMessage());
    }

    private static byte[] bedrockFrame(String eventType, String json) {
        byte[] headers = bedrockHeaders(Map.of(
                ":message-type", "exception",
                ":event-type", eventType,
                ":content-type", "application/json"
        ));
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        int total = 16 + headers.length + payload.length;
        ByteBuffer frame = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(total).putInt(headers.length);
        CRC32 prelude = new CRC32();
        prelude.update(frame.array(), 0, 8);
        frame.putInt((int) prelude.getValue());
        frame.put(headers).put(payload);
        CRC32 message = new CRC32();
        message.update(frame.array(), 0, total - 4);
        frame.putInt((int) message.getValue());
        return frame.array();
    }

    private static byte[] bedrockHeaders(Map<String, String> headers) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        headers.forEach((name, value) -> {
            byte[] key = name.getBytes(StandardCharsets.UTF_8);
            byte[] data = value.getBytes(StandardCharsets.UTF_8);
            output.write(key.length);
            output.writeBytes(key);
            output.write(7);
            output.write((data.length >>> 8) & 0xff);
            output.write(data.length & 0xff);
            output.writeBytes(data);
        });
        return output.toByteArray();
    }

    private static Model model(
            String id, String api, String provider, String baseUrl,
            int contextWindow, int maxTokens
    ) {
        return new Model(
                id, id, api, provider, baseUrl, true,
                List.of("text", "image"), contextWindow, maxTokens
        );
    }

    private static AssistantMessage assistant(
            Model model, List<ContentBlock> content
    ) {
        return new AssistantMessage(
                content, model.api(), model.provider(), model.id(),
                Usage.ZERO, StopReason.TOOL_USE, null, 2
        );
    }

    private static ToolDefinition tool() {
        LinkedHashMap<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", Map.of("q", Map.of("type", "string")));
        schema.put("required", List.of("q"));
        return new ToolDefinition("lookup", "Lookup", schema);
    }

    private static JsonNode fixture() throws Exception {
        return MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"),
                "provider-protocols-0.84.2.json"
        ).toFile());
    }
}
