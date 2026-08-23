package io.github.idoly.pi.vertx.bedrock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

class BedrockConverseCodecTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final BedrockConverseCodec codec = new BedrockConverseCodec(mapper);

    @Test
    void decodesSplitSmithyFramesAndValidatesCrc() {
        byte[] stream = concat(
                frame("messageStart", "{\"role\":\"assistant\"}"),
                frame("contentBlockStart", """
                        {"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"call","name":"lookup"}}}
                        """),
                frame("contentBlockDelta", """
                        {"contentBlockIndex":0,"delta":{"toolUse":{"input":"{\\"q\\":\\"x\\"}"}}}
                        """),
                frame("contentBlockStop", "{\"contentBlockIndex\":0}"),
                frame("messageStop", "{\"stopReason\":\"tool_use\"}"),
                frame("metadata", """
                        {"usage":{"inputTokens":10,"outputTokens":4,"cacheReadInputTokens":2,"cacheWriteInputTokens":1,"totalTokens":17}}
                        """)
        );
        List<byte[]> chunks = List.of(
                java.util.Arrays.copyOfRange(stream, 0, 7),
                java.util.Arrays.copyOfRange(stream, 7, 43),
                java.util.Arrays.copyOfRange(stream, 43, stream.length)
        );
        List<AssistantStreamEvent> events = codec.decode(
                Multi.createFrom().iterable(chunks), model()
        ).collect().asList().await().indefinitely();
        AssistantMessage done = ((AssistantStreamEvent.Done)
                events.getLast()).message();
        assertEquals(StopReason.TOOL_USE, done.stopReason());
        assertEquals(new ToolCallContent(
                "call", "lookup", Map.of("q", "x")
        ), done.content().getFirst());
        assertEquals(17, done.usage().totalTokens());

        byte[] corrupt = frame("messageStart", "{}");
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () ->
                new AwsEventStreamDecoder().decode(corrupt));
    }

    @Test
    void mapsModeledExceptionFramesToStableTerminalErrors() {
        Map<String, String> prefixes = Map.of(
                "internalServerException", "Internal server error",
                "modelStreamErrorException", "Model stream error",
                "validationException", "Validation error",
                "throttlingException", "Throttling error",
                "serviceUnavailableException", "Service unavailable"
        );
        prefixes.forEach((eventType, prefix) -> {
            List<AssistantStreamEvent> events = codec.decode(
                    Multi.createFrom().item(frame(
                            "exception", eventType,
                            "{\"message\":\"provider detail\"}"
                    )), model()
            ).collect().asList().await().indefinitely();
            AssistantMessage error = ((AssistantStreamEvent.Error)
                    events.getLast()).message();
            assertEquals(prefix + ": provider detail", error.errorMessage());
            assertNull(error.rawStopReason());
            assertEquals(StopReason.ERROR, error.stopReason());
        });

        AssistantMessage retention = ((AssistantStreamEvent.Error) codec.decode(
                Multi.createFrom().item(frame(
                        "exception", "validationException",
                        "{\"message\":\"data retention mode default is unavailable\"}"
                )), model()
        ).collect().asList().await().indefinitely().getLast()).message();
        assertTrue(retention.errorMessage().contains(
                "userguide/data-retention.html"
        ));
    }

    @Test
    void eventBinaryValuesAreDeeplyImmutable() {
        byte[] header = {1, 2, 3};
        byte[] payload = {4, 5, 6};
        AwsEventStreamDecoder.Event event = new AwsEventStreamDecoder.Event(
                Map.of("binary", header), payload
        );
        header[0] = 9;
        payload[0] = 9;
        assertArrayEquals(new byte[]{1, 2, 3},
                (byte[]) event.headers().get("binary"));
        assertArrayEquals(new byte[]{4, 5, 6}, event.payload());

        ((byte[]) event.headers().get("binary"))[1] = 9;
        event.payload()[1] = 9;
        assertArrayEquals(new byte[]{1, 2, 3},
                (byte[]) event.headers().get("binary"));
        assertArrayEquals(new byte[]{4, 5, 6}, event.payload());
    }

    @Test
    void encodesBedrockMessagesToolsAndThinking() {
        Model model = model();
        AssistantMessage assistant = new AssistantMessage(
                List.of(
                        new ThinkingContent("why", "signature"),
                        new ToolCallContent("call.id", "lookup", Map.of("q", "x"))
                ), model.api(), model.provider(), model.id(), Usage.ZERO,
                StopReason.TOOL_USE, null, 1
        );
        var request = codec.encodeRequest(model, new ModelContext(
                "system",
                List.of(UserMessage.text("hello", 0), assistant,
                        new ToolResultMessage(
                                "call.id", "lookup",
                                List.of(new TextContent("result")),
                                Map.of(), null, false, 2
                        )),
                List.of(new ToolDefinition(
                        "lookup", "Lookup", Map.of("type", "object")
                ))
        ), "medium");
        assertEquals("system", request.path("system").get(0)
                .path("text").asText());
        assertEquals("call_id", request.path("messages").get(1)
                .path("content").get(1).path("toolUse")
                .path("toolUseId").asText());
        assertEquals("signature", request.path("messages").get(1)
                .path("content").get(0).path("reasoningContent")
                .path("reasoningText").path("signature").asText());
        assertEquals("object", request.path("toolConfig").path("tools")
                .get(0).path("toolSpec").path("inputSchema")
                .path("json").path("type").asText());
    }

    @Test
    void signsBedrockRequestDeterministically() {
        Map<String, String> signed = AwsSigV4.sign(
                java.net.URI.create("https://bedrock-runtime.us-east-1.amazonaws.com/model/x/converse-stream"),
                "POST", "{}".getBytes(StandardCharsets.UTF_8),
                Map.of("content-type", "application/json"),
                "us-east-1", "bedrock",
                new AwsCredentials("AKID", "SECRET", "TOKEN"),
                Clock.fixed(Instant.parse("2025-01-02T03:04:05Z"), ZoneOffset.UTC)
        );
        assertEquals("20250102T030405Z", signed.get("x-amz-date"));
        assertEquals("TOKEN", signed.get("x-amz-security-token"));
        assertTrue(signed.get("authorization").startsWith(
                "AWS4-HMAC-SHA256 Credential=AKID/20250102/us-east-1/bedrock/aws4_request"
        ));
    }

    private static byte[] frame(String eventType, String json) {
        return frame("event", eventType, json);
    }

    private static byte[] frame(
            String messageType, String eventType, String json
    ) {
        byte[] headers = headers(Map.of(
                ":message-type", messageType,
                ":event-type", eventType,
                ":content-type", "application/json"
        ));
        byte[] payload = json.strip().getBytes(StandardCharsets.UTF_8);
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

    private static byte[] headers(Map<String, String> headers) {
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

    private static byte[] concat(byte[]... values) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] value : values) output.writeBytes(value);
        return output.toByteArray();
    }

    private static Model model() {
        return new Model(
                "anthropic.claude", "Claude", "bedrock-converse-stream",
                "amazon-bedrock",
                "https://bedrock-runtime.us-east-1.amazonaws.com",
                true, List.of("text", "image"), 200_000, 16_384
        );
    }
}
