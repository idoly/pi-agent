package io.github.idoly.pi.vertx;

import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SseDecoderTest {
    @Test
    void decodesUtf8EventsAcrossArbitraryBufferFragmentation() {
        SseDecoder decoder = new SseDecoder(1024);
        List<SseEvent> events = new ArrayList<>();

        byte[] bytes = "\uFEFFid: 7\r\nevent: delta\ndata: \u4f60\u597d\ndata: second\nretry: 1500\n\n"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (byte value : bytes) {
            events.addAll(decoder.decode(Buffer.buffer(new byte[]{value})));
        }

        assertEquals(1, events.size());
        SseEvent event = events.getFirst();
        assertEquals("delta", event.event());
        assertEquals("\u4f60\u597d\nsecond", event.data());
        assertEquals("7", event.id());
        assertEquals(1500L, event.retryMillis());
    }

    @Test
    void ignoresCommentsUnknownFieldsAndDataLessEvents() {
        SseDecoder decoder = new SseDecoder(1024);
        List<SseEvent> events = decoder.decode(Buffer.buffer(
                ": heartbeat\nunknown: value\nevent: ignored\n\ndata: payload\n\n"
        ));

        assertEquals(1, events.size());
        assertEquals("message", events.getFirst().event());
        assertEquals("payload", events.getFirst().data());
        assertNull(events.getFirst().id());
        assertNull(events.getFirst().retryMillis());
    }

    @Test
    void enforcesTheLineLimitAcrossBuffers() {
        SseDecoder decoder = new SseDecoder(5);
        decoder.decode(Buffer.buffer("data:"));

        assertThrows(IllegalStateException.class, () -> decoder.decode(Buffer.buffer(" value")));
    }
}
