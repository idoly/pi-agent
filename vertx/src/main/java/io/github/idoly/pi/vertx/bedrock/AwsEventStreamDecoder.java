package io.github.idoly.pi.vertx.bedrock;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/** Incremental AWS Smithy event-stream frame decoder with CRC validation. */
public final class AwsEventStreamDecoder {
    private static final int MIN_FRAME = 16;
    private static final int MAX_FRAME = 16 * 1024 * 1024;
    private byte[] pending = new byte[0];

    public List<Event> decode(byte[] chunk) {
        byte[] combined = new byte[pending.length + chunk.length];
        System.arraycopy(pending, 0, combined, 0, pending.length);
        System.arraycopy(chunk, 0, combined, pending.length, chunk.length);
        ArrayList<Event> events = new ArrayList<>();
        int offset = 0;
        while (combined.length - offset >= 4) {
            int totalLength = int32(combined, offset);
            if (totalLength < MIN_FRAME || totalLength > MAX_FRAME) {
                throw new IllegalArgumentException(
                        "Invalid AWS event-stream frame length " + totalLength
                );
            }
            if (combined.length - offset < totalLength) break;
            events.add(frame(combined, offset, totalLength));
            offset += totalLength;
        }
        pending = java.util.Arrays.copyOfRange(combined, offset, combined.length);
        return List.copyOf(events);
    }

    public void finish() {
        if (pending.length != 0) {
            throw new IllegalStateException(
                    "AWS event stream ended with " + pending.length
                            + " incomplete bytes"
            );
        }
    }

    private static Event frame(byte[] bytes, int offset, int totalLength) {
        int headerLength = int32(bytes, offset + 4);
        if (headerLength < 0 || headerLength > totalLength - MIN_FRAME) {
            throw new IllegalArgumentException(
                    "Invalid AWS event-stream header length " + headerLength
            );
        }
        verifyCrc(bytes, offset, 8, int32(bytes, offset + 8), "prelude");
        verifyCrc(
                bytes, offset, totalLength - 4,
                int32(bytes, offset + totalLength - 4), "message"
        );
        int headerStart = offset + 12;
        int payloadStart = headerStart + headerLength;
        Map<String, Object> headers = headers(
                bytes, headerStart, payloadStart
        );
        byte[] payload = java.util.Arrays.copyOfRange(
                bytes, payloadStart, offset + totalLength - 4
        );
        return new Event(headers, payload);
    }

    private static Map<String, Object> headers(
            byte[] bytes,
            int start,
            int end
    ) {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        ByteBuffer input = ByteBuffer.wrap(bytes, start, end - start)
                .order(ByteOrder.BIG_ENDIAN);
        while (input.hasRemaining()) {
            int nameLength = Byte.toUnsignedInt(input.get());
            require(input.remaining() >= nameLength + 1, "truncated header name");
            byte[] name = new byte[nameLength];
            input.get(name);
            int type = Byte.toUnsignedInt(input.get());
            Object value = switch (type) {
                case 0 -> true;
                case 1 -> false;
                case 2 -> input.get();
                case 3 -> input.getShort();
                case 4 -> input.getInt();
                case 5 -> input.getLong();
                case 6 -> bytes(input);
                case 7 -> new String(bytes(input), StandardCharsets.UTF_8);
                case 8 -> input.getLong();
                case 9 -> {
                    byte[] uuid = new byte[16];
                    input.get(uuid);
                    yield uuid;
                }
                default -> throw new IllegalArgumentException(
                        "Unknown AWS event-stream header type " + type
                );
            };
            result.put(new String(name, StandardCharsets.UTF_8), value);
        }
        return Map.copyOf(result);
    }

    private static byte[] bytes(ByteBuffer input) {
        require(input.remaining() >= 2, "truncated header length");
        int length = Short.toUnsignedInt(input.getShort());
        require(input.remaining() >= length, "truncated header value");
        byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static void verifyCrc(
            byte[] bytes,
            int offset,
            int length,
            int expected,
            String label
    ) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        if ((int) crc.getValue() != expected) {
            throw new IllegalArgumentException(
                    "Invalid AWS event-stream " + label + " CRC"
            );
        }
    }

    private static int int32(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, 4)
                .order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    public record Event(Map<String, Object> headers, byte[] payload) {
        public Event {
            headers = copyHeaders(headers);
            payload = payload.clone();
        }

        @Override
        public Map<String, Object> headers() {
            return copyHeaders(headers);
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }

        public String header(String name) {
            Object value = headers.get(name);
            return value == null ? null : String.valueOf(value);
        }

        private static Map<String, Object> copyHeaders(
                Map<String, Object> source
        ) {
            LinkedHashMap<String, Object> copied = new LinkedHashMap<>();
            source.forEach((name, value) -> copied.put(
                    name, value instanceof byte[] bytes
                            ? bytes.clone() : value
            ));
            return Map.copyOf(copied);
        }
    }
}
