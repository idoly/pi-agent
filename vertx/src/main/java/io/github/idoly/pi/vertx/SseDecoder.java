package io.github.idoly.pi.vertx;

import io.vertx.core.buffer.Buffer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Stateful incremental SSE decoder. One instance must be used for exactly one response stream. */
public final class SseDecoder {
    private final int maxLineLength;
    private final ByteArrayOutputStream line = new ByteArrayOutputStream();
    private final StringBuilder data = new StringBuilder();
    private boolean pendingCarriageReturn;
    private boolean firstLine = true;
    private boolean hasData;
    private String event;
    private String lastEventId;
    private Long retryMillis;

    public SseDecoder(int maxLineLength) {
        if (maxLineLength < 1) {
            throw new IllegalArgumentException("maxLineLength must be positive");
        }
        this.maxLineLength = maxLineLength;
    }

    public List<SseEvent> decode(Buffer buffer) {
        List<SseEvent> output = new ArrayList<>();
        byte[] bytes = buffer.getBytes();
        for (byte value : bytes) {
            int unsigned = value & 0xff;
            if (pendingCarriageReturn) {
                emitLine(output);
                pendingCarriageReturn = false;
                if (unsigned == '\n') {
                    continue;
                }
            }
            if (unsigned == '\r') {
                pendingCarriageReturn = true;
            } else if (unsigned == '\n') {
                emitLine(output);
            } else {
                if (line.size() >= maxLineLength) {
                    throw new IllegalStateException(
                            "SSE line exceeded " + maxLineLength + " bytes"
                    );
                }
                line.write(unsigned);
            }
        }
        return output;
    }

    private void emitLine(List<SseEvent> output) {
        String value = decodeUtf8(line.toByteArray());
        line.reset();
        if (firstLine) {
            firstLine = false;
            if (!value.isEmpty() && value.charAt(0) == '\uFEFF') {
                value = value.substring(1);
            }
        }
        if (value.isEmpty()) {
            dispatch(output);
            return;
        }
        if (value.charAt(0) == ':') {
            return;
        }

        int colon = value.indexOf(':');
        String field = colon < 0 ? value : value.substring(0, colon);
        String fieldValue = colon < 0 ? "" : value.substring(colon + 1);
        if (fieldValue.startsWith(" ")) {
            fieldValue = fieldValue.substring(1);
        }
        switch (field) {
            case "event" -> event = fieldValue;
            case "data" -> {
                data.append(fieldValue).append('\n');
                hasData = true;
            }
            case "id" -> {
                if (fieldValue.indexOf('\0') < 0) {
                    lastEventId = fieldValue;
                }
            }
            case "retry" -> parseRetry(fieldValue);
            default -> {
                // Unknown fields and comment lines have no stream semantics.
            }
        }
    }

    private void parseRetry(String value) {
        try {
            if (!value.isEmpty() && value.chars().allMatch(Character::isDigit)) {
                retryMillis = Long.parseLong(value);
            }
        } catch (NumberFormatException ignored) {
            // Retry values outside the long range are invalid.
        }
    }

    private void dispatch(List<SseEvent> output) {
        if (hasData) {
            data.setLength(data.length() - 1);
            output.add(new SseEvent(
                    event == null || event.isEmpty() ? "message" : event,
                    data.toString(),
                    lastEventId,
                    retryMillis
            ));
        }
        data.setLength(0);
        event = null;
        retryMillis = null;
        hasData = false;
    }

    private static String decodeUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("SSE line is not valid UTF-8", failure);
        }
    }
}
