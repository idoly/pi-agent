package io.github.idoly.pi.ai;

import java.util.Objects;

public record ImageContent(String data, String mimeType) implements ContentBlock {
    public ImageContent {
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(mimeType, "mimeType");
    }
}
