package io.github.idoly.pi.ai;

import java.util.Objects;

public record TextContent(String text, String signature) implements ContentBlock {
    public TextContent(String text) {
        this(text, null);
    }

    public TextContent {
        Objects.requireNonNull(text, "text");
    }
}
