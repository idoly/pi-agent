package io.github.idoly.pi.ai;

import java.util.Objects;

public record TextContent(String text) implements ContentBlock {
    public TextContent {
        Objects.requireNonNull(text, "text");
    }
}
