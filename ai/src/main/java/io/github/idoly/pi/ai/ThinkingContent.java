package io.github.idoly.pi.ai;

import java.util.Objects;

public record ThinkingContent(String thinking, String signature) implements ContentBlock {
    public ThinkingContent {
        Objects.requireNonNull(thinking, "thinking");
    }

    public ThinkingContent(String thinking) {
        this(thinking, null);
    }
}
