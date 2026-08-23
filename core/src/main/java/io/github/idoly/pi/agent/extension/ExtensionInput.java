package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.ai.ImageContent;

import java.util.List;
import java.util.Objects;

/** One headless host input before command, skill, or prompt expansion. */
public record ExtensionInput(String text, List<ImageContent> images, Source source) {
    public ExtensionInput {
        text = Objects.requireNonNull(text, "text");
        images = images == null ? List.of() : List.copyOf(images);
        source = source == null ? Source.HOST : source;
    }

    public enum Source {
        HOST,
        RPC,
        EXTENSION
    }
}
