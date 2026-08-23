package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Objects;

public record UserMessage(List<ContentBlock> content, long timestamp) implements Message {
    public UserMessage {
        content = List.copyOf(Objects.requireNonNull(content, "content"));
        if (content.stream().anyMatch(block -> !(block instanceof TextContent || block instanceof ImageContent))) {
            throw new IllegalArgumentException("User content supports only text and image blocks");
        }
    }

    public static UserMessage text(String text, long timestamp) {
        return new UserMessage(List.of(new TextContent(text)), timestamp);
    }
}
