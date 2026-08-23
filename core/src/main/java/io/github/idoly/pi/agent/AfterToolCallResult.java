package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.ContentBlock;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Map;

/** Null fields retain the executed result; fields are replaced, never deep-merged. */
public record AfterToolCallResult(
        List<ContentBlock> content,
        Map<String, Object> details,
        Usage usage,
        Boolean error,
        Boolean terminate
) {
    public static AfterToolCallResult unchanged() {
        return new AfterToolCallResult(null, null, null, null, null);
    }
}
