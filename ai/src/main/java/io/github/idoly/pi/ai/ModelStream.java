package io.github.idoly.pi.ai;

import java.util.concurrent.Flow;

@FunctionalInterface
public interface ModelStream {
    Flow.Publisher<AssistantStreamEvent> stream(Model model, ModelContext context, StreamOptions options);
}
