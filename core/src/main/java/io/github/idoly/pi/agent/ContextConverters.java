package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.Message;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class ContextConverters {
    private ContextConverters() {
    }

    public static ContextConverter standardMessages() {
        return messages -> CompletableFuture.completedFuture(
                messages.stream().filter(Message.class::isInstance).map(Message.class::cast).toList()
        );
    }
}
