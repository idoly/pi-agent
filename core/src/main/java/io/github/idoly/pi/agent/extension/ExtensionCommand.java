package io.github.idoly.pi.agent.extension;

import java.util.concurrent.CompletionStage;

public record ExtensionCommand(
        String name,
        String description,
        Handler handler,
        String extensionId
) {
    @FunctionalInterface
    public interface Handler {
        CompletionStage<Object> execute(String arguments, ExtensionContext context);
    }
}
