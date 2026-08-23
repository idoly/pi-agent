package io.github.idoly.pi.agent.extension;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public record ExtensionCommand(
        String name,
        String description,
        Handler handler,
        String extensionId
) {
    public ExtensionCommand {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("command name must not be blank");
        }
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(extensionId, "extensionId");
    }

    @FunctionalInterface
    public interface Handler {
        CompletionStage<Object> execute(String arguments, ExtensionContext context);
    }
}
