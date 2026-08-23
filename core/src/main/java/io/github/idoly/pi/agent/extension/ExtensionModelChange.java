package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.ai.Model;

import java.util.Objects;

/** Host notification after the active model changes. */
public record ExtensionModelChange(
        Model model,
        Model previous,
        Source source
) {
    public ExtensionModelChange {
        Objects.requireNonNull(model, "model");
        Objects.requireNonNull(source, "source");
    }

    public enum Source {
        SET,
        CYCLE,
        RESTORE
    }
}
