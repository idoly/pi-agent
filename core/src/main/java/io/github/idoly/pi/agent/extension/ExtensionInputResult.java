package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.ai.ImageContent;

import java.util.List;
import java.util.Objects;

/** Continue, transform, or fully handle one host input. */
public record ExtensionInputResult(
        Action action,
        String text,
        List<ImageContent> images
) {
    public ExtensionInputResult {
        action = action == null ? Action.CONTINUE : action;
        images = images == null ? List.of() : List.copyOf(images);
        if (action != Action.HANDLED) {
            Objects.requireNonNull(text, "text");
        }
    }

    public static ExtensionInputResult continueWith(ExtensionInput input) {
        return new ExtensionInputResult(
                Action.CONTINUE, input.text(), input.images()
        );
    }

    public static ExtensionInputResult transform(
            String text, List<ImageContent> images
    ) {
        return new ExtensionInputResult(Action.TRANSFORM, text, images);
    }

    public static ExtensionInputResult handled() {
        return new ExtensionInputResult(Action.HANDLED, null, List.of());
    }

    public enum Action {
        CONTINUE,
        TRANSFORM,
        HANDLED
    }
}
