package io.github.idoly.pi.ai;

import java.util.Map;

/** Immutable provider operation context supplied by an embedding host. */
public record ProviderContext(
        ProviderAuth auth,
        ProviderInteraction interaction,
        CancellationSignal cancellation,
        Map<String, Object> attributes
) {
    public ProviderContext {
        auth = auth == null ? ProviderAuth.NONE : auth;
        cancellation = cancellation == null ? CancellationSignal.NONE : cancellation;
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
