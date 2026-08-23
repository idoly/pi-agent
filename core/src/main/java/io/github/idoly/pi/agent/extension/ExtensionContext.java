package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.ProviderRegistry;

import java.nio.file.Path;
import java.util.Map;

/** Headless host context supplied to Java extension hooks. */
public record ExtensionContext(
        Path cwd,
        AgentSession session,
        ProviderRegistry providers,
        CancellationSignal cancellation,
        Map<String, Object> attributes
) {
    public ExtensionContext {
        cwd = cwd.toAbsolutePath().normalize();
        cancellation = cancellation == null
                ? CancellationSignal.NONE : cancellation;
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public ExtensionContext withCancellation(CancellationSignal value) {
        return new ExtensionContext(cwd, session, providers, value, attributes);
    }
}
