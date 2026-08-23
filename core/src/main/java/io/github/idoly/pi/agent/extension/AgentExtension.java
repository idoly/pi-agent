package io.github.idoly.pi.agent.extension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Native headless Java extension loaded by an ExtensionRuntime. */
public interface AgentExtension {
    String id();

    default int order() {
        return 0;
    }

    default CompletionStage<Void> initialize(ExtensionApi api) {
        configure(api);
        return CompletableFuture.completedFuture(null);
    }

    default void configure(ExtensionApi api) {
    }
}
