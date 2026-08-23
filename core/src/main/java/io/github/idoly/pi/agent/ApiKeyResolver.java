package io.github.idoly.pi.agent;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface ApiKeyResolver {
    CompletionStage<String> resolve(String provider);
}
