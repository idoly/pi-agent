package io.github.idoly.pi.ai;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface ConfigValueResolver {
    CompletionStage<String> resolve(String expression);
}
