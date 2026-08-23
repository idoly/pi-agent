package io.github.idoly.pi.vertx.bedrock;

import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.Model;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Asynchronously resolves short-lived AWS credentials for one model request. */
@FunctionalInterface
public interface AsyncAwsCredentialsProvider {
    CompletionStage<AwsCredentials> resolve(
            Model model,
            CancellationSignal cancellation
    );

    static AsyncAwsCredentialsProvider from(
            AwsCredentialsProvider provider
    ) {
        Objects.requireNonNull(provider, "provider");
        return (model, cancellation) -> {
            try {
                return CompletableFuture.completedFuture(provider.resolve());
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
        };
    }
}
