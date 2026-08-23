package io.github.idoly.pi.ai;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Framework-neutral middleware around one provider HTTP request. */
public interface ProviderRequestHooks {
    ProviderRequestHooks NONE = new ProviderRequestHooks() { };

    default CompletionStage<Map<String, String>> beforeHeaders(
            Model model,
            Map<String, String> headers,
            CancellationSignal cancellation
    ) {
        return CompletableFuture.completedFuture(Map.copyOf(headers));
    }

    /**
     * Receives a JSON-compatible tree made of maps, lists, scalar values,
     * and null. The returned value must use the same representation.
     */
    default CompletionStage<Object> beforeRequest(
            Model model,
            Object payload,
            CancellationSignal cancellation
    ) {
        return CompletableFuture.completedFuture(payload);
    }

    default CompletionStage<Void> afterResponse(
            Model model,
            int status,
            Map<String, List<String>> headers,
            CancellationSignal cancellation
    ) {
        return CompletableFuture.completedFuture(null);
    }
}
