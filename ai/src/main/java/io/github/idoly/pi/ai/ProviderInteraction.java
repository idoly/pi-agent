package io.github.idoly.pi.ai;

import java.util.List;
import java.util.concurrent.CompletionStage;

/** UI-neutral interactions used by provider authentication flows. */
public interface ProviderInteraction {
    CompletionStage<String> prompt(String message, boolean secret);

    CompletionStage<String> select(String message, List<Option> options);

    void openUrl(String url);

    void showDeviceCode(DeviceCode code);

    default void progress(String message) {
    }

    record Option(String id, String label) {
    }

    record DeviceCode(
            String userCode,
            String verificationUri,
            Integer intervalSeconds,
            Integer expiresInSeconds
    ) {
    }
}
