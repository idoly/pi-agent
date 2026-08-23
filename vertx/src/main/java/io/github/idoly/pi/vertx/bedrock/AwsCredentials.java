package io.github.idoly.pi.vertx.bedrock;

import java.util.Objects;

public record AwsCredentials(
        String accessKeyId,
        String secretAccessKey,
        String sessionToken
) {
    public AwsCredentials {
        Objects.requireNonNull(accessKeyId, "accessKeyId");
        Objects.requireNonNull(secretAccessKey, "secretAccessKey");
    }

    public static AwsCredentials fromEnvironment() {
        String access = System.getenv("AWS_ACCESS_KEY_ID");
        String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
        if (access == null || access.isBlank() || secret == null || secret.isBlank()) {
            return null;
        }
        return new AwsCredentials(
                access, secret, System.getenv("AWS_SESSION_TOKEN")
        );
    }
}
