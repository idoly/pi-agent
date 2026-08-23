package io.github.idoly.pi.vertx.bedrock;

@FunctionalInterface
public interface AwsCredentialsProvider {
    AwsCredentials resolve();
}
