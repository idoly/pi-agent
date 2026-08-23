package io.github.idoly.pi.vertx.bedrock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.*;
import io.github.idoly.pi.vertx.internal.ProviderHeaders;
import io.github.idoly.pi.vertx.internal.ProviderHttpHooks;
import io.github.idoly.pi.vertx.SseHttpRequest;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.smallrye.mutiny.Multi;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** AWS Bedrock ConverseStream with bearer-token or SigV4 authentication. */
public final class BedrockConverseModelStream
        implements ModelProvider, AutoCloseable {
    private final VertxSseHttpClient transport;
    private final ObjectMapper mapper;
    private final BedrockConverseCodec codec;
    private final AsyncAwsCredentialsProvider credentials;
    private final Clock clock;
    private final boolean ownsTransport;

    public BedrockConverseModelStream() {
        this(
                new VertxSseHttpClient(), new ObjectMapper(),
                AsyncAwsCredentialsProvider.from(AwsCredentials::fromEnvironment),
                Clock.systemUTC(), true
        );
    }

    public BedrockConverseModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            AwsCredentialsProvider credentials
    ) {
        this(
                transport, mapper, AsyncAwsCredentialsProvider.from(credentials),
                Clock.systemUTC(), false
        );
    }

    public static BedrockConverseModelStream withAsyncCredentials(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            AsyncAwsCredentialsProvider credentials
    ) {
        return new BedrockConverseModelStream(
                transport, mapper, credentials, Clock.systemUTC(), false
        );
    }

    BedrockConverseModelStream(
            VertxSseHttpClient transport,
            ObjectMapper mapper,
            AsyncAwsCredentialsProvider credentials,
            Clock clock,
            boolean ownsTransport
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.codec = new BedrockConverseCodec(mapper);
        this.credentials = Objects.requireNonNull(credentials, "credentials");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.ownsTransport = ownsTransport;
    }

    @Override
    public String id() {
        return "bedrock-converse-stream";
    }

    @Override
    public boolean supports(Model model) {
        return model.api().equals("bedrock-converse-stream");
    }

    @Override
    public Flow.Publisher<AssistantStreamEvent> stream(
            Model model,
            ModelContext context,
            StreamOptions options
    ) {
        if (!supports(model)) {
            return Multi.createFrom().failure(new IllegalArgumentException(
                    "Unsupported Bedrock API " + model.api()
            ));
        }
        URI uri = uri(model);
        LinkedHashMap<String, String> defaults = new LinkedHashMap<>();
        defaults.put("content-type", "application/json");
        defaults.put("accept", "application/vnd.amazon.eventstream");
        LinkedHashMap<String, String> overrides = new LinkedHashMap<>();
        options.headers().forEach((name, value) -> {
            String lower = name.toLowerCase(java.util.Locale.ROOT);
            if (!lower.equals("authorization") && !lower.equals("host")
                    && !lower.startsWith("x-amz-")) {
                overrides.put(name, value);
            }
        });
        LinkedHashMap<String, String> headers = ProviderHeaders.merge(
                defaults, overrides
        );
        String bearer = options.apiKey();
        if (bearer == null || bearer.isBlank()) {
            bearer = System.getenv("AWS_BEARER_TOKEN_BEDROCK");
        }
        boolean bearerAuth = bearer != null && !bearer.isBlank();
        if (bearerAuth) {
            headers.put("authorization", "Bearer " + bearer);
        }
        return ProviderHttpHooks.prepare(
                mapper, model,
                codec.encodeRequest(model, context, options.thinkingLevel()),
                headers, options
        ).toMulti().onItem().transformToMultiAndConcatenate(prepared -> {
            byte[] body;
            try {
                body = mapper.writeValueAsBytes(prepared.payload());
            } catch (JsonProcessingException failure) {
                return Multi.createFrom().failure(failure);
            }
            if (bearerAuth) {
                return execute(
                        uri, body, prepared.headers(), model, options
                );
            }
            return io.smallrye.mutiny.Uni.createFrom().completionStage(
                    () -> resolveCredentials(model, options)
            ).toMulti().onItem().transformToMultiAndConcatenate(resolved -> {
                if (resolved == null) {
                    return Multi.createFrom().failure(
                            new IllegalArgumentException(
                                    "No AWS credentials or Bedrock bearer token"
                            )
                    );
                }
                LinkedHashMap<String, String> unsigned = new LinkedHashMap<>();
                prepared.headers().forEach((name, value) -> {
                    String lower = name.toLowerCase(java.util.Locale.ROOT);
                    if (!lower.equals("authorization")
                            && !lower.equals("host")
                            && !lower.startsWith("x-amz-")) {
                        unsigned.put(name, value);
                    }
                });
                Map<String, String> signed = AwsSigV4.sign(
                        uri, "POST", body, unsigned,
                        region(model), "bedrock", resolved, clock
                );
                return execute(uri, body, signed, model, options);
            });
        });
    }

    private Multi<AssistantStreamEvent> execute(
            URI uri,
            byte[] body,
            Map<String, String> headers,
            Model model,
            StreamOptions options
    ) {
        return ProviderHttpHooks.observeBinary(transport.executeBinary(
                SseHttpRequest.post(uri, headers, body),
                options.cancellation()
        ), model, options).toMulti()
                .onItem().transformToMultiAndConcatenate(response ->
                        codec.decode(response.chunks(), model)
                );
    }

    private CompletionStage<AwsCredentials> resolveCredentials(
            Model model,
            StreamOptions options
    ) {
        try {
            CompletionStage<AwsCredentials> stage = credentials.resolve(
                    model, options.cancellation()
            );
            return stage == null ? CompletableFuture.failedFuture(
                    new NullPointerException(
                            "AsyncAwsCredentialsProvider returned null stage"
                    )
            ) : stage;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    static URI uri(Model model) {
        String normalized = model.baseUrl().endsWith("/")
                ? model.baseUrl().substring(0, model.baseUrl().length() - 1)
                : model.baseUrl();
        String id = URLEncoder.encode(
                model.id(), StandardCharsets.UTF_8
        ).replace("+", "%20");
        return URI.create(normalized + "/model/" + id + "/converse-stream");
    }

    static String region(Model model) {
        java.util.regex.Matcher arn = java.util.regex.Pattern.compile(
                "^arn:aws(?:-[a-z0-9-]+)?:bedrock:([a-z0-9-]+):"
        ).matcher(model.id());
        if (arn.find()) return arn.group(1);
        String configured = System.getenv("AWS_REGION");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("AWS_DEFAULT_REGION");
        }
        if (configured != null && !configured.isBlank()) return configured;
        java.util.regex.Matcher endpoint = java.util.regex.Pattern.compile(
                "bedrock-runtime[.]([a-z0-9-]+)[.]amazonaws[.]com"
        ).matcher(model.baseUrl());
        return endpoint.find() ? endpoint.group(1) : "us-east-1";
    }

    @Override
    public java.util.concurrent.CompletionStage<List<Model>> models(
            ProviderContext context
    ) {
        return java.util.concurrent.CompletableFuture.completedFuture(List.of());
    }

    @Override
    public void close() {
        if (ownsTransport) transport.close();
    }
}
