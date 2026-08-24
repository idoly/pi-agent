package io.github.idoly.pi.vertx.bedrock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelContext;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.UserMessage;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BedrockConverseModelStreamTest {
    @Test
    void asynchronouslyResolvesCredentialsBeforeSending() throws Exception {
        CompletableFuture<AwsCredentials> credentials = new CompletableFuture<>();
        CompletableFuture<Void> invoked = new CompletableFuture<>();
        AtomicReference<Model> resolvedModel = new AtomicReference<>();
        AtomicReference<CancellationSignal> resolvedCancellation =
                new AtomicReference<>();
        AsyncAwsCredentialsProvider provider = (model, cancellation) -> {
            resolvedModel.set(model);
            resolvedCancellation.set(cancellation);
            invoked.complete(null);
            return credentials;
        };
        Model model = model();
        StreamOptions options = new StreamOptions(
                "session", null, "off", CancellationSignal.NONE
        );

        try (VertxSseHttpClient transport = new VertxSseHttpClient()) {
            BedrockConverseModelStream stream =
                    BedrockConverseModelStream.withAsyncCredentials(
                            transport, new ObjectMapper(), provider
                    );
            CompletableFuture<List<AssistantStreamEvent>> result =
                    Multi.createFrom().publisher(stream.stream(
                            model,
                            new ModelContext(
                                    "", List.of(UserMessage.text("hello", 1))
                            ),
                            options
                    )).collect().asList().subscribeAsCompletionStage()
                            .toCompletableFuture();

            invoked.get(3, TimeUnit.SECONDS);
            assertSame(model, resolvedModel.get());
            assertSame(options.cancellation(), resolvedCancellation.get());
            assertFalse(result.isDone());

            credentials.completeExceptionally(
                    new IllegalStateException("credential refresh failed")
            );
            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> result.get(3, TimeUnit.SECONDS)
            );
            assertEquals("credential refresh failed",
                    rootCause(failure).getMessage());
            stream.close();
        }
    }

    @Test
    void cancellationWhileCredentialsArePendingPreventsLaterDispatch()
            throws Exception {
        CompletableFuture<AwsCredentials> credentials = new CompletableFuture<>();
        TestCancellation cancellation = new TestCancellation();
        AtomicInteger resolutions = new AtomicInteger();
        try (VertxSseHttpClient transport = new VertxSseHttpClient()) {
            BedrockConverseModelStream stream =
                    BedrockConverseModelStream.withAsyncCredentials(
                            transport, new ObjectMapper(),
                            (model, signal) -> {
                                resolutions.incrementAndGet();
                                return credentials;
                            }
                    );
            CompletableFuture<List<AssistantStreamEvent>> result =
                    collect(stream, null, cancellation);
            await(() -> resolutions.get() == 1);

            cancellation.cancel();
            java.util.concurrent.CancellationException failure = assertThrows(
                    java.util.concurrent.CancellationException.class,
                    () -> result.get(3, TimeUnit.SECONDS)
            );
            assertEquals(
                    "AWS credential resolution cancelled",
                    rootCause(failure).getMessage()
            );
            credentials.complete(new AwsCredentials("access", "secret", null));
            assertEquals(1, resolutions.get());
            stream.close();
        }
    }

    @Test
    void alreadyCancelledRequestsDoNotInvokeCredentialResolution()
            throws Exception {
        TestCancellation cancellation = new TestCancellation();
        cancellation.cancel();
        AtomicInteger resolutions = new AtomicInteger();
        try (VertxSseHttpClient transport = new VertxSseHttpClient()) {
            BedrockConverseModelStream stream =
                    BedrockConverseModelStream.withAsyncCredentials(
                            transport, new ObjectMapper(),
                            (model, signal) -> {
                                resolutions.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            }
                    );
            CompletableFuture<List<AssistantStreamEvent>> result = collect(
                    stream, null, cancellation
            );
            assertThrows(
                    java.util.concurrent.CancellationException.class,
                    () -> result.get(3, TimeUnit.SECONDS)
            );
            assertEquals(0, resolutions.get());
            stream.close();
        }
    }

    @Test
    void bearerAuthenticationBypassesAwsCredentialResolution()
            throws Exception {
        AtomicInteger resolutions = new AtomicInteger();
        try (VertxSseHttpClient transport = new VertxSseHttpClient()) {
            BedrockConverseModelStream stream =
                    BedrockConverseModelStream.withAsyncCredentials(
                            transport, new ObjectMapper(),
                            (model, cancellation) -> {
                                resolutions.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            }
                    );
            CompletableFuture<List<AssistantStreamEvent>> result = collect(
                    stream, "bedrock-bearer", CancellationSignal.NONE
            );
            assertThrows(
                    ExecutionException.class,
                    () -> result.get(3, TimeUnit.SECONDS)
            );
            assertEquals(0, resolutions.get());
            stream.close();
        }
    }

    @Test
    void adaptsSynchronousCredentialProviders() throws Exception {
        AwsCredentials expected = new AwsCredentials("access", "secret", "token");
        AwsCredentials actual = AsyncAwsCredentialsProvider
                .from(() -> expected)
                .resolve(model(), CancellationSignal.NONE)
                .toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertEquals(expected, actual);
    }

    @Test
    void rejectsANullCredentialStage() throws Exception {
        try (VertxSseHttpClient transport = new VertxSseHttpClient()) {
            BedrockConverseModelStream stream =
                    BedrockConverseModelStream.withAsyncCredentials(
                            transport, new ObjectMapper(),
                            (model, cancellation) -> null
                    );
            CompletableFuture<List<AssistantStreamEvent>> result =
                    Multi.createFrom().publisher(stream.stream(
                            model(),
                            new ModelContext(
                                    "", List.of(UserMessage.text("hello", 1))
                            ),
                            new StreamOptions(
                                    "session", null, "off",
                                    CancellationSignal.NONE
                            )
                    )).collect().asList().subscribeAsCompletionStage()
                            .toCompletableFuture();

            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> result.get(3, TimeUnit.SECONDS)
            );
            assertEquals(
                    "AsyncAwsCredentialsProvider returned null stage",
                    rootCause(failure).getMessage()
            );
            stream.close();
        }
    }

    private static CompletableFuture<List<AssistantStreamEvent>> collect(
            BedrockConverseModelStream stream,
            String bearer,
            CancellationSignal cancellation
    ) {
        return Multi.createFrom().publisher(stream.stream(
                model(),
                new ModelContext(
                        "", List.of(UserMessage.text("hello", 1))
                ),
                new StreamOptions(
                        "session", bearer, "off", cancellation
                )
        )).collect().asList().subscribeAsCompletionStage()
                .toCompletableFuture();
    }

    private static void await(java.util.function.BooleanSupplier condition)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition was not met");
            }
            Thread.sleep(10);
        }
    }

    private static Model model() {
        return new Model(
                "anthropic.claude-fixture", "Bedrock fixture",
                "bedrock-converse-stream", "amazon-bedrock",
                "http://127.0.0.1:1", false,
                List.of("text"), 200_000, 4_096
        );
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current;
    }

    private static final class TestCancellation implements CancellationSignal {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final CopyOnWriteArrayList<Runnable> callbacks =
                new CopyOnWriteArrayList<>();

        private void cancel() {
            if (!cancelled.compareAndSet(false, true)) return;
            callbacks.forEach(Runnable::run);
            callbacks.clear();
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void throwIfCancelled() {
            if (isCancelled()) {
                throw new java.util.concurrent.CancellationException(
                        "Operation cancelled"
                );
            }
        }

        @Override
        public AutoCloseable onCancel(Runnable callback) {
            if (isCancelled()) {
                callback.run();
                return () -> { };
            }
            callbacks.add(callback);
            if (isCancelled() && callbacks.remove(callback)) callback.run();
            return () -> callbacks.remove(callback);
        }
    }
}
