import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.agent.Agent;
import io.github.idoly.pi.agent.AgentEvent;
import io.github.idoly.pi.agent.AgentOptions;
import io.github.idoly.pi.agent.ApiKeyResolver;
import io.github.idoly.pi.agent.extension.ExtensionContext;
import io.github.idoly.pi.agent.extension.ExtensionLoadOptions;
import io.github.idoly.pi.agent.extension.ExtensionLoader;
import io.github.idoly.pi.agent.extension.ExtensionResources;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.JsonlSessionRepository;
import io.github.idoly.pi.agent.session.SessionRepository;
import io.github.idoly.pi.agent.skill.SkillCommandDispatcher;
import io.github.idoly.pi.agent.skill.SkillDiscoveryOptions;
import io.github.idoly.pi.agent.skill.SkillInvocation;
import io.github.idoly.pi.agent.skill.SkillRegistry;
import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.vertx.ProviderModelCatalog;
import io.github.idoly.pi.vertx.VertxModelProviders;
import io.github.idoly.pi.vertx.VertxSseHttpClient;
import io.github.idoly.pi.vertx.bedrock.AsyncAwsCredentialsProvider;
import io.github.idoly.pi.vertx.bedrock.AwsCredentials;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Compile-checked lifecycle for a complete headless pi-agent host. */
public final class HeadlessAgentRuntime implements AutoCloseable {
    private final VertxSseHttpClient transport;
    private final VertxModelProviders providers;
    private final AgentSession session;
    private final ExtensionLoader.LoadedRuntime extensions;
    private final Agent agent;
    private final SkillCommandDispatcher skills;
    private final AutoCloseable sessionWriter;

    private HeadlessAgentRuntime(
            VertxSseHttpClient transport,
            VertxModelProviders providers,
            AgentSession session,
            ExtensionLoader.LoadedRuntime extensions,
            Agent agent,
            SkillCommandDispatcher skills,
            AutoCloseable sessionWriter
    ) {
        this.transport = transport;
        this.providers = providers;
        this.session = session;
        this.extensions = extensions;
        this.agent = agent;
        this.skills = skills;
        this.sessionWriter = sessionWriter;
    }

    public static CompletionStage<HeadlessAgentRuntime> open(
            Path cwd,
            Path sessionsRoot,
            String provider,
            String modelId,
            ApiKeyResolver apiKeys,
            AsyncAwsCredentialsProvider awsCredentials,
            boolean projectTrusted
    ) {
        Objects.requireNonNull(apiKeys, "apiKeys");
        Path normalized = cwd.toAbsolutePath().normalize();
        VertxSseHttpClient transport = new VertxSseHttpClient();
        VertxModelProviders providers =
                VertxModelProviders.withAsyncAwsCredentials(
                        transport,
                        new ObjectMapper(),
                        ProviderModelCatalog.bundled(),
                        awsCredentials == null
                                ? environmentAwsCredentials()
                                : awsCredentials
                );
        Model model;
        try {
            model = providers.catalog().find(provider, modelId)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Unknown model " + provider + '/' + modelId
                    )).model();
        } catch (Throwable failure) {
            providers.close();
            transport.close();
            return CompletableFuture.failedFuture(failure);
        }
        JsonlSessionRepository repository = new JsonlSessionRepository(
                sessionsRoot, normalized
        );
        return repository.create(SessionRepository.CreateOptions.DEFAULT)
                .thenCompose(session -> loadExtensions(
                        normalized, projectTrusted, providers, session
                ).thenCompose(loaded -> {
                    var runtime = loaded.runtime();
                    return runtime.startSession().thenCompose(ignored ->
                            runtime.discoverSkills(
                                    new SkillDiscoveryOptions(
                                            Path.of(System.getProperty("user.home")),
                                            normalized, projectTrusted, true,
                                            java.util.List.of(),
                                            java.util.List.of()
                                    ),
                                    ExtensionResources.Reason.STARTUP
                            )
                    ).thenCompose(registry -> session.metadata().thenApply(
                            metadata -> create(
                                    transport, providers, session, loaded,
                                    registry, model, metadata.id(), apiKeys
                            )
                    ));
                }));
    }

    private static CompletionStage<ExtensionLoader.LoadedRuntime> loadExtensions(
            Path cwd,
            boolean projectTrusted,
            VertxModelProviders providers,
            AgentSession session
    ) {
        ExtensionContext context = new ExtensionContext(
                cwd, session, providers.registry(),
                CancellationSignal.NONE, java.util.Map.of()
        );
        return ExtensionLoader.load(
                context,
                new ExtensionLoadOptions(
                        Path.of(System.getProperty("user.home")), cwd,
                        projectTrusted, true, java.util.List.of()
                )
        );
    }

    private static HeadlessAgentRuntime create(
            VertxSseHttpClient transport,
            VertxModelProviders providers,
            AgentSession session,
            ExtensionLoader.LoadedRuntime loaded,
            SkillRegistry registry,
            Model model,
            String sessionId,
            ApiKeyResolver apiKeys
    ) {
        String prompt = registry.contributeToSystemPrompt(
                "You are a headless JVM agent."
        );
        AgentOptions options = new AgentOptions(
                prompt, model, "off", sessionId, providers,
                null, null, apiKeys, java.util.List.of(), null,
                null, null, null, null, null, null
        );
        Agent agent = loaded.runtime().createAgent(options);
        AutoCloseable writer = agent.subscribe((event, cancellation) ->
                event instanceof AgentEvent.MessageEnd end
                        ? session.appendMessage(end.message()).thenApply(id -> null)
                        : CompletableFuture.completedFuture(null)
        );
        return new HeadlessAgentRuntime(
                transport, providers, session, loaded, agent,
                new SkillCommandDispatcher(registry), writer
        );
    }

    public CompletionStage<Void> prompt(String input) {
        SkillInvocation skill = skills.dispatch(input).orElse(null);
        String effective = skill == null ? input : skill.prompt();
        // Enforce skill.allowedTools() in host policy before this call.
        return agent.prompt(effective);
    }

    public Agent agent() {
        return agent;
    }

    public AgentSession session() {
        return session;
    }

    @Override
    public void close() {
        agent.abort();
        agent.waitForIdle().toCompletableFuture().join();
        closeQuietly(sessionWriter);
        extensions.runtime().shutdownSession().toCompletableFuture().join();
        extensions.close();
        session.close().toCompletableFuture().join();
        providers.close();
        transport.close();
    }

    private static AsyncAwsCredentialsProvider environmentAwsCredentials() {
        return (model, cancellation) -> CompletableFuture.completedFuture(
                AwsCredentials.fromEnvironment()
        );
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception failure) {
            throw new IllegalStateException("Unable to close listener", failure);
        }
    }
}
