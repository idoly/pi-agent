import io.github.idoly.pi.agent.compaction.CompactionResult;
import io.github.idoly.pi.agent.extension.BeforeCompactionResult;
import io.github.idoly.pi.agent.extension.ExtensionCompaction;
import io.github.idoly.pi.agent.extension.ExtensionRuntime;
import io.github.idoly.pi.agent.extension.SessionTransition;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.SessionMetadata;
import io.github.idoly.pi.agent.session.SessionRepository;
import io.github.idoly.pi.agent.skill.SkillCommandDispatcher;
import io.github.idoly.pi.agent.skill.SkillInvocation;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Compile-checked orchestration patterns for an embedding application. */
public final class HeadlessExtensionHost {
    private HeadlessExtensionHost() {
    }

    public static CompletionStage<Dispatch> dispatch(
            String input,
            ExtensionRuntime extensions,
            SkillCommandDispatcher skills
    ) {
        SkillInvocation skill = skills.dispatch(input).orElse(null);
        if (skill != null) {
            // Enforce skill.allowedTools() against host policy before prompting.
            return CompletableFuture.completedFuture(
                    new Dispatch(true, skill.prompt(), skill)
            );
        }
        String normalized = input.strip();
        if (!normalized.startsWith("/") || normalized.length() == 1) {
            return CompletableFuture.completedFuture(
                    new Dispatch(false, input, null)
            );
        }
        int separator = firstWhitespace(normalized);
        String name = normalized.substring(
                1, separator < 0 ? normalized.length() : separator
        );
        boolean known = extensions.commands().stream()
                .anyMatch(command -> command.name().equals(name));
        if (!known) {
            return CompletableFuture.completedFuture(
                    new Dispatch(false, input, null)
            );
        }
        String arguments = separator < 0
                ? "" : normalized.substring(separator).strip();
        return extensions.executeCommand(name, arguments)
                .thenApply(value -> new Dispatch(true, null, value));
    }

    /**
     * The current runtime belongs to the old session. After this completes,
     * construct and start a new runtime with the returned session in context.
     */
    public static CompletionStage<AgentSession> resumeSession(
            ExtensionRuntime runtime,
            AgentSession current,
            SessionRepository repository,
            SessionMetadata target
    ) {
        Objects.requireNonNull(target, "target");
        return runtime.beforeSessionTransition(new SessionTransition(
                SessionTransition.Reason.RESUME, null, null
        )).thenCompose(decision -> {
            if (decision.cancel()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException(decision.reason() == null
                                ? "Session transition cancelled"
                                : decision.reason())
                );
            }
            return runtime.shutdownSession()
                    .thenCompose(ignored -> current.close())
                    .thenCompose(ignored -> repository.open(target));
        });
    }

    public static CompletionStage<CompactionResult> compact(
            ExtensionRuntime runtime,
            ExtensionCompaction before,
            Supplier<CompletionStage<CompactionResult>> operation
    ) {
        return runtime.beforeCompaction(before).thenCompose(decision ->
                resolveCompaction(decision, operation)
        ).thenCompose(result -> runtime.afterCompaction(
                new ExtensionCompaction(
                        before.preparation(), before.reason(),
                        before.willRetry(), result
                )
        ).thenApply(ignored -> result));
    }

    private static CompletionStage<CompactionResult> resolveCompaction(
            BeforeCompactionResult decision,
            Supplier<CompletionStage<CompactionResult>> operation
    ) {
        if (decision.cancel()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Compaction cancelled by extension")
            );
        }
        return decision.replacement() == null
                ? operation.get()
                : CompletableFuture.completedFuture(decision.replacement());
    }

    private static int firstWhitespace(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (Character.isWhitespace(value.charAt(index))) return index;
        }
        return -1;
    }

    public record Dispatch(boolean handled, String prompt, Object output) {
    }
}
