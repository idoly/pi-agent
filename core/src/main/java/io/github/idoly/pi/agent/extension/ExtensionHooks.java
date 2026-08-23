package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.AfterToolCallResult;
import io.github.idoly.pi.agent.AgentEvent;
import io.github.idoly.pi.agent.AgentToolResult;
import io.github.idoly.pi.agent.BeforeToolCallResult;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ToolCallContent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public final class ExtensionHooks {
    private ExtensionHooks() {
    }

    @FunctionalInterface
    public interface SessionHook {
        CompletionStage<Void> handle(ExtensionContext context);
    }

    @FunctionalInterface
    public interface ResourceDiscoveryHook {
        CompletionStage<ExtensionResources> discover(
                ExtensionResources.Reason reason, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface InputHook {
        CompletionStage<ExtensionInputResult> handle(
                ExtensionInput input, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface SessionTransitionHook {
        CompletionStage<SessionTransitionResult> before(
                SessionTransition transition, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface BeforeCompactionHook {
        CompletionStage<BeforeCompactionResult> before(
                ExtensionCompaction compaction, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface AfterCompactionHook {
        CompletionStage<Void> after(
                ExtensionCompaction compaction, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface ModelChangeHook {
        CompletionStage<Void> changed(
                ExtensionModelChange change, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface BeforeAgentStartHook {
        CompletionStage<BeforeAgentStartResult> handle(
                List<AgentMessage> prompts,
                String systemPrompt,
                ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface AgentEventHook {
        CompletionStage<Void> handle(AgentEvent event, ExtensionContext context);
    }

    @FunctionalInterface
    public interface ContextHook {
        CompletionStage<List<AgentMessage>> transform(
                List<AgentMessage> messages,
                ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface ProviderHeadersHook {
        CompletionStage<Map<String, String>> transform(
                Model model, Map<String, String> headers,
                ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface ProviderRequestHook {
        CompletionStage<Object> transform(
                Model model, Object payload, ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface ProviderResponseHook {
        CompletionStage<Void> handle(
                Model model, int status,
                Map<String, List<String>> headers,
                ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface BeforeToolHook {
        CompletionStage<BeforeToolCallResult> handle(
                ToolCallContent call,
                Map<String, Object> arguments,
                List<AgentMessage> messages,
                ExtensionContext context
        );
    }

    @FunctionalInterface
    public interface AfterToolHook {
        CompletionStage<AfterToolCallResult> handle(
                ToolCallContent call,
                Map<String, Object> arguments,
                AgentToolResult result,
                boolean error,
                List<AgentMessage> messages,
                ExtensionContext context
        );
    }
}
