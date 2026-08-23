package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.AgentTool;

import java.util.List;
import io.github.idoly.pi.ai.ModelProvider;

public interface ExtensionApi {
    String extensionId();

    void registerTool(AgentTool tool);

    List<AgentTool> getAllTools();

    List<String> getActiveTools();

    void setActiveTools(List<String> names);

    void registerProvider(ModelProvider provider);

    ExtensionStateStore state();

    void registerCommand(
            String name,
            String description,
            ExtensionCommand.Handler handler
    );

    void onSessionStart(ExtensionHooks.SessionHook hook);

    void onResourcesDiscover(ExtensionHooks.ResourceDiscoveryHook hook);

    void onInput(ExtensionHooks.InputHook hook);

    void onSessionTransition(ExtensionHooks.SessionTransitionHook hook);

    void onBeforeCompaction(ExtensionHooks.BeforeCompactionHook hook);

    void onAfterCompaction(ExtensionHooks.AfterCompactionHook hook);

    void onModelChange(ExtensionHooks.ModelChangeHook hook);

    void onProviderHeaders(ExtensionHooks.ProviderHeadersHook hook);

    void onProviderRequest(ExtensionHooks.ProviderRequestHook hook);

    void onProviderResponse(ExtensionHooks.ProviderResponseHook hook);

    void onSessionShutdown(ExtensionHooks.SessionHook hook);

    void onBeforeAgentStart(ExtensionHooks.BeforeAgentStartHook hook);

    void onAgentEvent(ExtensionHooks.AgentEventHook hook);

    void onContext(ExtensionHooks.ContextHook hook);

    void onBeforeTool(ExtensionHooks.BeforeToolHook hook);

    void onAfterTool(ExtensionHooks.AfterToolHook hook);

    void onEvent(String topic, ExtensionEventBus.Listener listener);

    void emit(String topic, Object value);
}
