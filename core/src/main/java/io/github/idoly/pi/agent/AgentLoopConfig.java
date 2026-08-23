package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelStream;

import java.util.Objects;

/** Runtime configuration for one low-level agent-loop invocation. */
public record AgentLoopConfig(
        Model model,
        String thinkingLevel,
        String sessionId,
        ModelStream modelStream,
        ContextConverter contextConverter,
        ContextTransformer contextTransformer,
        ApiKeyResolver apiKeyResolver,
        ToolExecutionMode toolExecution,
        BeforeToolCall beforeToolCall,
        AfterToolCall afterToolCall,
        PrepareNextTurn prepareNextTurn,
        ShouldStopAfterTurn shouldStopAfterTurn,
        AgentMessageSupplier steeringMessages,
        AgentMessageSupplier followUpMessages
) {
    public AgentLoopConfig {
        Objects.requireNonNull(model, "model");
        thinkingLevel = thinkingLevel == null ? "off" : thinkingLevel;
        Objects.requireNonNull(modelStream, "modelStream");
        contextConverter = contextConverter == null
                ? ContextConverters.standardMessages()
                : contextConverter;
        toolExecution = toolExecution == null ? ToolExecutionMode.PARALLEL : toolExecution;
    }

    public AgentLoopConfig(Model model, ModelStream modelStream) {
        this(
                model, "off", null, modelStream, null, null, null,
                ToolExecutionMode.PARALLEL, null, null, null, null, null, null
        );
    }
}
