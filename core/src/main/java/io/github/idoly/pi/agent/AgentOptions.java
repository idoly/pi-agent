package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelStream;

import java.util.List;
import java.util.Objects;

public record AgentOptions(
        String systemPrompt,
        Model model,
        String thinkingLevel,
        String sessionId,
        ModelStream modelStream,
        ContextConverter contextConverter,
        ContextTransformer contextTransformer,
        ApiKeyResolver apiKeyResolver,
        List<AgentTool> tools,
        ToolExecutionMode toolExecution,
        QueueMode steeringMode,
        QueueMode followUpMode,
        BeforeToolCall beforeToolCall,
        AfterToolCall afterToolCall,
        PrepareNextTurn prepareNextTurn,
        ShouldStopAfterTurn shouldStopAfterTurn,
        AgentMessageSupplier steeringMessages,
        AgentMessageSupplier followUpMessages
) {
    public AgentOptions {
        Objects.requireNonNull(systemPrompt, "systemPrompt");
        Objects.requireNonNull(model, "model");
        thinkingLevel = thinkingLevel == null ? "off" : thinkingLevel;
        Objects.requireNonNull(modelStream, "modelStream");
        contextConverter = contextConverter == null ? ContextConverters.standardMessages() : contextConverter;
        tools = tools == null ? List.of() : List.copyOf(tools);
        toolExecution = toolExecution == null ? ToolExecutionMode.PARALLEL : toolExecution;
        steeringMode = steeringMode == null ? QueueMode.ONE_AT_A_TIME : steeringMode;
        followUpMode = followUpMode == null ? QueueMode.ONE_AT_A_TIME : followUpMode;
    }

    public AgentOptions(
            String systemPrompt,
            Model model,
            String thinkingLevel,
            String sessionId,
            ModelStream modelStream,
            ContextConverter contextConverter,
            ContextTransformer contextTransformer,
            ApiKeyResolver apiKeyResolver,
            List<AgentTool> tools,
            ToolExecutionMode toolExecution,
            QueueMode steeringMode,
            QueueMode followUpMode,
            BeforeToolCall beforeToolCall,
            AfterToolCall afterToolCall,
            PrepareNextTurn prepareNextTurn,
            ShouldStopAfterTurn shouldStopAfterTurn
    ) {
        this(
                systemPrompt, model, thinkingLevel, sessionId, modelStream,
                contextConverter, contextTransformer, apiKeyResolver, tools, toolExecution,
                steeringMode, followUpMode, beforeToolCall, afterToolCall,
                prepareNextTurn, shouldStopAfterTurn, null, null
        );
    }

    public AgentOptions(String systemPrompt, Model model, ModelStream modelStream) {
        this(
                systemPrompt, model, "off", null, modelStream,
                null, null, null, List.of(), ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                null, null, null, null, null, null
        );
    }

    public AgentOptions(String systemPrompt, Model model, ModelStream modelStream, List<AgentTool> tools) {
        this(
                systemPrompt, model, "off", null, modelStream,
                null, null, null, tools, ToolExecutionMode.PARALLEL,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME,
                null, null, null, null, null, null
        );
    }
}
