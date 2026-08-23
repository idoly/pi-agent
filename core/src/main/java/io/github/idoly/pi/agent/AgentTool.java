package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.ToolDefinition;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface AgentTool {
    ToolDefinition definition();

    default String name() {
        return definition().name();
    }

    default ToolExecutionMode executionMode() {
        return ToolExecutionMode.PARALLEL;
    }

    default Map<String, Object> prepareArguments(Map<String, Object> arguments) {
        return arguments;
    }

    default void validateArguments(Map<String, Object> arguments) {
    }

    CompletionStage<AgentToolResult> execute(
            String toolCallId,
            Map<String, Object> arguments,
            CancellationSignal cancellation,
            Consumer<AgentToolResult> onUpdate
    );
}
