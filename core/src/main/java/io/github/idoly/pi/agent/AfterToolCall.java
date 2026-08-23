package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.CancellationSignal;
import io.github.idoly.pi.ai.ToolCallContent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface AfterToolCall {
    CompletionStage<AfterToolCallResult> apply(
            ToolCallContent toolCall,
            Map<String, Object> arguments,
            AgentToolResult result,
            boolean error,
            List<io.github.idoly.pi.ai.AgentMessage> context,
            CancellationSignal cancellation
    );
}
