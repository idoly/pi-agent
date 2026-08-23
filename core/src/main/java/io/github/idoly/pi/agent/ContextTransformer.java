package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.CancellationSignal;

import java.util.List;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface ContextTransformer {
    CompletionStage<List<AgentMessage>> transform(
            List<AgentMessage> messages,
            CancellationSignal cancellation
    );
}
