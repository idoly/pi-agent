package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;

import java.util.List;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface AgentMessageSupplier {
    CompletionStage<List<AgentMessage>> get();
}
