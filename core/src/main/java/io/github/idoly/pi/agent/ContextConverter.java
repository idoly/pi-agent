package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Message;

import java.util.List;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface ContextConverter {
    CompletionStage<List<Message>> convert(List<AgentMessage> messages);
}
