package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.AssistantStreamEvent;
import io.github.idoly.pi.ai.ToolResultMessage;

import java.util.List;
import java.util.Objects;

public sealed interface AgentEvent {
    record AgentStart() implements AgentEvent { }

    record AgentEnd(List<AgentMessage> messages) implements AgentEvent {
        public AgentEnd {
            messages = List.copyOf(messages);
        }
    }

    record TurnStart() implements AgentEvent { }

    record TurnEnd(AssistantMessage message, List<ToolResultMessage> toolResults) implements AgentEvent {
        public TurnEnd {
            Objects.requireNonNull(message, "message");
            toolResults = List.copyOf(toolResults);
        }
    }

    record MessageStart(AgentMessage message) implements AgentEvent {
        public MessageStart {
            Objects.requireNonNull(message, "message");
        }
    }

    record MessageUpdate(AssistantMessage message, AssistantStreamEvent streamEvent) implements AgentEvent {
        public MessageUpdate {
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(streamEvent, "streamEvent");
        }
    }

    record MessageEnd(AgentMessage message) implements AgentEvent {
        public MessageEnd {
            Objects.requireNonNull(message, "message");
        }
    }

    record ToolExecutionStart(String toolCallId, String toolName, Object arguments) implements AgentEvent { }

    record ToolExecutionUpdate(String toolCallId, String toolName, Object arguments, Object partialResult)
            implements AgentEvent { }

    record ToolExecutionEnd(String toolCallId, String toolName, Object result, boolean error) implements AgentEvent { }
}
