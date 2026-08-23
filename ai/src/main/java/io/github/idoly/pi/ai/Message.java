package io.github.idoly.pi.ai;

public sealed interface Message extends AgentMessage permits UserMessage, AssistantMessage, ToolResultMessage {
}
