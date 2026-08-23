package io.github.idoly.pi.ai;

public sealed interface ContentBlock permits TextContent, ImageContent, ThinkingContent, ToolCallContent {
}
