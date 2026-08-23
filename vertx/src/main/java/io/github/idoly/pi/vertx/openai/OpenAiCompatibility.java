package io.github.idoly.pi.vertx.openai;

import java.util.Objects;

public record OpenAiCompatibility(
        MaxTokensField maxTokensField,
        boolean supportsStreamingUsage,
        ReasoningFormat reasoningFormat,
        boolean supportsReasoningEffort,
        boolean supportsDeveloperRole,
        boolean supportsStrictMode,
        boolean supportsGrammarTools
) {
    public static final OpenAiCompatibility DEFAULT = new OpenAiCompatibility(
            MaxTokensField.MAX_COMPLETION_TOKENS,
            true,
            ReasoningFormat.STANDARD,
            true,
            true,
            true,
            false
    );

    public static final OpenAiCompatibility LEGACY = new OpenAiCompatibility(
            MaxTokensField.MAX_TOKENS,
            false,
            ReasoningFormat.NONE,
            false,
            false,
            false,
            false
    );

    public OpenAiCompatibility(
            MaxTokensField maxTokensField,
            boolean supportsStreamingUsage,
            ReasoningFormat reasoningFormat,
            boolean supportsReasoningEffort
    ) {
        this(
                maxTokensField, supportsStreamingUsage, reasoningFormat,
                supportsReasoningEffort, true, true, false
        );
    }

    public OpenAiCompatibility(
            MaxTokensField maxTokensField,
            boolean supportsStreamingUsage,
            ReasoningFormat reasoningFormat,
            boolean supportsReasoningEffort,
            boolean supportsDeveloperRole
    ) {
        this(
                maxTokensField, supportsStreamingUsage, reasoningFormat,
                supportsReasoningEffort, supportsDeveloperRole, true, false
        );
    }

    public OpenAiCompatibility(
            MaxTokensField maxTokensField,
            boolean supportsStreamingUsage,
            ReasoningFormat reasoningFormat,
            boolean supportsReasoningEffort,
            boolean supportsDeveloperRole,
            boolean supportsStrictMode
    ) {
        this(
                maxTokensField, supportsStreamingUsage, reasoningFormat,
                supportsReasoningEffort, supportsDeveloperRole, supportsStrictMode, false
        );
    }

    public OpenAiCompatibility {
        Objects.requireNonNull(maxTokensField, "maxTokensField");
        Objects.requireNonNull(reasoningFormat, "reasoningFormat");
    }

    public enum MaxTokensField {
        MAX_TOKENS,
        MAX_COMPLETION_TOKENS
    }

    public enum ReasoningFormat {
        NONE,
        STANDARD,
        QWEN,
        QWEN_CHAT_TEMPLATE,
        ZAI,
        DEEPSEEK,
        OPENROUTER,
        TOGETHER,
        STRING_THINKING
    }
}
