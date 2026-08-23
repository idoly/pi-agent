package io.github.idoly.pi.vertx.openai;

import java.util.Objects;

/** Provider-specific wire behavior for OpenAI Responses-compatible endpoints. */
public record OpenAiResponsesCompatibility(
        boolean supportsDeveloperRole,
        String offReasoningEffort,
        SessionAffinityFormat sessionAffinityFormat,
        boolean normalizeToolCallIds,
        boolean supportsStrictMode,
        boolean supportsGrammarTools
) {
    public static final OpenAiResponsesCompatibility DEFAULT = new OpenAiResponsesCompatibility(
            true, "none", SessionAffinityFormat.AUTO, true, true, false
    );

    public static final OpenAiResponsesCompatibility GITHUB_COPILOT =
            new OpenAiResponsesCompatibility(
                    true, null, SessionAffinityFormat.OPENAI, false, false, false
            );

    public OpenAiResponsesCompatibility(
            boolean supportsDeveloperRole,
            String offReasoningEffort,
            SessionAffinityFormat sessionAffinityFormat,
            boolean normalizeToolCallIds
    ) {
        this(
                supportsDeveloperRole, offReasoningEffort, sessionAffinityFormat,
                normalizeToolCallIds, true, false
        );
    }

    public OpenAiResponsesCompatibility(
            boolean supportsDeveloperRole,
            String offReasoningEffort,
            SessionAffinityFormat sessionAffinityFormat,
            boolean normalizeToolCallIds,
            boolean supportsStrictMode
    ) {
        this(
                supportsDeveloperRole, offReasoningEffort, sessionAffinityFormat,
                normalizeToolCallIds, supportsStrictMode, false
        );
    }

    public OpenAiResponsesCompatibility {
        Objects.requireNonNull(sessionAffinityFormat, "sessionAffinityFormat");
    }

    public enum SessionAffinityFormat {
        AUTO,
        OPENAI,
        OPENAI_NO_SESSION,
        OPENROUTER
    }
}
