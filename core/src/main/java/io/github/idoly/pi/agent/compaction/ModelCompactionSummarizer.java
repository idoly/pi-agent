package io.github.idoly.pi.agent.compaction;

import io.github.idoly.pi.agent.AgentContext;
import io.github.idoly.pi.agent.AgentLoop;
import io.github.idoly.pi.agent.AgentLoopConfig;
import io.github.idoly.pi.agent.ApiKeyResolver;
import io.github.idoly.pi.agent.ContextConverter;
import io.github.idoly.pi.agent.ContextConverters;
import io.github.idoly.pi.agent.session.SessionContextConverters;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.Message;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.ModelStream;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.TextContent;
import io.github.idoly.pi.ai.UserMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public final class ModelCompactionSummarizer implements CompactionSummarizer {
    public static final String SYSTEM_PROMPT = """
            You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

            Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.""";

    private static final String HISTORY_PROMPT = """
            The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work.

            Use this EXACT format:

            ## Goal
            [What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

            ## Constraints & Preferences
            - [Any constraints, preferences, or requirements mentioned by user]
            - [Or "(none)" if none were mentioned]

            ## Progress
            ### Done
            - [x] [Completed tasks/changes]

            ### In Progress
            - [ ] [Current work]

            ### Blocked
            - [Issues preventing progress, if any]

            ## Key Decisions
            - **[Decision]**: [Brief rationale]

            ## Next Steps
            1. [Ordered list of what should happen next]

            ## Critical Context
            - [Any data, examples, or references needed to continue]
            - [Or "(none)" if not applicable]

            Keep each section concise. Preserve exact file paths, function names, and error messages.""";

    private static final String UPDATE_PROMPT = """
            The messages above are NEW conversation messages to incorporate into the existing summary provided in <previous-summary> tags.

            Update the existing structured summary with new information. RULES:
            - PRESERVE all existing information from the previous summary
            - ADD new progress, decisions, and context from the new messages
            - UPDATE the Progress section: move items from "In Progress" to "Done" when completed
            - UPDATE "Next Steps" based on what was accomplished
            - PRESERVE exact file paths, function names, and error messages
            - If something is no longer relevant, you may remove it

            Use this EXACT format:

            ## Goal
            [Preserve existing goals, add new ones if the task expanded]

            ## Constraints & Preferences
            - [Preserve existing, add new ones discovered]

            ## Progress
            ### Done
            - [x] [Include previously done items AND newly completed items]

            ### In Progress
            - [ ] [Current work - update based on progress]

            ### Blocked
            - [Current blockers - remove if resolved]

            ## Key Decisions
            - **[Decision]**: [Brief rationale] (preserve all previous, add new)

            ## Next Steps
            1. [Update based on current state]

            ## Critical Context
            - [Preserve important context, add new if needed]

            Keep each section concise. Preserve exact file paths, function names, and error messages.""";

    private static final String TURN_PREFIX_PROMPT = """
            This is the PREFIX of a turn that was too large to keep. The SUFFIX (recent work) is retained.

            Summarize the prefix to provide context for the retained suffix:

            ## Original Request
            [What did the user ask for in this turn?]

            ## Early Progress
            - [Key decisions and work done in the prefix]

            ## Context for Suffix
            - [Information needed to understand the retained recent work]

            Be concise. Focus on what's needed to understand the kept suffix.""";

    private static final String BRANCH_PROMPT = """
            Create a structured summary of this conversation branch for context when returning later.

            Use this EXACT format:

            ## Goal
            [What was the user trying to accomplish in this branch?]

            ## Constraints & Preferences
            - [Any constraints, preferences, or requirements mentioned]
            - [Or "(none)" if none were mentioned]

            ## Progress
            ### Done
            - [x] [Completed tasks/changes]

            ### In Progress
            - [ ] [Work that was started but not finished]

            ### Blocked
            - [Issues preventing progress, if any]

            ## Key Decisions
            - **[Decision]**: [Brief rationale]

            ## Next Steps
            1. [What should happen next to continue this work]

            Keep each section concise. Preserve exact file paths, function names, and error messages.""";

    private final Model model;
    private final ModelStream modelStream;
    private final String thinkingLevel;
    private final ApiKeyResolver apiKeyResolver;
    private final ContextConverter sourceConverter;

    public ModelCompactionSummarizer(
            Model model,
            ModelStream modelStream,
            String thinkingLevel,
            ApiKeyResolver apiKeyResolver
    ) {
        this(model, modelStream, thinkingLevel, apiKeyResolver,
                SessionContextConverters.standardMessages());
    }

    public ModelCompactionSummarizer(
            Model model,
            ModelStream modelStream,
            String thinkingLevel,
            ApiKeyResolver apiKeyResolver,
            ContextConverter sourceConverter
    ) {
        this.model = Objects.requireNonNull(model, "model");
        this.modelStream = Objects.requireNonNull(modelStream, "modelStream");
        this.thinkingLevel = thinkingLevel == null ? "off" : thinkingLevel;
        this.apiKeyResolver = apiKeyResolver;
        this.sourceConverter = sourceConverter == null
                ? ContextConverters.standardMessages() : sourceConverter;
    }

    @Override
    public CompletionStage<Summary> summarize(Request request) {
        return sourceConverter.convert(request.messages()).thenCompose(messages -> {
            String prompt = prompt(request, ContextCompaction.serializeConversation(messages));
            Model requestModel = withOutputCap(model, request.maxTokens());
            AgentLoopConfig config = new AgentLoopConfig(
                    requestModel, thinkingLevel, UUID.randomUUID().toString(), modelStream,
                    ContextConverters.standardMessages(), null, apiKeyResolver,
                    null, null, null, null, null, null, null
            );
            return AgentLoop.run(
                    List.of(UserMessage.text(prompt, System.currentTimeMillis())),
                    new AgentContext(SYSTEM_PROMPT, List.of(), List.of()), config
            ).result().thenApply(result -> summary(result, request.kind()));
        });
    }

    private static Summary summary(
            List<AgentMessage> messages,
            Kind kind
    ) {
        AssistantMessage assistant = null;
        for (int index = messages.size() - 1; index >= 0; index--) {
            if (messages.get(index) instanceof AssistantMessage value) {
                assistant = value;
                break;
            }
        }
        if (assistant == null) {
            throw failed(kind, "Unknown error");
        }
        if (assistant.stopReason() == StopReason.ABORTED) {
            throw new CompactionException(
                    CompactionException.Code.ABORTED,
                    assistant.errorMessage() == null
                            ? abortedMessage(kind) : assistant.errorMessage()
            );
        }
        if (assistant.stopReason() == StopReason.ERROR) {
            throw failed(kind, assistant.errorMessage() == null
                    ? "Unknown error" : assistant.errorMessage());
        }
        ArrayList<String> text = new ArrayList<>();
        assistant.content().forEach(block -> {
            if (block instanceof TextContent value) text.add(value.text());
        });
        return new Summary(String.join("", text), assistant.usage());
    }

    private static String prompt(Request request, String conversation) {
        String instructions = switch (request.kind()) {
            case TURN_PREFIX -> TURN_PREFIX_PROMPT;
            case BRANCH -> BRANCH_PROMPT;
            case HISTORY -> request.previousSummary() == null ? HISTORY_PROMPT : UPDATE_PROMPT;
        };
        if (request.customInstructions() != null) {
            instructions += "\n\nAdditional focus: " + request.customInstructions();
        }
        StringBuilder prompt = new StringBuilder()
                .append("<conversation>\n").append(conversation)
                .append("\n</conversation>\n\n");
        if (request.previousSummary() != null) {
            prompt.append("<previous-summary>\n")
                    .append(request.previousSummary())
                    .append("\n</previous-summary>\n\n");
        }
        return prompt.append(instructions).toString();
    }

    private static Model withOutputCap(Model model, long requested) {
        long cap = model.maxTokens() > 0
                ? Math.min(requested, model.maxTokens()) : requested;
        int maxTokens = (int) Math.max(0, Math.min(Integer.MAX_VALUE, cap));
        return new Model(
                model.id(), model.name(), model.api(), model.provider(), model.baseUrl(),
                model.reasoning(), model.input(), model.contextWindow(), maxTokens,
                model.thinkingLevelMap()
        );
    }

    private static CompactionException failed(Kind kind, String message) {
        String prefix = kind == Kind.TURN_PREFIX
                ? "Turn prefix summarization failed: "
                : kind == Kind.BRANCH
                        ? "Branch summary failed: " : "Summarization failed: ";
        return new CompactionException(
                CompactionException.Code.SUMMARIZATION_FAILED, prefix + message
        );
    }

    private static String abortedMessage(Kind kind) {
        return switch (kind) {
            case HISTORY -> "Summarization aborted";
            case TURN_PREFIX -> "Turn prefix summarization aborted";
            case BRANCH -> "Branch summary aborted";
        };
    }
}
