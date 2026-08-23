package io.github.idoly.pi.agent.extension;

import io.github.idoly.pi.agent.Agent;
import io.github.idoly.pi.agent.AgentListener;
import io.github.idoly.pi.agent.AgentState;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.UserMessage;

import java.util.List;
import java.util.concurrent.CompletionStage;

/** Agent facade that applies before-agent extension middleware to every run. */
public final class ExtensionAgent {
    private final ExtensionRuntime runtime;
    private final Agent agent;
    private final String baseSystemPrompt;

    ExtensionAgent(
            ExtensionRuntime runtime,
            Agent agent,
            String baseSystemPrompt
    ) {
        this.runtime = runtime;
        this.agent = agent;
        this.baseSystemPrompt = baseSystemPrompt;
    }

    public CompletionStage<Void> prompt(String text) {
        return prompt(List.of(UserMessage.text(
                text, System.currentTimeMillis()
        )));
    }

    public CompletionStage<Void> prompt(AgentMessage prompt) {
        return prompt(List.of(prompt));
    }

    public CompletionStage<Void> prompt(List<AgentMessage> prompts) {
        return runtime.beforeAgentStart(prompts, baseSystemPrompt)
                .thenCompose(result -> {
                    agent.systemPrompt(result.systemPrompt());
                    return agent.prompt(result.prompts());
                }).whenComplete((ignored, failure) ->
                        agent.systemPrompt(baseSystemPrompt));
    }

    public CompletionStage<Void> continueRun() {
        return agent.continueRun();
    }

    public void abort() {
        agent.abort();
    }

    public CompletionStage<Void> waitForIdle() {
        return agent.waitForIdle();
    }

    public AgentState state() {
        return agent.state();
    }

    public AutoCloseable subscribe(AgentListener listener) {
        return agent.subscribe(listener);
    }

    public Agent delegate() {
        return agent;
    }
}
