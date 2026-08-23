package io.github.idoly.pi.agent.harness;

import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.QueueMode;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StreamOptions;

import java.util.List;
import java.util.Objects;

public record AgentHarnessOptions(
        AgentSession session,
        Model model,
        String thinkingLevel,
        List<String> activeToolNames,
        List<AgentTool> tools,
        HarnessResources resources,
        StreamOptions streamOptions,
        RetryPolicy retryPolicy,
        CompactionSettings compactionSettings,
        QueueMode steeringMode,
        QueueMode followUpMode
) {
    public AgentHarnessOptions {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(model, "model");
        thinkingLevel = thinkingLevel == null ? "off" : thinkingLevel;
        tools = tools == null ? List.of() : List.copyOf(tools);
        activeToolNames = activeToolNames == null
                ? tools.stream().map(AgentTool::name).toList()
                : List.copyOf(activeToolNames);
        resources = resources == null ? HarnessResources.EMPTY : resources.copy();
        retryPolicy = retryPolicy == null ? RetryPolicy.DISABLED : retryPolicy;
        compactionSettings = compactionSettings == null
                ? CompactionSettings.DEFAULT : compactionSettings;
        steeringMode = steeringMode == null ? QueueMode.ONE_AT_A_TIME : steeringMode;
        followUpMode = followUpMode == null ? QueueMode.ONE_AT_A_TIME : followUpMode;
    }

    public AgentHarnessOptions(AgentSession session, Model model) {
        this(
                session, model, "off", null, List.of(), HarnessResources.EMPTY,
                null, RetryPolicy.DISABLED, CompactionSettings.DEFAULT,
                QueueMode.ONE_AT_A_TIME, QueueMode.ONE_AT_A_TIME
        );
    }
}
