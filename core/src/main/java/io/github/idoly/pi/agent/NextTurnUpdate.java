package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.Model;

/** Null fields keep the current runtime value. */
public record NextTurnUpdate(
        AgentContext context,
        Model model,
        String thinkingLevel
) {
    public NextTurnUpdate(Model model, String thinkingLevel) {
        this(null, model, thinkingLevel);
    }

    public static NextTurnUpdate unchanged() {
        return new NextTurnUpdate(null, null, null);
    }
}
