package io.github.idoly.pi.agent;

import io.github.idoly.pi.ai.CancellationSignal;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface PrepareNextTurn {
    CompletionStage<NextTurnUpdate> prepare(TurnContext context, CancellationSignal cancellation);
}
