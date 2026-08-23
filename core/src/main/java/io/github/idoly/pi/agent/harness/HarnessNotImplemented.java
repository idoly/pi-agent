package io.github.idoly.pi.agent.harness;

import java.util.Objects;

public final class HarnessNotImplemented extends UnsupportedOperationException {
    private final String operation;

    public HarnessNotImplemented(String operation) {
        super("AgentHarness." + Objects.requireNonNull(operation, "operation")
                + " is not implemented yet");
        this.operation = operation;
    }

    public String operation() {
        return operation;
    }
}
