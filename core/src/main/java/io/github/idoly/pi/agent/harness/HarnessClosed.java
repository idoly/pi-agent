package io.github.idoly.pi.agent.harness;

public final class HarnessClosed extends IllegalStateException {
    public HarnessClosed() {
        super("AgentHarness was closed while the operation was active");
    }
}
