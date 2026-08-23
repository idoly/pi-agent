package io.github.idoly.pi.agent.harness;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.idoly.pi.agent.AgentTool;
import io.github.idoly.pi.agent.QueueMode;
import io.github.idoly.pi.agent.session.AgentSession;
import io.github.idoly.pi.agent.session.SessionRecordQuery;
import io.github.idoly.pi.ai.AgentMessage;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.ai.StreamOptions;
import io.github.idoly.pi.ai.Usage;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

/** Behavior-compatible scaffold for the AgentHarness surface published in pi 0.84.2. */
public final class AgentHarness implements AutoCloseable {
    public static final String MAIN_LANE = "main";

    private final AgentSession durableSession;
    private final Registry hooks = new UnavailableRegistry("hooks.on");
    private final Registry events = new UnavailableRegistry("events.on");
    private Model model;
    private String thinkingLevel;
    private List<String> activeToolNames;
    private List<AgentTool> tools;
    private HarnessResources resources;
    private StreamOptions streamOptions;
    private RetryPolicy retryPolicy;
    private CompactionSettings compactionSettings;
    private QueueMode steeringMode;
    private QueueMode followUpMode;
    private volatile boolean closed;

    private AgentHarness(AgentHarnessOptions options) {
        durableSession = options.session();
        model = options.model();
        thinkingLevel = options.thinkingLevel();
        activeToolNames = options.activeToolNames();
        tools = options.tools();
        resources = options.resources();
        streamOptions = options.streamOptions();
        retryPolicy = options.retryPolicy();
        compactionSettings = options.compactionSettings();
        steeringMode = options.steeringMode();
        followUpMode = options.followUpMode();
    }

    public static CompletionStage<CreateResult> create(AgentHarnessOptions options) {
        Objects.requireNonNull(options, "options");
        return options.session().findRecords(new SessionRecordQuery(
                null, null, null, null, null, null, 1
        )).thenCompose(records -> {
            if (!records.isEmpty()) {
                return CompletableFuture.failedFuture(
                        new HarnessNotImplemented("create.restore")
                );
            }
            return CompletableFuture.completedFuture(new CreateResult(
                    new AgentHarness(options), List.of()
            ));
        });
    }

    public String name() { return MAIN_LANE; }

    public AgentSession session() { return durableSession; }

    public Registry hooks() { return hooks; }

    public Registry events() { return events; }

    public CompletionStage<String> leafId() { return durableSession.leafId(); }

    public CompletionStage<Object> prompt(String text) { return unavailable("prompt"); }

    public CompletionStage<Object> prompt(AgentMessage message) { return unavailable("prompt"); }

    public CompletionStage<Object> prompt(List<AgentMessage> messages) { return unavailable("prompt"); }

    public CompletionStage<Object> skill(String name, String additionalInstructions) {
        return unavailable("skill");
    }

    public CompletionStage<Object> promptFromTemplate(String name, List<String> arguments) {
        return unavailable("promptFromTemplate");
    }

    public CompletionStage<Object> compact(String customInstructions) {
        return unavailable("compact");
    }

    public CompletionStage<Object> navigateTree(String targetId) {
        return unavailable("navigateTree");
    }

    public CompletionStage<Object> resume() { return unavailable("resume"); }

    public CompletionStage<Object> abort() { return unavailable("abort"); }

    public CompletionStage<Object> steer(AgentMessage message) { return unavailable("steer"); }

    public CompletionStage<Object> followUp(AgentMessage message) { return unavailable("followUp"); }

    public CompletionStage<Object> nextRun(AgentMessage message) { return unavailable("nextRun"); }

    public CompletionStage<Object> cancelQueued(String entryId) {
        return unavailable("cancelQueued");
    }

    public CompletionStage<Object> recordUsage(Usage usage, String entryId, JsonNode details) {
        return unavailable("recordUsage");
    }

    public CompletionStage<Void> waitForIdle() { return unavailable("waitForIdle"); }

    public CompletionStage<Void> runWhenIdle(Supplier<CompletionStage<Void>> callback) {
        return unavailable("runWhenIdle");
    }

    public CompletionStage<Object> peekAction() { return unavailable("peekAction"); }

    public CompletionStage<Object> executeAction() { return unavailable("executeAction"); }

    public CompletionStage<Void> runToCompletion() { return unavailable("runToCompletion"); }

    public CompletionStage<Object> watch() { return unavailable("watch"); }

    public CompletionStage<Object> lane(String name) { return unavailable("lane"); }

    public CompletionStage<Object> createLane(String name, String at) {
        return unavailable("createLane");
    }

    public CompletionStage<Object> lanes() { return unavailable("lanes"); }

    public CompletionStage<Object> watchSession() { return unavailable("watchSession"); }

    public synchronized Model model() { return model; }

    public synchronized void model(Model value) { model = Objects.requireNonNull(value, "value"); }

    public synchronized String thinkingLevel() { return thinkingLevel; }

    public synchronized void thinkingLevel(String value) {
        thinkingLevel = Objects.requireNonNull(value, "value");
    }

    public synchronized List<String> activeToolNames() { return List.copyOf(activeToolNames); }

    public synchronized void activeToolNames(List<String> value) {
        activeToolNames = List.copyOf(value);
    }

    public synchronized List<AgentTool> tools() { return List.copyOf(tools); }

    public synchronized void tools(List<AgentTool> value, List<String> activeNames) {
        tools = List.copyOf(value);
        activeToolNames = activeNames == null
                ? tools.stream().map(AgentTool::name).toList()
                : List.copyOf(activeNames);
    }

    public synchronized HarnessResources resources() { return resources.copy(); }

    public synchronized void resources(HarnessResources value) {
        resources = Objects.requireNonNull(value, "value").copy();
    }

    public synchronized StreamOptions streamOptions() { return streamOptions; }

    public synchronized void streamOptions(StreamOptions value) { streamOptions = value; }

    public synchronized RetryPolicy retryPolicy() { return retryPolicy; }

    public synchronized void retryPolicy(RetryPolicy value) {
        retryPolicy = Objects.requireNonNull(value, "value");
    }

    public synchronized CompactionSettings compactionSettings() {
        return compactionSettings;
    }

    public synchronized void compactionSettings(CompactionSettings value) {
        compactionSettings = Objects.requireNonNull(value, "value");
    }

    public synchronized QueueMode steeringMode() { return steeringMode; }

    public synchronized void steeringMode(QueueMode value) {
        steeringMode = Objects.requireNonNull(value, "value");
    }

    public synchronized QueueMode followUpMode() { return followUpMode; }

    public synchronized void followUpMode(QueueMode value) {
        followUpMode = Objects.requireNonNull(value, "value");
    }

    @Override
    public void close() { closed = true; }

    private <T> CompletionStage<T> unavailable(String operation) {
        return CompletableFuture.failedFuture(
                closed ? new HarnessClosed() : new HarnessNotImplemented(operation)
        );
    }

    public record CreateResult(AgentHarness harness, List<SuspendedOperation> suspended) {
        public CreateResult {
            Objects.requireNonNull(harness, "harness");
            suspended = List.copyOf(suspended);
        }
    }

    public interface Registry {
        AutoCloseable on(String name, Function<Object, CompletionStage<?>> handler);
    }

    private final class UnavailableRegistry implements Registry {
        private final String operation;

        private UnavailableRegistry(String operation) { this.operation = operation; }

        @Override
        public AutoCloseable on(String name, Function<Object, CompletionStage<?>> handler) {
            throw closed ? new HarnessClosed() : new HarnessNotImplemented(operation);
        }
    }
}
