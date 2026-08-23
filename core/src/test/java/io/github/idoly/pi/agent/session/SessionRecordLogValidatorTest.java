package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.ToolCallContent;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionRecordLogValidatorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void validationReasonsAndMessagesMatchUpstreamFixture() throws Exception {
        JsonNode expected = MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"),
                "record-log-0.84.2.json"
        ).toFile()).path("results");
        Map<String, Scenario> scenarios = scenarios();
        for (Map.Entry<String, Scenario> entry : scenarios.entrySet()) {
            JsonNode result = expected.path(entry.getKey());
            if (result.path("valid").asBoolean()) {
                validate(entry.getValue());
                continue;
            }
            RecordLogCorruption failure = assertThrows(
                    RecordLogCorruption.class,
                    () -> validate(entry.getValue()),
                    entry.getKey()
            );
            assertEquals(result.path("reason").asText(), reason(failure.reason()),
                    entry.getKey());
            assertEquals(result.path("message").asText(), failure.getMessage(),
                    entry.getKey());
        }
    }

    @Test
    void validatesRecordsLoadedThroughAgentSession() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = repository.create(
                new SessionRepository.CreateOptions("session", null)
        ).toCompletableFuture().join();
        session.appendRecord(startDraft("run")).toCompletableFuture().join();
        session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt", "main", "run", SessionRecordDraft.Step.ASSISTANT,
                1, "response", null
        )).toCompletableFuture().join();
        session.validateRecordLog("main").toCompletableFuture().join();

        AgentSession invalid = repository.create(
                new SessionRepository.CreateOptions("invalid", null)
        ).toCompletableFuture().join();
        invalid.appendRecord(new SessionRecordDraft.AbortRequested(
                "abort", "main", "missing"
        )).toCompletableFuture().join();
        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> invalid.validateRecordLog("main").toCompletableFuture().join()
        );
        assertEquals(
                RecordLogCorruption.Reason.UNKNOWN_OPERATION,
                ((RecordLogCorruption) failure.getCause()).reason()
        );
    }

    private static Map<String, Scenario> scenarios() {
        LinkedHashMap<String, Scenario> values = new LinkedHashMap<>();
        SessionRecord start = record(1, startDraft("run"));
        SessionRecord attempt = record(2, new SessionRecordDraft.StepAttempt(
                "attempt", "main", "run", SessionRecordDraft.Step.ASSISTANT,
                1, "response", null
        ));
        values.put("valid", new Scenario(List.of(start), List.of(start, attempt), List.of()));

        SessionRecord one = record(1, startDraft("one"));
        SessionRecord two = record(2, startDraft("two"));
        values.put("multipleOpenOperations", new Scenario(
                List.of(one, two), List.of(one, two), List.of()
        ));
        values.put("unknownOperation", new Scenario(
                List.of(), List.of(record(1, new SessionRecordDraft.AbortRequested(
                        "abort", "main", "missing"
                ))), List.of()
        ));
        SessionRecord finish = record(2, new SessionRecordDraft.OperationFinished(
                "finish", "main", "run",
                SessionRecordDraft.OperationOutcome.COMPLETED, null
        ));
        values.put("recordAfterFinish", new Scenario(
                List.of(), List.of(start, finish, record(3,
                        new SessionRecordDraft.AbortRequested("abort", "main", "run")
                )), List.of()
        ));
        values.put("nonConsecutiveAttempt", new Scenario(
                List.of(start), List.of(start, record(2,
                        new SessionRecordDraft.StepAttempt(
                                "attempt", "main", "run",
                                SessionRecordDraft.Step.ASSISTANT, 2,
                                "response", null
                        )
                )), List.of()
        ));
        SessionRecord abort = record(2, new SessionRecordDraft.AbortRequested(
                "abort", "main", "run"
        ));
        SessionEntryDraft.Message queued = new SessionEntryDraft.Message(
                "queued", UserMessage.text("late", 1L)
        );
        values.put("queueAfterAbort", new Scenario(
                List.of(start), List.of(start, abort, record(3,
                        new SessionRecordDraft.QueueEnqueued(
                                "queue", "main", SessionRecordDraft.Queue.STEER,
                                "run", queued
                        )
                )), List.of()
        ));
        values.put("invalidQueueCancellation", new Scenario(
                List.of(start), List.of(start, record(2,
                        new SessionRecordDraft.QueueCancelled(
                                "cancel", "main", "run", "missing"
                        )
                )), List.of()
        ));

        SessionEntry assistant = assistantEntry();
        values.put("toolCallMismatch", new Scenario(
                List.of(start), List.of(start, record(2, tool("tool", 1))),
                List.of(assistant)
        ));
        values.put("duplicateToolInvocation", new Scenario(
                List.of(start), List.of(
                        start, record(2, tool("first", 0)),
                        record(3, tool("second", 0))
                ), List.of(assistant)
        ));
        return values;
    }

    private static SessionRecordDraft.OperationStarted startDraft(String id) {
        return new SessionRecordDraft.OperationStarted(
                id, "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), null, null
                )
        );
    }

    private static SessionRecordDraft.ToolStarted tool(String id, int index) {
        return new SessionRecordDraft.ToolStarted(
                id, "main", "run", "assistant", index,
                "call", "read", MAPPER.valueToTree(Map.of("path", "a.ts")),
                "result-" + id, SessionRecordDraft.Replay.SAFE
        );
    }

    private static SessionEntry assistantEntry() {
        AssistantMessage message = new AssistantMessage(
                List.of(new ToolCallContent(
                        "call", "read", Map.of("path", "a.ts")
                )), "openai-responses", "openai", "gpt-5",
                new Usage(1, 1, 0, 0, 2, Cost.ZERO),
                StopReason.TOOL_USE, null, 2L
        );
        return new SessionEntry.Message(
                "assistant", null, 2, 2, message, false
        );
    }

    private static SessionRecord record(long sequence, SessionRecordDraft draft) {
        return new SessionRecord(sequence, sequence, draft);
    }

    private static void validate(Scenario scenario) {
        SessionRecordLogValidator.validate(
                "main", scenario.open(), scenario.records(), scenario.entries()
        );
    }

    private static String reason(RecordLogCorruption.Reason reason) {
        return reason.name().toLowerCase(java.util.Locale.ROOT);
    }

    private record Scenario(
            List<SessionRecord> open,
            List<SessionRecord> records,
            List<SessionEntry> entries
    ) {
    }
}
