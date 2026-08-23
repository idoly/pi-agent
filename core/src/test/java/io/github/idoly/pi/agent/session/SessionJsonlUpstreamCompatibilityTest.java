package io.github.idoly.pi.agent.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.idoly.pi.ai.Cost;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SessionJsonlUpstreamCompatibilityTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long TIMESTAMP = 1_767_225_600_000L;

    @TempDir
    Path temporary;

    @Test
    void codecAndReplayMatchPublishedTypescriptV4Fixture() throws Exception {
        JsonNode fixture = MAPPER.readTree(Path.of(
                System.getProperty("pi.compatFixtures"),
                "session-jsonl-0.84.2.json"
        ).toFile());
        JsonNode expected = fixture.path("lines");
        ArrayList<JsonNode> actual = new ArrayList<>();
        actual.add(MAPPER.readTree(JsonlSessionCodec.encodeHeader(
                new JsonlSessionCodec.Header(
                        "oracle", TIMESTAMP, "/workspace", "parent",
                        MAPPER.valueToTree(java.util.Map.of("owner", "test"))
                )
        )));
        SessionEntry message = new SessionEntry.Message(
                "message", null, 1, TIMESTAMP, UserMessage.text("hello", 1L), false
        );
        actual.add(encoded(new SessionLogItem.Entry(1, message, "main")));
        actual.add(encoded(new SessionLogItem.Lane(2, "thread", "message")));
        actual.add(encoded(new SessionLogItem.Record(
                3, new SessionRecord(
                        3, TIMESTAMP,
                        new SessionRecordDraft.OperationStarted(
                                "run", "thread", "message",
                                new SessionRecordDraft.OperationIntent.Run(
                                        List.of(), List.of(), null, null
                                )
                        )
                )
        )));
        actual.add(encoded(new SessionLogItem.Name(4, "Oracle")));
        Usage usage = new Usage(
                10, 2, 3, 1, 13,
                new Cost(1, 2, 3, 4, 10)
        );
        actual.add(encoded(new SessionLogItem.Record(
                5, new SessionRecord(
                        5, TIMESTAMP,
                        new SessionRecordDraft.UsageRecord(
                                "usage", "thread", "adjustment", usage,
                                null, null, null, null, null,
                                MAPPER.valueToTree(java.util.Map.of("source", "oracle"))
                        )
                )
        )));
        assertEquals(expected, MAPPER.valueToTree(actual));

        Path directory = temporary.resolve("import");
        Files.createDirectories(directory);
        Path imported = directory.resolve("fixed_oracle.jsonl");
        StringBuilder content = new StringBuilder();
        expected.forEach(line -> content.append(line).append('\n'));
        Files.writeString(imported, content, StandardCharsets.UTF_8);
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary, Path.of("/workspace"),
                Clock.fixed(Instant.ofEpochMilli(TIMESTAMP), ZoneOffset.UTC),
                ignored -> "generated"
        );
        AgentSession session = join(repository.open(new SessionMetadata(
                "oracle", TIMESTAMP, SessionMetadata.CURRENT_STORAGE_VERSION, "parent"
        )));
        assertEquals("Oracle", join(session.name()));
        assertEquals("message", join(session.leafId()));
        assertEquals(List.of("run"), join(session.findOpenOperations("thread", 2))
                .stream().map(SessionRecord::id).toList());
        assertEquals(new SessionStats(1, 3, 11, 13, 10), join(session.stats()));
    }

    private static JsonNode encoded(SessionLogItem mutation) throws Exception {
        return MAPPER.readTree(JsonlSessionCodec.encodeMutation(mutation));
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
