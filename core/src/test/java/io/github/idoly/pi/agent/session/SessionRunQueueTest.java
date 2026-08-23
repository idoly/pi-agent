package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.ai.AssistantMessage;
import io.github.idoly.pi.ai.StopReason;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionRunQueueTest {
    @TempDir
    Path temporary;

    @Test
    void snapshotWatchBuffersCommitEventsAndPreservesReentrantOrder() throws Exception {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = openRun(repository, "queue-watch");
        SessionRunQueueEventBus.WatchHandle<List<SessionRunQueue.Pending>> watch =
                session.queueEvents().watch(() -> {
                    join(SessionRunQueue.enqueueMessage(
                            session, SessionRecordDraft.Queue.STEER, "run",
                            UserMessage.text("during snapshot", 1)
                    ));
                    return join(SessionRunQueue.pending(
                            session, SessionRecordDraft.Queue.STEER, "run", null
                    ));
                });
        SessionRunQueue.Pending second = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("before start", 2)
        ));
        ArrayList<SessionRunQueueEvent> received = new ArrayList<>();
        watch.start(event -> {
            received.add(event);
            if (received.size() == 1) {
                join(SessionRunQueue.cancel(
                        session, SessionRecordDraft.Queue.STEER, "run",
                        watch.snapshot().getFirst().target().id()
                ));
            }
        });

        assertEquals(1, watch.snapshot().size());
        assertEquals(List.of(
                SessionRunQueueEvent.Enqueued.class,
                SessionRunQueueEvent.Enqueued.class,
                SessionRunQueueEvent.Cancelled.class
        ), received.stream().map(Object::getClass).toList());
        assertEquals(second.target().id(),
                ((SessionRunQueueEvent.Enqueued) received.get(1))
                        .pending().target().id());
        join(SessionRunQueue.drain(
                session, SessionRecordDraft.Queue.STEER, "run", null
        ));
        assertInstanceOf(SessionRunQueueEvent.Consumed.class, received.getLast());

        watch.close();
        join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("after close", 3)
        ));
        assertEquals(4, received.size());
    }

    @Test
    void queueWatchCapacityBoundsEventsBeforeStart() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = openRun(repository, "bounded-queue-watch");
        SessionRunQueueEventBus.WatchHandle<List<SessionRunQueue.Pending>> watch =
                SessionRunQueue.watch(
                        session, SessionRecordDraft.Queue.STEER, "run", null, 1
                );
        join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("one", 1)
        ));
        join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("two", 2)
        ));
        assertTrue(watch.snapshot().isEmpty());
        assertTrue(watch.overflowed());
        assertEquals(1, watch.droppedEvents());
        assertThrows(SessionWatchOverflowException.class,
                () -> watch.start(ignored -> { }));
        assertEquals(2, join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.STEER, "run", null
        )).size());
    }

    @Test
    void failedQueueMutationEmitsNoEventAndLaneViewsShareBus() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = openRun(repository, "queue-passive");
        AgentSession view = session.view("main");
        ArrayList<SessionRunQueueEvent> received = new ArrayList<>();
        view.queueEvents().onEvent(received::add);
        SessionRunQueue.Pending pending = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("valid", 1)
        ));
        assertEquals(1, received.size());
        assertCode(SessionError.Code.ALREADY_EXISTS, SessionRunQueue.enqueue(
                session, SessionRecordDraft.Queue.STEER, "run", pending.target()
        ));
        assertEquals(1, received.size());
    }

    @Test
    void steerQueueCancelsAndDrainsInEnqueueOrder() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = openRun(repository, "steer-queue");
        SessionRunQueue.Pending first = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("first", 1)
        ));
        SessionRunQueue.Pending second = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.STEER, "run",
                UserMessage.text("second", 2)
        ));
        assertEquals(List.of(first.target().id(), second.target().id()),
                join(SessionRunQueue.pending(
                        session, SessionRecordDraft.Queue.STEER, "run", null
                )).stream().map(value -> value.target().id()).toList());

        assertTrue(join(SessionRunQueue.cancel(
                session, SessionRecordDraft.Queue.STEER, "run", first.target().id()
        )));
        assertTrue(!join(SessionRunQueue.cancel(
                session, SessionRecordDraft.Queue.STEER, "run", first.target().id()
        )));
        List<SessionEntry> drained = join(SessionRunQueue.drain(
                session, SessionRecordDraft.Queue.STEER, "run", null
        ));
        assertEquals(List.of(second.target().id()), drained.stream()
                .map(SessionEntry::id).toList());
        assertTrue(join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.STEER, "run", null
        )).isEmpty());
        assertEquals(second.target().id(), join(session.leafId()));
        join(session.validateRecordLog("main"));
    }

    @Test
    void nextRunQueueNeedsNoOpenOperationAndHonorsDrainLimit() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "next-run-queue");
        SessionRunQueue.Pending first = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.NEXT_RUN, null,
                UserMessage.text("first", 1)
        ));
        SessionRunQueue.Pending second = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.NEXT_RUN, null,
                UserMessage.text("second", 2)
        ));

        assertEquals(List.of(first.target().id()), join(SessionRunQueue.drain(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, 1
        )).stream().map(SessionEntry::id).toList());
        assertEquals(List.of(second.target().id()), join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).stream().map(value -> value.target().id()).toList());
        assertEquals(List.of(second.target().id()), join(SessionRunQueue.drain(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).stream().map(SessionEntry::id).toList());
        assertTrue(join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).isEmpty());
        join(session.validateRecordLog("main"));
    }

    @Test
    void abortAndWrongRunIdRejectBeforeQueueMutation() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = openRun(repository, "queue-admission");
        join(SessionRunOperation.requestAbort(session, "run"));
        long sequence = join(session.log(0, null)).size();

        assertCode(SessionError.Code.INVALID_PAYLOAD,
                SessionRunQueue.enqueueMessage(
                        session, SessionRecordDraft.Queue.FOLLOW_UP, "run",
                        UserMessage.text("rejected", 1)
                ));
        assertCode(SessionError.Code.NOT_FOUND,
                SessionRunQueue.enqueueMessage(
                        session, SessionRecordDraft.Queue.STEER, "missing",
                        UserMessage.text("rejected", 2)
                ));
        assertThrows(SessionError.class, () -> SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.NEXT_RUN, "run",
                UserMessage.text("invalid", 3)
        ));
        assertEquals(sequence, join(session.log(0, null)).size());
    }

    @Test
    void failedDrainRollsBackAndLeavesTargetPending() {
        InMemorySessionRepository repository = new InMemorySessionRepository();
        AgentSession session = create(repository, "failed-drain");
        AssistantMessage pending = new AssistantMessage(
                List.of(), "api", "provider", "model", Usage.ZERO,
                StopReason.PENDING, null, 1
        );
        SessionRunQueue.Pending queued = join(SessionRunQueue.enqueue(
                session, SessionRecordDraft.Queue.NEXT_RUN, null,
                new SessionEntryDraft.Message("pending-assistant", pending)
        ));
        long sequence = join(session.log(0, null)).size();

        assertCode(SessionError.Code.INVALID_ENTRY, SessionRunQueue.drain(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        ));
        assertEquals(sequence, join(session.log(0, null)).size());
        assertEquals(List.of(queued.target().id()), join(SessionRunQueue.pending(
                session, SessionRecordDraft.Queue.NEXT_RUN, null, null
        )).stream().map(value -> value.target().id()).toList());
    }

    @Test
    void jsonlQueueProjectionSurvivesEnqueueCancelAndDrainReopens() {
        JsonlSessionRepository repository = new JsonlSessionRepository(
                temporary.resolve("sessions"), temporary
        );
        AgentSession session = openRun(repository, "jsonl-queue");
        SessionMetadata metadata = join(session.metadata());
        SessionRunQueue.Pending canceled = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.FOLLOW_UP, "run",
                UserMessage.text("canceled", 1)
        ));
        SessionRunQueue.Pending kept = join(SessionRunQueue.enqueueMessage(
                session, SessionRecordDraft.Queue.FOLLOW_UP, "run",
                UserMessage.text("kept", 2)
        ));
        join(SessionRunQueue.cancel(
                session, SessionRecordDraft.Queue.FOLLOW_UP,
                "run", canceled.target().id()
        ));

        AgentSession reopened = join(repository.open(metadata));
        assertEquals(List.of(kept.target().id()), join(SessionRunQueue.pending(
                reopened, SessionRecordDraft.Queue.FOLLOW_UP, "run", null
        )).stream().map(value -> value.target().id()).toList());
        join(SessionRunQueue.drain(
                reopened, SessionRecordDraft.Queue.FOLLOW_UP, "run", null
        ));
        AgentSession verified = join(repository.open(metadata));
        assertEquals(kept.target().id(), join(verified.leafId()));
        assertTrue(join(SessionRunQueue.pending(
                verified, SessionRecordDraft.Queue.FOLLOW_UP, "run", null
        )).isEmpty());
        join(verified.validateRecordLog("main"));
    }

    private static AgentSession openRun(SessionRepository repository, String id) {
        AgentSession session = create(repository, id);
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", null,
                new SessionRecordDraft.OperationIntent.Run(
                        List.of(), List.of(), "system", null
                )
        )));
        return session;
    }

    private static AgentSession create(SessionRepository repository, String id) {
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionError assertCode(
            SessionError.Code code,
            CompletionStage<?> stage
    ) {
        CompletionException failure = assertThrows(
                CompletionException.class, () -> stage.toCompletableFuture().join()
        );
        SessionError error = assertInstanceOf(SessionError.class, failure.getCause());
        assertEquals(code, error.code());
        return error;
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
