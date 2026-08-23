package io.github.idoly.pi.agent.session;

import io.github.idoly.pi.agent.compaction.CompactionSummarizer;
import io.github.idoly.pi.agent.harness.CompactionSettings;
import io.github.idoly.pi.ai.Usage;
import io.github.idoly.pi.ai.UserMessage;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionOperationEventTest {
    private static final CompactionSettings SETTINGS =
            new CompactionSettings(true, 100, 1);

    @Test
    void compactionEventsFollowDurableAcceptanceAttemptAndTerminalCommits() {
        AgentSession session = memory("compaction-events");
        append(session, "first", "history");
        append(session, "source", "recent");
        SessionOperationEventBus events = new SessionOperationEventBus();
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        events.onEvent(event -> {
            received.add(event);
            if (event instanceof SessionOperationEvent.Started) {
                assertEquals(1, join(session.findOpenOperations("main", 2)).size());
            } else if (event instanceof SessionOperationEvent.AttemptStarted attempt) {
                assertEquals(1, recordsOldest(session).stream()
                        .filter(record -> record.type()
                                == SessionRecordDraft.Type.STEP_ATTEMPT)
                        .count());
                assertEquals(1, attempt.attempt());
            } else if (event instanceof SessionOperationEvent.Finished finished) {
                assertTrue(join(session.findOpenOperations("main", 2)).isEmpty());
                assertEquals(finished.resultEntryId(), join(session.leafId()));
            }
        });

        SessionCompactionOperation.Outcome.Completed outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Completed.class,
                join(SessionCompactionOperation.compact(
                        session,
                        new SessionCompactionOperation.Options(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL,
                                0, null
                        ),
                        ignored -> CompletableFuture.completedFuture(
                                new CompactionSummarizer.Summary("summary", Usage.ZERO)
                        ),
                        events
                ))
        );

        assertEquals(List.of(
                SessionOperationEvent.Started.class,
                SessionOperationEvent.AttemptStarted.class,
                SessionOperationEvent.Finished.class
        ), received.stream().map(Object::getClass).toList());
        SessionOperationEvent.Finished finished =
                (SessionOperationEvent.Finished) received.getLast();
        assertEquals(SessionOperationEvent.Outcome.COMPLETED, finished.outcome());
        assertEquals(outcome.entry().id(), finished.resultEntryId());
        assertNull(finished.error());
    }

    @Test
    void unsummarizedNavigationEmitsNoAttemptAndRejectedNavigationEmitsNothing() {
        AgentSession session = memory("navigation-events");
        SessionEntry target = append(session, "target", "target");
        append(session, "source", "source");
        SessionOperationEventBus events = new SessionOperationEventBus();
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        events.onEvent(received::add);

        join(SessionNavigation.navigate(
                session, target.id(), SessionNavigation.Options.DEFAULT, null, events
        ));
        assertEquals(List.of(
                SessionOperationEvent.Started.class,
                SessionOperationEvent.Finished.class
        ), received.stream().map(Object::getClass).toList());

        received.clear();
        try {
            join(SessionNavigation.navigate(
                    session, target.id(), SessionNavigation.Options.DEFAULT, null, events
            ));
        } catch (java.util.concurrent.CompletionException ignored) {
        }
        assertEquals(List.of(), received);
    }

    @Test
    void recoveryEmitsLaterAttemptAndFinishWithoutRepeatingStart() {
        AgentSession session = memory("recovery-events");
        append(session, "first", "history");
        SessionEntry source = append(session, "source", "source");
        join(session.appendRecord(new SessionRecordDraft.OperationStarted(
                "run", "main", source.id(),
                new SessionRecordDraft.OperationIntent.Compaction(null, "result")
        )));
        join(session.appendRecord(new SessionRecordDraft.StepAttempt(
                "attempt-1", "main", "run", SessionRecordDraft.Step.COMPACTION,
                1, "result", SessionRecordDraft.CompactionReason.MANUAL
        )));
        SessionOperationEventBus events = new SessionOperationEventBus();
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        events.onEvent(received::add);

        join(SessionCompactionOperation.resume(
                session,
                new SessionCompactionOperation.RecoveryOptions(
                        SETTINGS, SessionRecordDraft.CompactionReason.MANUAL, 3
                ),
                ignored -> CompletableFuture.completedFuture(
                        new CompactionSummarizer.Summary("recovered", Usage.ZERO)
                ),
                events
        ));

        assertEquals(List.of(
                SessionOperationEvent.AttemptStarted.class,
                SessionOperationEvent.Finished.class
        ), received.stream().map(Object::getClass).toList());
        assertEquals(2,
                ((SessionOperationEvent.AttemptStarted) received.getFirst()).attempt());
    }

    @Test
    void failedEffectPublishesFailedTerminalEventAndListenerFailuresArePassive() {
        AgentSession session = memory("failure-events");
        append(session, "first", "history");
        SessionEntry source = append(session, "source", "source");
        SessionOperationEventBus events = new SessionOperationEventBus();
        events.onEvent(ignored -> { throw new IllegalStateException("listener"); });
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        events.onEvent(received::add);

        SessionCompactionOperation.Outcome.Failed outcome = assertInstanceOf(
                SessionCompactionOperation.Outcome.Failed.class,
                join(SessionCompactionOperation.compact(
                        session,
                        new SessionCompactionOperation.Options(
                                SETTINGS, SessionRecordDraft.CompactionReason.MANUAL,
                                0, null
                        ),
                        ignored -> CompletableFuture.failedFuture(
                                new IllegalStateException("provider")
                        ),
                        events
                ))
        );
        assertEquals(source.id(), outcome.leafId());
        SessionOperationEvent.Finished finished = assertInstanceOf(
                SessionOperationEvent.Finished.class, received.getLast()
        );
        assertEquals(SessionOperationEvent.Outcome.FAILED, finished.outcome());
        assertEquals("provider", finished.error().message());
        assertNull(finished.resultEntryId());
    }

    @Test
    void boundedWatchReportsRegistrationOverflowAndRequiresNewSnapshot() {
        SessionOperationEventBus events = new SessionOperationEventBus();
        SessionOperationEventBus.WatchHandle<String> watch = events.watch(() -> {
            events.emit(started("one"));
            events.emit(started("two"));
            events.emit(started("three"));
            events.emit(started("four"));
            return "stale-snapshot";
        }, 2);

        assertEquals("stale-snapshot", watch.snapshot());
        assertTrue(watch.overflowed());
        assertEquals(0, watch.bufferedEvents());
        assertEquals(2, watch.droppedEvents());
        SessionWatchOverflowException overflow = assertThrows(
                SessionWatchOverflowException.class,
                () -> watch.start(ignored -> { })
        );
        assertEquals(2, overflow.capacity());
        assertEquals(2, overflow.droppedEvents());
        assertTrue(overflow.getMessage().contains("capture a new snapshot"));
        events.emit(started("after-overflow"));
        assertEquals(2, watch.droppedEvents());

        assertThrows(IllegalArgumentException.class,
                () -> events.watch(() -> "snapshot", 0));
    }

    @Test
    void watchDiagnosticsTrackBufferThenResetAtLiveDelivery() {
        SessionOperationEventBus events = new SessionOperationEventBus();
        SessionOperationEventBus.WatchHandle<String> watch = events.watch(() -> {
            events.emit(started("buffered"));
            return "snapshot";
        }, 2);
        assertFalse(watch.overflowed());
        assertEquals(1, watch.bufferedEvents());
        assertEquals(0, watch.droppedEvents());
        watch.start(ignored -> { });
        assertEquals(0, watch.bufferedEvents());
        assertFalse(watch.overflowed());
        watch.close();
    }

    @Test
    void watchRegistersBeforeSnapshotAndPreservesReentrantOrder() {
        SessionOperationEventBus events = new SessionOperationEventBus();
        SessionOperationEvent.Started during = started("during");
        SessionOperationEvent.Started queued = started("queued");
        SessionOperationEvent.Finished reentrant = new SessionOperationEvent.Finished(
                "main", "during",
                io.github.idoly.pi.agent.harness.SuspendedOperation.Kind.NAVIGATION,
                SessionOperationEvent.Outcome.COMPLETED,
                null, null, null
        );
        SessionOperationEventBus.WatchHandle<String> watch = events.watch(() -> {
            events.emit(during);
            return "snapshot";
        });
        events.emit(queued);
        ArrayList<SessionOperationEvent> received = new ArrayList<>();
        watch.start(event -> {
            received.add(event);
            if (event.equals(during)) events.emit(reentrant);
        });
        assertEquals("snapshot", watch.snapshot());
        assertEquals(List.of(during, queued, reentrant), received);
        watch.close();
    }

    private static SessionOperationEvent.Started started(String id) {
        return new SessionOperationEvent.Started(
                "main", id,
                io.github.idoly.pi.agent.harness.SuspendedOperation.Kind.NAVIGATION,
                null
        );
    }

    private static AgentSession memory(String id) {
        AtomicInteger ids = new AtomicInteger();
        InMemorySessionRepository repository = new InMemorySessionRepository(
                Clock.systemUTC(), ignored -> id + "-id-" + ids.incrementAndGet()
        );
        return join(repository.create(new SessionRepository.CreateOptions(id, null)));
    }

    private static SessionEntry append(AgentSession session, String id, String text) {
        return join(session.append(new SessionEntryDraft.Message(
                id, UserMessage.text(text, 1)
        )));
    }

    private static List<SessionRecord> recordsOldest(AgentSession session) {
        return join(session.findRecords(new SessionRecordQuery(
                null, null, null, null, null,
                SessionEntryQuery.Order.OLDEST_FIRST, null
        )));
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }
}
