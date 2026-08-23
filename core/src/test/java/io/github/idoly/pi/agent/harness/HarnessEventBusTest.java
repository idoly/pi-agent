package io.github.idoly.pi.agent.harness;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HarnessEventBusTest {
    @Test
    void deliversTypedFutureEventsAndUnsubscribes() throws Exception {
        HarnessEventBus bus = new HarnessEventBus();
        ArrayList<HarnessEvent> received = new ArrayList<>();
        AutoCloseable startSubscription = bus.onRunStart(received::add);
        bus.onRunEnd(received::add);
        HarnessEvent.RunStart start = new HarnessEvent.RunStart("main", "run");
        HarnessEvent.RunEnd end = new HarnessEvent.RunEnd(
                "main", "run", HarnessEvent.Outcome.COMPLETED, "leaf"
        );
        bus.emit(start);
        bus.emit(end);
        startSubscription.close();
        bus.emit(new HarnessEvent.RunStart("main", "later"));
        assertEquals(List.of(start, end), received);
    }

    @Test
    void registersWatcherBeforeSnapshotAndBuffersUntilStart() {
        HarnessEventBus bus = new HarnessEventBus();
        HarnessEvent.RunStart duringSnapshot =
                new HarnessEvent.RunStart("main", "during-snapshot");
        HarnessEventBus.WatchHandle<String> watch = bus.watch(() -> {
            bus.emit(duringSnapshot);
            return "snapshot";
        });
        HarnessEvent.RunStart beforeStart =
                new HarnessEvent.RunStart("main", "before-start");
        bus.emit(beforeStart);
        ArrayList<HarnessEvent> received = new ArrayList<>();
        watch.start(received::add);
        HarnessEvent.RunEnd afterStart = new HarnessEvent.RunEnd(
                "main", "before-start", HarnessEvent.Outcome.FAILED, "leaf"
        );
        bus.emit(afterStart);
        assertEquals("snapshot", watch.snapshot());
        assertEquals(List.of(duringSnapshot, beforeStart, afterStart), received);
    }

    @Test
    void preservesEventsEmittedReentrantlyWhileFlushing() {
        HarnessEventBus bus = new HarnessEventBus();
        HarnessEventBus.WatchHandle<Integer> watch = bus.watch(() -> 1);
        HarnessEvent.RunStart first = new HarnessEvent.RunStart("main", "first");
        HarnessEvent.RunStart second = new HarnessEvent.RunStart("main", "second");
        bus.emit(first);
        ArrayList<HarnessEvent> received = new ArrayList<>();
        watch.start(event -> {
            received.add(event);
            if (event.equals(first)) bus.emit(second);
        });
        assertEquals(List.of(first, second), received);
        watch.close();
        bus.emit(new HarnessEvent.RunStart("main", "ignored"));
        assertEquals(List.of(first, second), received);
    }
}
