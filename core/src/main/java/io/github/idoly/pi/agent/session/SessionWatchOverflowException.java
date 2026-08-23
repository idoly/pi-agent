package io.github.idoly.pi.agent.session;

/** A snapshot-first watch could not retain its complete registration window. */
public final class SessionWatchOverflowException extends IllegalStateException {
    private final int capacity;
    private final long droppedEvents;

    SessionWatchOverflowException(int capacity, long droppedEvents) {
        super("Session watch registration buffer exceeded capacity " + capacity
                + " and dropped " + droppedEvents
                + " event(s); capture a new snapshot");
        this.capacity = capacity;
        this.droppedEvents = droppedEvents;
    }

    public int capacity() {
        return capacity;
    }

    public long droppedEvents() {
        return droppedEvents;
    }
}
