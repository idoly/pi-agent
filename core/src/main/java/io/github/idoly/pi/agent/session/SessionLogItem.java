package io.github.idoly.pi.agent.session;

public sealed interface SessionLogItem permits
        SessionLogItem.Entry,
        SessionLogItem.Lane,
        SessionLogItem.Name,
        SessionLogItem.Label,
        SessionLogItem.Record,
        SessionLogItem.Usage {
    long sequence();

    record Entry(long sequence, SessionEntry entry, String lane) implements SessionLogItem {
        public Entry(long sequence, SessionEntry entry) {
            this(sequence, entry, null);
        }
    }

    record Lane(long sequence, String lane, String leafId) implements SessionLogItem {
    }

    record Name(long sequence, String name) implements SessionLogItem {
    }

    record Label(long sequence, String targetId, String label) implements SessionLogItem {
    }

    record Record(long sequence, SessionRecord record) implements SessionLogItem {
    }

    /** @deprecated Use {@link Record}; retained for source compatibility. */
    @Deprecated
    record Usage(long sequence, SessionUsageRow row) implements SessionLogItem {
    }
}
