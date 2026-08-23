package io.github.idoly.pi.agent.session;

public record SessionEntryQuery(
        SessionEntry.Type type,
        String customType,
        Order order,
        Integer limit,
        Long afterSequence
) {
    public static final SessionEntryQuery ALL = new SessionEntryQuery(
            null, null, Order.NEWEST_FIRST, null, null
    );

    public SessionEntryQuery {
        order = order == null ? Order.NEWEST_FIRST : order;
        if (limit != null && limit <= 0) {
            throw new SessionError(SessionError.Code.INVALID_QUERY, "limit must be positive");
        }
        if (afterSequence != null && afterSequence < 0) {
            throw new SessionError(
                    SessionError.Code.INVALID_QUERY,
                    "cursor sequence must be non-negative"
            );
        }
        if (customType != null && customType.isBlank()) {
            throw new SessionError(SessionError.Code.INVALID_QUERY, "customType must not be blank");
        }
    }

    public enum Order {
        NEWEST_FIRST,
        OLDEST_FIRST
    }
}
