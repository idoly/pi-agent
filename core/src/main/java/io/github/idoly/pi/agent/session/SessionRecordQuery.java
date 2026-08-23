package io.github.idoly.pi.agent.session;

public record SessionRecordQuery(
        String lane,
        SessionRecordDraft.Type type,
        String runId,
        SessionRecordDraft.OperationKind operationKind,
        Long afterSequence,
        SessionEntryQuery.Order order,
        Integer limit
) {
    public SessionRecordQuery {
        order = order == null ? SessionEntryQuery.Order.NEWEST_FIRST : order;
        if (operationKind != null && type != SessionRecordDraft.Type.OPERATION_STARTED) {
            throw invalid("operationKind requires type OPERATION_STARTED");
        }
        if (afterSequence != null && afterSequence < 0) {
            throw invalid("cursor sequence must be non-negative");
        }
        if (limit != null && limit <= 0) {
            throw invalid("limit must be positive");
        }
    }

    public static SessionRecordQuery all() {
        return new SessionRecordQuery(null, null, null, null, null, null, null);
    }

    private static SessionError invalid(String message) {
        return new SessionError(SessionError.Code.INVALID_QUERY, message);
    }
}
