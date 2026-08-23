package io.github.idoly.pi.agent.session;

public record SessionBranchQuery(
        String start,
        String stopAtId,
        SessionEntry.Type stopAtType,
        SessionEntryQuery entries
) {
    public SessionBranchQuery {
        entries = entries == null ? SessionEntryQuery.ALL : entries;
    }

    public static SessionBranchQuery current() {
        return new SessionBranchQuery(null, null, null, SessionEntryQuery.ALL);
    }
}
