package io.github.idoly.pi.agent.session;

public record SessionForkOptions(
        Scope scope,
        String entryId,
        Position position,
        String id,
        String parentSessionId
) {
    public SessionForkOptions {
        scope = scope == null ? Scope.BRANCH : scope;
        position = position == null
                ? entryId == null ? Position.AT : Position.BEFORE
                : position;
    }

    public enum Scope {
        BRANCH,
        TREE
    }

    public enum Position {
        BEFORE,
        AT
    }
}
