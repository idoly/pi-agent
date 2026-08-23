package io.github.idoly.pi.agent.session;

import java.util.Set;

/** Options for an administrative copy containing an exact set of source mutations. */
public record SessionRetainedCopyOptions(
        Set<Long> retainedSequences,
        String id,
        String parentSessionId
) {
    public SessionRetainedCopyOptions {
        retainedSequences = Set.copyOf(retainedSequences);
        for (long sequence : retainedSequences) {
            if (sequence <= 0) {
                throw new IllegalArgumentException(
                        "retained sequences must be positive"
                );
            }
        }
    }
}
