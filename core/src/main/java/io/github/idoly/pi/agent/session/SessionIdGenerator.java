package io.github.idoly.pi.agent.session;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

@FunctionalInterface
public interface SessionIdGenerator {
    String next(Long timestampMillis);

    default String next() {
        return next(null);
    }

    static SessionIdGenerator uuidV7() {
        return uuidV7(Clock.systemUTC(), new SecureRandom());
    }

    static SessionIdGenerator uuidV7(Clock clock, SecureRandom random) {
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(random, "random");
        return timestamp -> {
            long millis = timestamp == null ? clock.millis() : timestamp;
            if (millis < 0 || millis > 0xffffffffffffL) {
                throw new IllegalArgumentException("UUIDv7 timestamp is out of range");
            }
            long most = (millis << 16) | 0x7000L | random.nextInt(1 << 12);
            long least = random.nextLong();
            least = (least & 0x3fffffffffffffffL) | 0x8000000000000000L;
            return new UUID(most, least).toString();
        };
    }
}
