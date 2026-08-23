package io.github.idoly.pi.agent.session;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks Java session-level extensions that are not part of the upstream pi 0.84.2
 * compatibility surface and may evolve before this project reaches 1.0.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR})
public @interface ExperimentalSessionApi {
}
