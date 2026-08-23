package io.github.idoly.pi.ai;

import java.util.Objects;

public sealed interface ConstrainedSampling permits ConstrainedSampling.JsonSchema, ConstrainedSampling.Grammar {
    record JsonSchema(Strictness strictness) implements ConstrainedSampling {
        public JsonSchema {
            Objects.requireNonNull(strictness, "strictness");
        }
    }

    record Grammar(String openAiLark, String openAiRegex) implements ConstrainedSampling {
    }

    enum Strictness {
        PREFER,
        REQUIRE
    }
}
