package io.github.idoly.pi.agent;

public record BeforeToolCallResult(boolean block, String reason, boolean terminate) {
    public static BeforeToolCallResult allow() {
        return new BeforeToolCallResult(false, null, false);
    }

    public static BeforeToolCallResult block(String reason) {
        return new BeforeToolCallResult(true, reason, false);
    }
}
