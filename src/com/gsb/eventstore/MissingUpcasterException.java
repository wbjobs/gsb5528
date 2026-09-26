package com.gsb.eventstore;

/**
 * Thrown by {@link EventStore#read} when an event must be upcast from version N to
 * version N+1 but no upcaster is registered for that step. The message always names
 * both versions of the missing step.
 */
public class MissingUpcasterException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int fromVersion;
    private final int toVersion;

    public MissingUpcasterException(int fromVersion, int toVersion) {
        super("Missing upcaster from version " + fromVersion + " to version " + toVersion);
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    public int getFromVersion() {
        return fromVersion;
    }

    public int getToVersion() {
        return toVersion;
    }
}
