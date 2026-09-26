package com.gsb.eventstore;

/**
 * Thrown when reading an event requires an upcasting step that has not been
 * registered. The message always names the missing step, e.g.
 * "Missing upcaster from version 2 to version 3".
 */
public class MissingUpcasterException extends RuntimeException {

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
