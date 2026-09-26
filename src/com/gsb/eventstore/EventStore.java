package com.gsb.eventstore;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A long-lived, append-only event store with explicit, step-by-step schema evolution.
 *
 * <p>Events are stored immutably at the version they were written with. On {@link #read},
 * each event is upcast one level at a time (v1 -&gt; v2 -&gt; v3, never skipping a level)
 * up to the highest version reachable through the registered upcasters. The stored bytes
 * are never modified by upcasting.
 */
public interface EventStore {

    /**
     * Appends an event to a stream. Versions within a stream must start at 1 and
     * increase by exactly 1 per event; gaps or duplicates are rejected.
     *
     * @param streamId identifies the stream; any non-empty string is allowed
     * @param version  expected version of this event (must equal current stream size + 1)
     * @param payload  event payload; values may only be String, Long, Boolean, Double,
     *                 List or Map (recursively), keys must be Strings
     */
    void append(String streamId, int version, Map<String, Object> payload);

    /**
     * Reads all events of a stream in write order, each upcast level by level to the
     * highest registered version. Events stored at a version higher than anything the
     * registered upcasters can reach are returned as stored.
     *
     * @throws MissingUpcasterException if a required single-step upcaster is not registered
     */
    List<Map<String, Object>> read(String streamId);

    /**
     * Registers a single-step upcaster. {@code toVersion} must equal
     * {@code fromVersion + 1}; multi-version jumps are rejected so that evolution
     * always happens level by level.
     */
    void registerUpcaster(int fromVersion, int toVersion,
                          Function<Map<String, Object>, Map<String, Object>> upcaster);
}
