package com.gsb.eventstore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Append-only, file-backed event store with schema evolution via upcasters.
 *
 * Each stream is stored as one append-only file inside the store directory.
 * Events are written once and never modified; reads replay the file and
 * upcast each event in memory from its stored version up to the highest
 * registered schema version, one step at a time (v1 -&gt; v2 -&gt; v3, never
 * skipping). Events stored at a version higher than any registered upcaster
 * are returned as-is, so a reader with an older upcaster graph can still read
 * newer files without losing unknown fields.
 */
public final class EventStore {

    private final Path directory;
    private final Map<Integer, Function<Map<String, Object>, Map<String, Object>>> upcasters =
            new HashMap<Integer, Function<Map<String, Object>, Map<String, Object>>>();
    private int highestVersion;

    public EventStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create store directory " + directory, e);
        }
    }

    /**
     * Registers one upcasting step. Only adjacent steps are allowed
     * (toVersion must equal fromVersion + 1) so chains can never skip a level.
     */
    public synchronized void registerUpcaster(int fromVersion, int toVersion,
            Function<Map<String, Object>, Map<String, Object>> upcaster) {
        if (toVersion != fromVersion + 1) {
            throw new IllegalArgumentException(
                    "Upcasters must advance exactly one version, got " + fromVersion + " -> " + toVersion);
        }
        if (upcaster == null) {
            throw new IllegalArgumentException("upcaster must not be null");
        }
        upcasters.put(Integer.valueOf(fromVersion), upcaster);
        highestVersion = Math.max(highestVersion, toVersion);
    }

    /**
     * Appends an event to a stream. Versions within a stream must start at 1
     * and increase by exactly 1 per event; gaps or duplicates are rejected.
     */
    public synchronized void append(String streamId, int version, Map<String, Object> payload) {
        validateStreamId(streamId);
        validateValue(payload, "payload");
        Path file = streamFile(streamId);
        List<StoredEvent> existing = readRecords(file);
        int expected = existing.isEmpty() ? 1 : existing.get(existing.size() - 1).version + 1;
        if (version != expected) {
            throw new IllegalStateException("Stream '" + streamId + "' expects version " + expected
                    + " but got " + version + "; versions must be consecutive with no gaps");
        }
        byte[] record = encodeRecord(version, payload);
        try {
            Files.write(file, record, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot append to stream '" + streamId + "'", e);
        }
    }

    /**
     * Reads a stream, upcasting every event in memory to the highest
     * registered version. Stored bytes are never modified. Unknown fields are
     * carried through untouched.
     */
    public synchronized List<Map<String, Object>> read(String streamId) {
        validateStreamId(streamId);
        List<StoredEvent> stored = readRecords(streamFile(streamId));
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(stored.size());
        for (StoredEvent event : stored) {
            result.add(upcast(event.version, event.payload));
        }
        return result;
    }

    private Map<String, Object> upcast(int storedVersion, Map<String, Object> payload) {
        Map<String, Object> current = deepCopyMap(payload);
        int version = storedVersion;
        while (version < highestVersion) {
            Function<Map<String, Object>, Map<String, Object>> step = upcasters.get(Integer.valueOf(version));
            if (step == null) {
                throw new MissingUpcasterException(version, version + 1);
            }
            Map<String, Object> next = step.apply(current);
            if (next == null) {
                throw new IllegalStateException(
                        "Upcaster " + version + " -> " + (version + 1) + " returned null");
            }
            current = deepCopyMap(next);
            version++;
        }
        return current;
    }

    private static byte[] encodeRecord(int version, Map<String, Object> payload) {
        byte[] body = Codec.encode(payload);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write('V');
        String digits = Integer.toString(version);
        for (int i = 0; i < digits.length(); i++) {
            out.write((byte) digits.charAt(i));
        }
        out.write(';');
        out.write(body, 0, body.length);
        return out.toByteArray();
    }

    private static List<StoredEvent> readRecords(Path file) {
        List<StoredEvent> events = new ArrayList<StoredEvent>();
        if (!Files.exists(file)) {
            return events;
        }
        byte[] data;
        try {
            data = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read stream file " + file, e);
        }
        Codec.Decoder decoder = Codec.decoder(data);
        while (decoder.hasMore()) {
            decoder.expect('V');
            int version = decoder.readInt(';');
            Object value = decoder.readValue();
            if (!(value instanceof Map)) {
                throw new IllegalStateException("Corrupt data: event payload is not a map in " + file);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) value;
            events.add(new StoredEvent(version, payload));
        }
        return events;
    }

    private Path streamFile(String streamId) {
        return directory.resolve(streamId + ".events");
    }

    private static void validateStreamId(String streamId) {
        if (streamId == null || streamId.isEmpty()) {
            throw new IllegalArgumentException("streamId must not be null or empty");
        }
        for (int i = 0; i < streamId.length(); i++) {
            char c = streamId.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.';
            if (!ok) {
                throw new IllegalArgumentException("Illegal character '" + c + "' in streamId");
            }
        }
        if (streamId.equals(".") || streamId.equals("..")) {
            throw new IllegalArgumentException("Illegal streamId: " + streamId);
        }
    }

    private static void validateValue(Object value, String path) {
        if (value instanceof String || value instanceof Long
                || value instanceof Boolean || value instanceof Double) {
            return;
        }
        if (value instanceof List) {
            int i = 0;
            for (Object element : (List<?>) value) {
                validateValue(element, path + "[" + i + "]");
                i++;
            }
            return;
        }
        if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException(
                            "Map keys must be Strings at " + path + ", got " + entry.getKey());
                }
                validateValue(entry.getValue(), path + "." + entry.getKey());
            }
            return;
        }
        throw new IllegalArgumentException("Unsupported value type at " + path + ": "
                + (value == null ? "null" : value.getClass().getName())
                + " (allowed: String, Long, Boolean, Double, List, Map)");
    }

    private static Map<String, Object> deepCopyMap(Map<String, Object> map) {
        Map<String, Object> copy = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            copy.put(entry.getKey(), deepCopy(entry.getValue()));
        }
        return copy;
    }

    private static Object deepCopy(Object value) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            return deepCopyMap(map);
        }
        if (value instanceof List) {
            List<Object> copy = new ArrayList<Object>();
            for (Object element : (List<?>) value) {
                copy.add(deepCopy(element));
            }
            return copy;
        }
        return value;
    }

    private static final class StoredEvent {
        final int version;
        final Map<String, Object> payload;

        StoredEvent(int version, Map<String, Object> payload) {
            this.version = version;
            this.payload = payload;
        }
    }

}
