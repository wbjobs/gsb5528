package com.gsb.eventstore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * File-backed {@link EventStore}. Each stream is one append-only file in a directory;
 * events are never rewritten or deleted, so historical payloads stay byte-for-byte
 * intact no matter how the schema evolves.
 *
 * <p>File layout: a fixed header followed by concatenated records
 * {@code V<version>;<encoded-payload>}. Records are self-delimiting, so a file can be
 * re-scanned from scratch at any time — e.g. by a new JVM with a different upcaster
 * graph reading the same directory.
 */
public final class FileEventStore implements EventStore {

    private static final String HEADER = "GSBES1;";
    private static final String FILE_SUFFIX = ".events";

    private final Path directory;
    private final Map<Integer, Function<Map<String, Object>, Map<String, Object>>> upcasters =
            new HashMap<Integer, Function<Map<String, Object>, Map<String, Object>>>();
    private final Map<String, Integer> nextVersions = new HashMap<String, Integer>();
    private int maxTargetVersion;

    public FileEventStore(String directory) {
        this(Paths.get(directory));
    }

    public FileEventStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create store directory " + directory, e);
        }
    }

    @Override
    public synchronized void append(String streamId, int version, Map<String, Object> payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        String encoded = Codec.encode(payload); // validates allowed types before touching disk
        int expected = nextVersion(streamId);
        if (version != expected) {
            throw new IllegalArgumentException("Stream '" + streamId + "': versions must be "
                    + "consecutive starting at 1; expected " + expected + " but got " + version);
        }
        Path file = fileFor(streamId);
        boolean isNew = !Files.exists(file);
        StringBuilder record = new StringBuilder();
        if (isNew) {
            record.append(HEADER);
        }
        record.append('V').append(version).append(';').append(encoded);
        try {
            Files.write(file, record.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot append to " + file, e);
        }
        nextVersions.put(streamId, Integer.valueOf(version + 1));
    }

    @Override
    public synchronized List<Map<String, Object>> read(String streamId) {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        Path file = fileFor(streamId);
        if (!Files.exists(file)) {
            return events;
        }
        String content;
        try {
            content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
        Codec.Parser parser = new Codec.Parser(content);
        parser.expect(HEADER);
        while (!parser.atEnd()) {
            parser.expectChar('V');
            int version = (int) parser.parseLong(';');
            Object value = parser.parseValue();
            if (!(value instanceof Map)) {
                throw new IllegalStateException(
                        "Corrupt data in " + file + ": event payload is not a map");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> stored = (Map<String, Object>) value;
            events.add(upcast(version, stored));
        }
        return events;
    }

    @Override
    public synchronized void registerUpcaster(int fromVersion, int toVersion,
            Function<Map<String, Object>, Map<String, Object>> upcaster) {
        if (upcaster == null) {
            throw new IllegalArgumentException("upcaster must not be null");
        }
        if (toVersion != fromVersion + 1) {
            throw new IllegalArgumentException("Upcasters must be single-step (N -> N+1); "
                    + "got " + fromVersion + " -> " + toVersion
                    + ". Register one upcaster per level instead of jumping versions.");
        }
        upcasters.put(Integer.valueOf(fromVersion), upcaster);
        if (toVersion > maxTargetVersion) {
            maxTargetVersion = toVersion;
        }
    }

    /**
     * Upcasts one event level by level up to the highest registered version. Events
     * already at or above that version are returned untouched, so a reader with an
     * older upcaster graph can still read files written by a newer application and
     * keeps every newer field. The stored bytes are never modified.
     */
    private Map<String, Object> upcast(int storedVersion, Map<String, Object> payload) {
        Map<String, Object> current = payload;
        int version = storedVersion;
        while (version < maxTargetVersion) {
            Function<Map<String, Object>, Map<String, Object>> step =
                    upcasters.get(Integer.valueOf(version));
            if (step == null) {
                throw new MissingUpcasterException(version, version + 1);
            }
            current = step.apply(current);
            if (current == null) {
                throw new IllegalStateException("Upcaster " + version + " -> " + (version + 1)
                        + " returned null");
            }
            version++;
        }
        return current;
    }

    private int nextVersion(String streamId) {
        Integer cached = nextVersions.get(streamId);
        if (cached != null) {
            return cached.intValue();
        }
        int next = 1;
        Path file = fileFor(streamId);
        if (Files.exists(file)) {
            String content;
            try {
                content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read " + file, e);
            }
            Codec.Parser parser = new Codec.Parser(content);
            parser.expect(HEADER);
            int last = 0;
            while (!parser.atEnd()) {
                parser.expectChar('V');
                last = (int) parser.parseLong(';');
                parser.parseValue();
            }
            next = last + 1;
        }
        nextVersions.put(streamId, Integer.valueOf(next));
        return next;
    }

    /** Maps any stream id to a safe file name: safe chars are kept, others become %XXXX. */
    private Path fileFor(String streamId) {
        if (streamId == null || streamId.isEmpty()) {
            throw new IllegalArgumentException("streamId must not be null or empty");
        }
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < streamId.length(); i++) {
            char c = streamId.charAt(i);
            boolean safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
            if (safe) {
                name.append(c);
            } else {
                name.append('%');
                String hex = Integer.toHexString(c);
                for (int pad = hex.length(); pad < 4; pad++) {
                    name.append('0');
                }
                name.append(hex);
            }
        }
        return directory.resolve(name.toString() + FILE_SUFFIX);
    }
}
