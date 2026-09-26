import com.gsb.eventstore.EventStore;
import com.gsb.eventstore.FileEventStore;
import com.gsb.eventstore.MissingUpcasterException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public final class TestMain {

    private static int passed;
    private static int failed;

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("eventstore-test");
        try {
            testChainedUpgradeToV3(root.resolve("t1"));
            testPartialGraphReadsV2AndKeepsNewerFields(root.resolve("t2"));
            testMissingUpcasterNamesVersions(root.resolve("t3"));
            testUnknownFieldsSurviveUpgradeAndWriteBack(root.resolve("t4"));
            testDeepEqualityRoundTrip(root.resolve("t5"));
            testVersionContinuityEnforced(root.resolve("t6"));
            testReadsNeverModifyFiles(root.resolve("t7"));
            testRejectsMultiStepUpcaster(root.resolve("t8"));
        } finally {
            deleteRecursively(root);
        }
        System.out.println();
        System.out.println("passed=" + passed + " failed=" + failed);
        if (failed > 0) {
            System.exit(1);
        }
        System.out.println("ALL TESTS PASSED");
    }

    /** v1 event, graph 1->2->3 registered: read yields v3 shape, both steps applied in order. */
    private static void testChainedUpgradeToV3(Path dir) {
        EventStore store = new FileEventStore(dir);
        Map<String, Object> v1 = map(
                "type", "OrderCreated",
                "amount", 100L);
        store.append("orders", 1, v1);
        store.registerUpcaster(1, 2, addField("currency", "CNY"));
        // 2->3 depends on the field added by 1->2, proving the chain runs in order.
        store.registerUpcaster(2, 3, payload -> {
            Map<String, Object> copy = copyOf(payload);
            copy.put("label", payload.get("amount") + ":" + payload.get("currency"));
            return copy;
        });

        List<Map<String, Object>> events = store.read("orders");
        check("t1: one event", events.size() == 1);
        Map<String, Object> event = events.get(0);
        check("t1: original fields kept",
                Objects.equals(event.get("type"), "OrderCreated")
                        && Objects.equals(event.get("amount"), 100L));
        check("t1: v2 field present", Objects.equals(event.get("currency"), "CNY"));
        check("t1: v3 field derived from v2 output",
                Objects.equals(event.get("label"), "100:CNY"));

        // A brand-new store instance over the same files must see the same v3 shape.
        EventStore reopened = new FileEventStore(dir);
        reopened.registerUpcaster(1, 2, addField("currency", "CNY"));
        reopened.registerUpcaster(2, 3, payload -> {
            Map<String, Object> copy = copyOf(payload);
            copy.put("label", payload.get("amount") + ":" + payload.get("currency"));
            return copy;
        });
        check("t1: reopened store agrees", deepEquals(events, reopened.read("orders")));
    }

    /** Same file, graph registered only up to v2: reads v2 shape without errors;
     *  events already stored at v3 keep their newer fields. */
    private static void testPartialGraphReadsV2AndKeepsNewerFields(Path dir) {
        EventStore writer = new FileEventStore(dir);
        writer.append("orders", 1, map("type", "OrderCreated", "amount", 42L));
        // A "newer application" writes an event already in v3 shape, with a field
        // that the old graph knows nothing about.
        writer.append("orders", 2, map(
                "type", "OrderCreated",
                "amount", 7L,
                "currency", "CNY",
                "label", "7:CNY",
                "futureField", "from-the-future"));

        EventStore reader = new FileEventStore(dir);
        reader.registerUpcaster(1, 2, addField("currency", "CNY"));

        List<Map<String, Object>> events = reader.read("orders");
        check("t2: two events", events.size() == 2);
        Map<String, Object> first = events.get(0);
        check("t2: v1 event upcast to v2 shape",
                Objects.equals(first.get("currency"), "CNY")
                        && Objects.equals(first.get("amount"), 42L)
                        && !first.containsKey("label"));
        Map<String, Object> second = events.get(1);
        check("t2: newer stored event returned as-is, new fields preserved",
                Objects.equals(second.get("futureField"), "from-the-future")
                        && Objects.equals(second.get("label"), "7:CNY"));
    }

    /** Only 2->3 registered; reading a v1 event must fail naming the missing 1->2 step. */
    private static void testMissingUpcasterNamesVersions(Path dir) {
        EventStore store = new FileEventStore(dir);
        store.append("orders", 1, map("type", "OrderCreated"));
        store.registerUpcaster(2, 3, addField("label", "x"));
        try {
            store.read("orders");
            check("t3: MissingUpcasterException thrown", false);
        } catch (MissingUpcasterException e) {
            String message = String.valueOf(e.getMessage());
            check("t3: message names missing step 1 -> 2",
                    message.contains("1") && message.contains("2")
                            && e.getFromVersion() == 1 && e.getToVersion() == 2);
        }
    }

    /** Unknown fields (incl. nested) survive an upgrade round and a write-back. */
    private static void testUnknownFieldsSurviveUpgradeAndWriteBack(Path dir) {
        EventStore store = new FileEventStore(dir);
        Map<String, Object> v1 = map(
                "type", "OrderCreated",
                "mystery", "keep-me",
                "meta", map("odd", list(1L, 2L), "note", "unknown to every upcaster"));
        store.append("legacy", 1, v1);
        // Upcasters only touch their own fields; unknown ones must ride along.
        store.registerUpcaster(1, 2, addField("currency", "CNY"));
        store.registerUpcaster(2, 3, addField("label", "done"));

        Map<String, Object> upgraded = store.read("legacy").get(0);
        check("t4: unknown fields present after upgrade to v3",
                Objects.equals(upgraded.get("mystery"), "keep-me")
                        && deepEquals(upgraded.get("meta"),
                                map("odd", list(1L, 2L), "note", "unknown to every upcaster")));

        // Write the upgraded payload back as the next version, then read everything again.
        store.append("legacy", 2, upgraded);
        EventStore reopened = new FileEventStore(dir);
        reopened.registerUpcaster(1, 2, addField("currency", "CNY"));
        reopened.registerUpcaster(2, 3, addField("label", "done"));
        List<Map<String, Object>> events = reopened.read("legacy");
        check("t4: two events after write-back", events.size() == 2);
        for (int i = 0; i < events.size(); i++) {
            Map<String, Object> event = events.get(i);
            check("t4: event " + (i + 1) + " kept unknown fields after write-back",
                    Objects.equals(event.get("mystery"), "keep-me")
                            && deepEquals(event.get("meta"),
                                    map("odd", list(1L, 2L), "note", "unknown to every upcaster")));
        }
    }

    /** Payload with all six types (nested, with hostile strings) reads back deep-equal. */
    private static void testDeepEqualityRoundTrip(Path dir) {
        EventStore store = new FileEventStore(dir);
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("text", "héllo\nwörld: {S3:x} M1{}\t\"quoted\"");
        payload.put("empty", "");
        payload.put("count", 123456789012345L);
        payload.put("negative", -7L);
        payload.put("flag", Boolean.TRUE);
        payload.put("otherFlag", Boolean.FALSE);
        payload.put("ratio", 3.141592653589793d);
        payload.put("negativeZero", -0.0d);
        payload.put("items", list("a", 1L, Boolean.FALSE, 2.5d,
                list("nested"), map("k", "v")));
        payload.put("emptyList", new ArrayList<Object>());
        payload.put("emptyMap", new LinkedHashMap<String, Object>());
        payload.put("nested", map("level2", map("level3", list(map("deep", Boolean.TRUE)))));

        store.append("types", 1, payload);
        List<Map<String, Object>> events = store.read("types");
        check("t5: one event", events.size() == 1);
        check("t5: payload deep-equals original", deepEquals(events.get(0), payload));

        EventStore reopened = new FileEventStore(dir);
        check("t5: deep-equal after reopen", deepEquals(reopened.read("types").get(0), payload));
    }

    /** Versions must start at 1 and increment by 1; gaps and duplicates are rejected. */
    private static void testVersionContinuityEnforced(Path dir) {
        EventStore store = new FileEventStore(dir);
        expectIllegal("t6: first event must be v1",
                () -> store.append("seq", 2, map("a", 1L)));
        store.append("seq", 1, map("a", 1L));
        expectIllegal("t6: gap to v3 rejected",
                () -> store.append("seq", 3, map("a", 3L)));
        expectIllegal("t6: duplicate v1 rejected",
                () -> store.append("seq", 1, map("a", 1L)));
        store.append("seq", 2, map("a", 2L));
        check("t6: two events stored", store.read("seq").size() == 2);

        // Continuity is also enforced for a fresh instance over existing files.
        EventStore reopened = new FileEventStore(dir);
        expectIllegal("t6: reopened store still enforces continuity",
                () -> reopened.append("seq", 4, map("a", 4L)));
        reopened.append("seq", 3, map("a", 3L));
        check("t6: three events after reopen", reopened.read("seq").size() == 3);
    }

    /** Reading (with upcasting) must never modify the stored bytes. */
    private static void testReadsNeverModifyFiles(Path dir) {
        EventStore store = new FileEventStore(dir);
        store.append("orders", 1, map("type", "OrderCreated", "amount", 9L));
        byte[] before = readOnlyFile(dir);
        store.registerUpcaster(1, 2, addField("currency", "CNY"));
        store.read("orders");
        store.read("orders");
        byte[] after = readOnlyFile(dir);
        check("t7: file bytes unchanged by upcasting reads", Arrays.equals(before, after));
    }

    /** Registering a multi-version jump is rejected; evolution is level by level. */
    private static void testRejectsMultiStepUpcaster(Path dir) {
        EventStore store = new FileEventStore(dir);
        expectIllegal("t8: 1 -> 3 jump rejected",
                () -> store.registerUpcaster(1, 3, addField("x", "y")));
    }

    // ---------------------------------------------------------------- helpers

    private static Function<Map<String, Object>, Map<String, Object>> addField(
            String key, Object value) {
        return payload -> {
            Map<String, Object> copy = copyOf(payload);
            copy.put(key, value);
            return copy;
        };
    }

    private static Map<String, Object> copyOf(Map<String, Object> payload) {
        return new LinkedHashMap<String, Object>(payload);
    }

    private static Map<String, Object> map(Object... keysAndValues) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    private static List<Object> list(Object... items) {
        return new ArrayList<Object>(Arrays.asList(items));
    }

    static boolean deepEquals(Object a, Object b) {
        if (a instanceof Map && b instanceof Map) {
            Map<?, ?> ma = (Map<?, ?>) a;
            Map<?, ?> mb = (Map<?, ?>) b;
            if (ma.size() != mb.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : ma.entrySet()) {
                if (!mb.containsKey(entry.getKey())
                        || !deepEquals(entry.getValue(), mb.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;
            if (la.size() != lb.size()) {
                return false;
            }
            for (int i = 0; i < la.size(); i++) {
                if (!deepEquals(la.get(i), lb.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof Double && b instanceof Double) {
            return Double.compare((Double) a, (Double) b) == 0;
        }
        return Objects.equals(a, b);
    }

    private static byte[] readOnlyFile(Path dir) {
        try {
            Path file = dir.resolve("orders.events");
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void expectIllegal(String name, Runnable action) {
        try {
            action.run();
            check(name, false);
        } catch (IllegalArgumentException expected) {
            check(name, true);
        }
    }

    private static void check(String name, boolean condition) {
        if (condition) {
            passed++;
            System.out.println("PASS " + name);
        } else {
            failed++;
            System.out.println("FAIL " + name);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

}
