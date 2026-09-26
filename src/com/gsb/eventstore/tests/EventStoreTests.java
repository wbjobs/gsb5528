package com.gsb.eventstore.tests;

import com.gsb.eventstore.EventStore;
import com.gsb.eventstore.MissingUpcasterException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Self-contained test runner (no JUnit). Prints one line per test and exits
 * with a non-zero status if anything failed.
 */
public final class EventStoreTests {

    private static int passed;
    private static int failed;

    public static void main(String[] args) throws Exception {
        testV1EventUpcastToV3Shape();
        testPartialGraphReadsV2ShapeWithoutError();
        testMissingUpcasterThrowsWithVersions();
        testUnknownFieldsSurviveUpgradeAndWriteBack();
        testDeepEqualityRoundTrip();
        testVersionContinuityEnforced();
        testStoredEventsNeverModifiedByReads();
        testReturnedDataIsDecoupledFromStore();
        testRejectsUnsupportedValueTypes();
        testMultipleStreamsAreIndependent();

        System.out.println();
        System.out.println("Passed: " + passed + ", Failed: " + failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    /** v1 written, graph v1->v2->v3 registered: read must yield the v3 shape. */
    private static void testV1EventUpcastToV3Shape() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);
        store.registerUpcaster(1, 2, upcasterV1toV2());
        store.registerUpcaster(2, 3, upcasterV2toV3());

        Map<String, Object> v1 = map();
        v1.put("name", "order-1");
        v1.put("amount", 100L);
        store.append("orders", 1, v1);

        List<Map<String, Object>> events = store.read("orders");
        check("v1 -> v3: one event", events.size() == 1);
        Map<String, Object> e = events.get(0);
        check("v1 -> v3: original fields kept",
                "order-1".equals(e.get("name")) && Long.valueOf(100L).equals(e.get("amount")));
        check("v1 -> v3: v2 field present", "v2-added".equals(e.get("v2field")));
        check("v1 -> v3: v3 field present", "v3-added".equals(e.get("v3field")));
        check("v1 -> v3: v2 transformation applied before v3",
                "v2-added|order-1".equals(e.get("v3derived")));
    }

    /** Same file, graph registered only up to v2: must read the v2 shape without error. */
    private static void testPartialGraphReadsV2ShapeWithoutError() throws Exception {
        Path dir = freshDir();
        EventStore writer = new EventStore(dir);
        writer.registerUpcaster(1, 2, upcasterV1toV2());
        writer.registerUpcaster(2, 3, upcasterV2toV3());
        Map<String, Object> v1 = map();
        v1.put("name", "order-9");
        writer.append("orders", 1, v1);

        EventStore reader = new EventStore(dir);
        reader.registerUpcaster(1, 2, upcasterV1toV2());
        List<Map<String, Object>> events = reader.read("orders");
        check("v2-only graph: one event", events.size() == 1);
        Map<String, Object> e = events.get(0);
        check("v2-only graph: v2 field present", "v2-added".equals(e.get("v2field")));
        check("v2-only graph: v3 field absent", !e.containsKey("v3field"));
        check("v2-only graph: original field kept", "order-9".equals(e.get("name")));
    }

    /** A gap in the chain must throw MissingUpcasterException naming the missing step. */
    private static void testMissingUpcasterThrowsWithVersions() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);
        Map<String, Object> v1 = map();
        v1.put("name", "x");
        store.append("s", 1, v1);

        store.registerUpcaster(1, 2, upcasterV1toV2());
        store.registerUpcaster(3, 4, new Function<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> apply(Map<String, Object> m) {
                return m;
            }
        });

        try {
            store.read("s");
            check("missing upcaster: exception thrown", false);
        } catch (MissingUpcasterException e) {
            String msg = e.getMessage();
            check("missing upcaster: exception thrown", true);
            check("missing upcaster: message names step 2 -> 3",
                    msg != null && msg.contains("2") && msg.contains("3"));
            check("missing upcaster: getters expose versions",
                    e.getFromVersion() == 2 && e.getToVersion() == 3);
        }
    }

    /** Unknown fields must survive an upcast round-trip and a write-back. */
    private static void testUnknownFieldsSurviveUpgradeAndWriteBack() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);
        store.registerUpcaster(1, 2, upcasterV1toV2());

        Map<String, Object> mystery = map();
        mystery.put("str", "keep:me;intact");
        mystery.put("num", 7L);
        mystery.put("list", new ArrayList<Object>(Arrays.asList("a", 2L, Boolean.TRUE, 2.5d)));
        Map<String, Object> nested = map();
        nested.put("deep", "value");
        mystery.put("nested", nested);

        Map<String, Object> v1 = map();
        v1.put("name", "order-42");
        v1.put("mystery", mystery);
        store.append("orders", 1, v1);

        Map<String, Object> upgraded = store.read("orders").get(0);
        check("unknown fields: present after upcast", deepEquals(mystery, upgraded.get("mystery")));

        // Write the upgraded (v2) payload back as the next event, then re-read.
        store.append("orders", 2, upgraded);
        List<Map<String, Object>> events = store.read("orders");
        check("unknown fields: two events after write-back", events.size() == 2);
        check("unknown fields: intact after write-back", deepEquals(mystery, events.get(1).get("mystery")));
        check("unknown fields: first event still intact", deepEquals(mystery, events.get(0).get("mystery")));
    }

    /** Every supported type must come back deep-equal after write + read. */
    private static void testDeepEqualityRoundTrip() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);

        Map<String, Object> inner = map();
        inner.put("flag", Boolean.FALSE);
        inner.put("ratio", -0.125d);
        inner.put("tags", new ArrayList<Object>(Arrays.asList("x", "y:z", "line1\nline2")));

        Map<String, Object> payload = map();
        payload.put("text", "héllo wörld :;{} 你好");
        payload.put("count", 123456789012L);
        payload.put("negative", -1L);
        payload.put("active", Boolean.TRUE);
        payload.put("score", 3.14159d);
        payload.put("emptyList", new ArrayList<Object>());
        payload.put("emptyMap", map());
        payload.put("mixed", new ArrayList<Object>(Arrays.asList("s", 1L, Boolean.FALSE, 0.5d, inner)));
        payload.put("inner", inner);

        store.append("all-types", 1, payload);
        Map<String, Object> readBack = store.read("all-types").get(0);
        check("round trip: deep equal", deepEquals(payload, readBack));
    }

    /** Versions per stream must start at 1 and be consecutive. */
    private static void testVersionContinuityEnforced() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);
        Map<String, Object> p = map();
        p.put("a", 1L);

        expectIllegal("version: first event must be v1", new Runnable() {
            public void run() {
                store.append("s", 2, p);
            }
        });
        store.append("s", 1, p);
        expectIllegal("version: gap not allowed", new Runnable() {
            public void run() {
                store.append("s", 3, p);
            }
        });
        expectIllegal("version: duplicate not allowed", new Runnable() {
            public void run() {
                store.append("s", 1, p);
            }
        });
        store.append("s", 2, p);
        check("version: consecutive appends accepted", store.read("s").size() == 2);
    }

    /** Upcasting on read must never rewrite the stored bytes. */
    private static void testStoredEventsNeverModifiedByReads() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);
        store.registerUpcaster(1, 2, upcasterV1toV2());
        store.registerUpcaster(2, 3, upcasterV2toV3());

        Map<String, Object> v1 = map();
        v1.put("name", "immutable");
        store.append("s", 1, v1);

        byte[] before = Files.readAllBytes(dir.resolve("s.events"));
        store.read("s");
        store.read("s");
        byte[] after = Files.readAllBytes(dir.resolve("s.events"));
        check("immutability: file bytes unchanged by reads", Arrays.equals(before, after));

        // A fresh store with no upcasters must still see the original v1 shape.
        EventStore raw = new EventStore(dir);
        Map<String, Object> e = raw.read("s").get(0);
        check("immutability: raw read has no v2/v3 fields",
                !e.containsKey("v2field") && !e.containsKey("v3field") && "immutable".equals(e.get("name")));
    }

    /** Mutating returned structures or the source map must not affect the store. */
    private static void testReturnedDataIsDecoupledFromStore() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);

        Map<String, Object> payload = map();
        payload.put("name", "original");
        store.append("s", 1, payload);
        payload.put("name", "mutated-after-append");

        Map<String, Object> first = store.read("s").get(0);
        check("decoupling: append copies the payload", "original".equals(first.get("name")));

        first.put("junk", "junk");
        Map<String, Object> second = store.read("s").get(0);
        check("decoupling: mutating read result does not leak", !second.containsKey("junk"));
    }

    /** Only String, Long, Boolean, Double, List, Map values are accepted. */
    private static void testRejectsUnsupportedValueTypes() throws Exception {
        Path dir = freshDir();
        final EventStore store = new EventStore(dir);

        final Map<String, Object> withInteger = map();
        withInteger.put("n", Integer.valueOf(5));
        expectIllegal("types: Integer rejected", new Runnable() {
            public void run() {
                store.append("s", 1, withInteger);
            }
        });

        final Map<String, Object> withNull = map();
        withNull.put("n", null);
        expectIllegal("types: null rejected", new Runnable() {
            public void run() {
                store.append("s", 1, withNull);
            }
        });

        final Map<String, Object> withNestedInteger = map();
        withNestedInteger.put("list", new ArrayList<Object>(Arrays.asList("ok", Integer.valueOf(1))));
        expectIllegal("types: nested Integer rejected", new Runnable() {
            public void run() {
                store.append("s", 1, withNestedInteger);
            }
        });
    }

    /** Streams must not interfere with each other. */
    private static void testMultipleStreamsAreIndependent() throws Exception {
        Path dir = freshDir();
        EventStore store = new EventStore(dir);
        Map<String, Object> p = map();
        p.put("k", "v");
        store.append("a", 1, p);
        store.append("a", 2, p);
        store.append("b", 1, p);
        check("streams: independent version counters",
                store.read("a").size() == 2 && store.read("b").size() == 1);
        check("streams: unknown stream reads empty", store.read("nope").isEmpty());
    }

    // ------------------------------------------------------------------
    // Upcasters used by the tests
    // ------------------------------------------------------------------

    private static Function<Map<String, Object>, Map<String, Object>> upcasterV1toV2() {
        return new Function<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> apply(Map<String, Object> m) {
                Map<String, Object> r = new LinkedHashMap<String, Object>(m);
                r.put("v2field", "v2-added");
                return r;
            }
        };
    }

    private static Function<Map<String, Object>, Map<String, Object>> upcasterV2toV3() {
        return new Function<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> apply(Map<String, Object> m) {
                Map<String, Object> r = new LinkedHashMap<String, Object>(m);
                r.put("v3field", "v3-added");
                // Depends on the field added by the v1 -> v2 step, so this only
                // produces the expected value if the chain ran v1 -> v2 -> v3.
                Object v2field = r.get("v2field");
                if (v2field instanceof String) {
                    r.put("v3derived", ((String) v2field) + "|" + r.get("name"));
                }
                return r;
            }
        };
    }

    // ------------------------------------------------------------------
    // Assertions and helpers
    // ------------------------------------------------------------------

    private static Path freshDir() throws Exception {
        Path dir = Files.createTempDirectory("eventstore-test");
        dir.toFile().deleteOnExit();
        return dir;
    }

    private static Map<String, Object> map() {
        return new LinkedHashMap<String, Object>();
    }

    private static void check(String name, boolean condition) {
        if (condition) {
            passed++;
            System.out.println("PASS  " + name);
        } else {
            failed++;
            System.out.println("FAIL  " + name);
        }
    }

    private static void expectIllegal(String name, Runnable action) {
        try {
            action.run();
            check(name, false);
        } catch (IllegalStateException e) {
            check(name, true);
        } catch (IllegalArgumentException e) {
            check(name, true);
        }
    }

    private static boolean deepEquals(Object a, Object b) {
        if (a instanceof Map && b instanceof Map) {
            Map<?, ?> ma = (Map<?, ?>) a;
            Map<?, ?> mb = (Map<?, ?>) b;
            if (ma.size() != mb.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : ma.entrySet()) {
                if (!mb.containsKey(entry.getKey()) || !deepEquals(entry.getValue(), mb.get(entry.getKey()))) {
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
        return a == null ? b == null : a.equals(b);
    }
}
