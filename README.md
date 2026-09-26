# EventStore

A long-lived, append-only event store with schema evolution. Pure JDK 8
standard library — no Maven/Gradle, no third-party dependencies, no JSON
library.

## Layout

- `src/com/gsb/eventstore/EventStore.java` — public API
- `src/com/gsb/eventstore/Codec.java` — stable self-delimiting text encoding
- `src/com/gsb/eventstore/MissingUpcasterException.java` — gap in the upcaster chain
- `src/com/gsb/eventstore/tests/EventStoreTests.java` — self-contained test runner
- `run-tests.sh` — compiles with `javac` and runs the tests

## API

```java
EventStore store = new EventStore(Paths.get("data"));
store.registerUpcaster(1, 2, upcaster); // Function<Map<String,Object>, Map<String,Object>>
store.append("orders", 1, payload);     // versions start at 1, strictly consecutive
List<Map<String, Object>> events = store.read("orders");
```

Payload values may only be `String`, `Long`, `Boolean`, `Double`, `List`,
`Map` (with `String` keys); anything else is rejected on append.

## Evolution rules

- Reads upcast each event in memory from its stored version up to the highest
  registered version, one step at a time (v1 -> v2 -> v3, never skipping).
- A missing step throws `MissingUpcasterException` naming the step
  (e.g. `Missing upcaster from version 2 to version 3`).
- Only adjacent steps can be registered (`toVersion == fromVersion + 1`).
- Stored bytes are never modified; upcasting happens only in memory on read.
- Unknown fields are carried through untouched, and events stored at a version
  higher than any registered upcaster are returned as-is — so a reader with a
  v2-only graph can read a file written by a v3-aware application.

## Storage format

One append-only file per stream (`<streamId>.events`). Each record is
`V<version>;` followed by a self-delimiting encoded payload:

```
S<len>:<utf8>   String      L<digits>;    Long        B<t|f>;  Boolean
D<double>;      Double      A<n>:values   List        M<n>:pairs  Map
```

Length/count prefixes mean no escaping is ever needed and records can be
decoded sequentially.

## Run the tests

```sh
./run-tests.sh
```
