# Event Store (com.gsb.eventstore)

Append-only, file-backed event store with explicit step-by-step schema evolution.
JDK 8, standard library only — no Maven/Gradle, no third-party dependencies.

## Layout

- `src/com/gsb/eventstore/EventStore.java` — public API (`append` / `read` / `registerUpcaster`)
- `src/com/gsb/eventstore/FileEventStore.java` — file-backed implementation (one append-only file per stream)
- `src/com/gsb/eventstore/Codec.java` — stable length-prefixed text encoding (no JSON library)
- `src/com/gsb/eventstore/MissingUpcasterException.java` — thrown when a level is missing
- `tests/TestMain.java` — acceptance tests (plain `main`, no JUnit)
- `run-tests.sh` — compiles with `javac` and runs the tests

## Guarantees

- Versions within a stream start at 1 and increase by exactly 1; gaps/duplicates are rejected.
- Stored events are immutable; upcasting happens only in memory during `read`.
- `read` upcasts level by level (v1 -> v2 -> v3, never skipping) up to the highest
  registered version; a missing level throws `MissingUpcasterException` naming the step.
- A reader with an older upcaster graph (e.g. only up to v2) can read files written by
  a newer application: newer events are returned as stored, new fields preserved.
- Unknown fields are carried through upgrades untouched.
- Payload values are restricted to String, Long, Boolean, Double, List, Map.

## Encoding

Self-delimiting tagged text: `S<len>:<chars>` strings (length-prefixed, no escaping),
`L<n>;` longs, `B0|B1` booleans, `D<n>;` doubles, `A<n>{...}` lists, `M<n>{...}` maps.
Each stream file is `GSBES1;` followed by `V<version>;<payload>` records.

## Run

```sh
./run-tests.sh
```
