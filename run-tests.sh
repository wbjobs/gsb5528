#!/usr/bin/env bash
# Builds the event store (JDK 8 sources, no third-party libraries) and runs the
# acceptance tests. Requires only javac/java on PATH.
set -euo pipefail
cd "$(dirname "$0")"

BUILD_DIR="build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/classes" "$BUILD_DIR/test-classes"

echo "== Compiling src/ =="
find src -name '*.java' -print0 | xargs -0 javac -encoding UTF-8 -source 8 -target 8 \
    -d "$BUILD_DIR/classes"

echo "== Compiling tests/ =="
find tests -name '*.java' -print0 | xargs -0 javac -encoding UTF-8 -source 8 -target 8 \
    -cp "$BUILD_DIR/classes" -d "$BUILD_DIR/test-classes"

echo "== Running tests =="
java -cp "$BUILD_DIR/classes:$BUILD_DIR/test-classes" TestMain
