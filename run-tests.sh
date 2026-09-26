#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

OUT_DIR="out"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "Compiling..."
find src -name '*.java' -print0 | xargs -0 javac -encoding UTF-8 -source 8 -target 8 -d "$OUT_DIR"

echo "Running tests..."
java -cp "$OUT_DIR" com.gsb.eventstore.tests.EventStoreTests
