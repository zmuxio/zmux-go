#!/usr/bin/env bash
# Runs every native fuzz target of the root module for the given time each
# (default 10s). A failing input is written to the package's testdata/fuzz
# and kept there as a regression seed.
set -euo pipefail
cd "$(dirname "$0")/.."
time=${1:-10s}
targets=(
  "./varint FuzzConsumeVarint"
  "./internal/wire FuzzDecodeFrame"
  "./internal/wire FuzzParsePreamble"
  "./internal/wire FuzzDecodeParams"
  ". FuzzParseCapsuleHeader"
  ". FuzzServerInput"
  ". FuzzListenerInput"
  "./seal/proto FuzzParse"
  "./shaping/bundle FuzzParse"
)
for t in "${targets[@]}"; do
  read -r pkg name <<<"$t"
  echo "== $pkg $name"
  (cd "$pkg" && timeout 300 go test -run '^$' -fuzz "^${name}\$" -fuzztime "$time" .)
done
