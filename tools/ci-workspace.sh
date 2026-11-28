#!/usr/bin/env bash
# Create a workspace outside the checkout for CI's local module checks.
set -euo pipefail

cd "$RUNNER_TEMP"
export GOWORK="$RUNNER_TEMP/go.work"
root=$GITHUB_WORKSPACE
go work init
go work use "$root" "$root/zmuxquic" "$root/zmuxh3" "$root/zmuxws" "$root/seal" \
  "$root/examples/xorpipe" "$root/interop/webpeer"
echo "GOWORK=$GOWORK" >> "$GITHUB_ENV"
