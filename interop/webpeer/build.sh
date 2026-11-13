#!/usr/bin/env sh
# Builds the scripted WebSocket and HTTP streaming peers into OUT (default ./out):
#   wspeer   server|client|bad ADDR            (ZMUX_GO_WS_PEER)
#   httppeer server|client|bad KIND ADDR       (ZMUX_GO_HTTP_PEER)
# The module replaces zmux-go and zmuxws with the checkout it lives in; the
# repository's go.work is not used.
set -eu
cd "$(dirname "$0")"
out=${1:-${OUT:-$PWD/out}}
mkdir -p "$out"
GOWORK=off timeout 300 go build -o "$out/" ./cmd/...
