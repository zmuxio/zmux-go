#!/usr/bin/env sh
# Builds the QUIC datagram-channel peer into OUT (default ./out):
#   dgpeer server CERT KEY | client ADDR N     (ZMUX_GO_QUIC_CHANNEL_PEER)
# The module replaces zmux-go and zmuxquic with the checkout it lives in; the
# repository's go.work is not used.
set -eu
cd "$(dirname "$0")"
out=${1:-${OUT:-$PWD/out}}
mkdir -p "$out"
GOWORK=off timeout 300 go build -o "$out/dgpeer" .
