//go:build quicgo_upstream_repro

package zmuxquic

import "testing"

// This dependency reproducer intentionally bypasses the handshake barrier.
// Run it under -race.
func TestQuicGoUpstreamEarlyStreamRejection(t *testing.T) {
	exerciseRejectedEarlyConnection(t, dialNative)
}
