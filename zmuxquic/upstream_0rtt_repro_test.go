//go:build quicgo_upstream_repro

package zmuxquic

import "testing"

// Explicit dependency reproducer. It intentionally bypasses the adapter's
// temporary handshake barrier; run under -race before removing that barrier.
func TestQuicGoUpstreamEarlyStreamRejection(t *testing.T) {
	exerciseRejectedEarlyConnection(t, dialNative)
}
