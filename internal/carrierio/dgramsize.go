package carrierio

import (
	"sync/atomic"
	"time"
)

// DatagramSizeRefresh bounds how stale a channel's maximum datagram may
// be: the path MTU, and with it the maximum, may grow. A channel asks its
// carrier at most this often (the QUIC, HTTP/3 and WebTransport channels
// of zmuxquic and zmuxh3 alike); a probe sends nothing (an oversized
// send the carrier refuses locally, or a read of the connection's state),
// so a short refresh costs little and the maximum follows path MTU
// discovery closely.
const DatagramSizeRefresh = 250 * time.Millisecond

var sizeEpoch = time.Now()

// DatagramSize is a datagram channel's current maximum payload, learned
// from the carrier's refusals. It is safe for concurrent use; the zero
// value knows none.
type DatagramSize struct {
	max      atomic.Int64
	probedAt atomic.Int64 // monotonic nanoseconds of the last probe, 0: none yet
}

// Get returns the maximum, asking probe at most once per
// DatagramSizeRefresh: probe reports the maximum now, ok false when the
// channel takes none (not negotiated, or the probe failed otherwise).
func (d *DatagramSize) Get(probe func() (n int64, ok bool)) int {
	now := int64(time.Since(sizeEpoch)) + 1 // never 0
	if last := d.probedAt.Load(); last != 0 && now-last < int64(DatagramSizeRefresh) {
		return int(d.max.Load())
	}
	n, ok := probe()
	if !ok {
		n = 0
	}
	d.max.Store(max(n, 0))
	// After the result: a concurrent caller that sees a recent probe reads it.
	d.probedAt.Store(now)
	return int(d.max.Load())
}

// Set records a maximum a refused send reported.
func (d *DatagramSize) Set(n int64) { d.max.Store(max(n, 0)) }
