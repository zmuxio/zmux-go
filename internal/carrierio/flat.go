// Package carrierio holds the carrier helpers that the root package uses
// itself and package carrier exposes: the flat batch write and the
// demultiplexer of channel datagrams.
package carrierio

import (
	"io"
	"sync"
)

// flatKeep is the largest buffer the pool keeps: sixteen records of the
// largest size.
const flatKeep = 16 << 16

var flatPool = sync.Pool{New: func() any { return new([]byte) }}

// WriteFlat writes bufs to w with one Write of their concatenation, copied
// into a pooled buffer: one call per batch for a writer whose every call
// costs a record or a locked hand-over (TLS, a QUIC stream).
func WriteFlat(w io.Writer, bufs [][]byte) (int, error) {
	if len(bufs) == 1 {
		return w.Write(bufs[0])
	}
	bp := flatPool.Get().(*[]byte)
	b := (*bp)[:0]
	for _, p := range bufs {
		b = append(b, p...)
	}
	n, err := w.Write(b) // w retains nothing of b once it returns
	if cap(b) <= flatKeep {
		*bp = b[:0]
		flatPool.Put(bp)
	} // a rare huge batch goes to the collector, not into the pool for good
	return n, err
}
