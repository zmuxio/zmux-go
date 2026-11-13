// Package carrierio holds the carrier helpers that the root package uses
// itself and package carrier exposes: the flat batch write and the
// demultiplexer of channel datagrams.
package carrierio

import (
	"io"
	"sync"
)

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
	*bp = b[:0]
	flatPool.Put(bp)
	return n, err
}
