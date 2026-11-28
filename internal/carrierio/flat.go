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

var flatPool = sync.Pool{New: func() any { return new(FlatBuffer) }}

// FlatBuffer holds reusable capacity for concatenating a batch.
// The caller must serialize use through the end of the write. As with
// io.Writer, the downstream writer must stop using the bytes when it returns.
// The zero value is ready to use.
type FlatBuffer []byte

// Reserve returns empty scratch with room for size bytes. Oversized
// allocations are usable for this write and dropped from the cache.
func (f *FlatBuffer) Reserve(size int) []byte {
	if cap(*f) >= size {
		return (*f)[:0]
	}
	b := make([]byte, 0, size)
	f.keep(b)
	return b
}

func (f *FlatBuffer) keep(b []byte) {
	if cap(b) <= flatKeep {
		*f = b[:0]
	}
}

// WriteFlat writes bufs to w with one Write of their concatenation, copied
// into a pooled buffer: one call per batch for a writer whose every call
// costs a record or a locked hand-over (TLS, a QUIC stream).
func WriteFlat(w io.Writer, bufs [][]byte) (int, error) {
	if len(bufs) == 1 {
		return w.Write(bufs[0])
	}
	f := flatPool.Get().(*FlatBuffer)
	b := (*f)[:0]
	for _, p := range bufs {
		b = append(b, p...)
	}
	n, err := w.Write(b)
	f.keep(b)
	flatPool.Put(f)
	return n, err
}
