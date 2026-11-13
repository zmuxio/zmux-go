package carrier

import (
	"io"

	"github.com/zmuxio/zmux-go/internal/carrierio"
)

// WriteFlat writes bufs to w with one Write of their concatenation, copied
// into a pooled buffer: one call per batch for a writer whose every call
// costs a record or a locked hand-over (TLS, a QUIC stream). A
// zmux.BuffersWriter may implement WriteBuffers with it.
func WriteFlat(w io.Writer, bufs [][]byte) (int, error) { return carrierio.WriteFlat(w, bufs) }
