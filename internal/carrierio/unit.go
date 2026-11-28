package carrierio

import (
	"errors"
	"io"
)

// WriteUnit writes one complete handshake unit. The caller owns serialization.
func WriteUnit(w io.Writer, messages bool, b []byte) error {
	if messages {
		m, ok := w.(interface{ WriteMessage([]byte) error })
		if !ok {
			return errors.New("zmux: Records framing needs a MessageWriter")
		}
		return m.WriteMessage(b)
	}
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

// ReadStreamUnit appends one bounded chunk. The parser owns the unit
// framing, maximum size, and unconsumed bytes.
func ReadStreamUnit(r io.Reader, buf []byte, limit, batch int) ([]byte, error) {
	size := min(limit-len(buf), batch)
	if size <= 0 {
		return buf, errors.New("zmux: handshake unit exceeds its read bound")
	}
	if cap(buf)-len(buf) < size {
		// Never grow past the parser's absolute bound, even when the
		// runtime's usual growth factor would round up.
		grown := make([]byte, len(buf), min(limit, max(len(buf)+size, 2*cap(buf))))
		copy(grown, buf)
		buf = grown
	}
	start := len(buf)
	n, err := r.Read(buf[start : start+size : start+size])
	if n < 0 || n > size {
		return buf, errors.New("zmux: invalid carrier read count")
	}
	return buf[:start+n], err
}
