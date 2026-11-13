package httpx

import "io"

// ReplayBody is a request body with bytes read earlier put back in front
// of rest; Close closes the original body.
func ReplayBody(read []byte, rest io.Reader, body io.Closer) io.ReadCloser {
	return &replayBody{Reader: io.MultiReader(&sliceReader{b: read}, rest), c: body}
}

type replayBody struct {
	io.Reader
	c io.Closer
}

func (b *replayBody) Close() error { return b.c.Close() }

type sliceReader struct{ b []byte }

func (s *sliceReader) Read(p []byte) (int, error) {
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.b)
	s.b = s.b[n:]
	return n, nil
}

// ErrReader returns err from every Read.
func ErrReader(err error) io.Reader { return errReader{err} }

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }
