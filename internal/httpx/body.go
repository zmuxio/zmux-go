package httpx

import (
	"context"
	"io"
	"net/http"
)

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

func (e errReader) Read(_ []byte) (int, error) { return 0, e.err }

// DelegateRequest keeps the transport's final trailers visible through the
// request copy, including keys first declared by the terminal chunk.
func DelegateRequest(ctx context.Context, original *http.Request) *http.Request {
	r := original.WithContext(ctx)
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &trailerBody{r.Body, original, r}
	}
	return r
}

type trailerBody struct {
	io.ReadCloser
	original, delegate *http.Request
}

func (b *trailerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.delegate.Trailer = b.original.Trailer
	}
	return n, err
}
