package zmuxhttp

import (
	"errors"
	"io"
	"testing"
)

// The reader releases the buffer of a piece with no data.
func TestExClientReaderReleasesEmptyPiece(t *testing.T) {
	buf := getExBuf(16)
	buf.ref() // the test's own, to observe the reader's release
	c := &exClient{wake: make(chan struct{}, 1), held: map[uint64]*exPiece{
		0: {buf: buf, data: buf.b[:0]},
		1: {fin: true},
	}, fin: 1, finKnown: true}
	c.rc.L, c.wc.L = &c.mu, &c.mu
	if n, err := (&exClientReader{c}).Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read %d, %v", n, err)
	}
	if !buf.unref() {
		t.Fatal("the empty piece's buffer was not released")
	}
}
