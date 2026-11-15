package zmuxhttp

import (
	"errors"
	"io"
	"testing"
)

// TestExBodyHopReferences: every hop body holds its own reference to the
// buffer, which is released only once the attempt and every hop closed;
// a Read after Close finds nothing.
func TestExBodyHopReferences(t *testing.T) {
	buf := getExBuf(16)
	copy(buf.b, "payload")
	src := exBody{buf: buf, data: buf.b[:7]}
	h1, h2 := src.hop(), src.hop()
	if got := buf.refs.Load(); got != 3 {
		t.Fatalf("refs %d, want 3", got)
	}
	_ = src.Close()
	_ = h1.Close()
	_ = h1.Close() // a second Close of one body releases nothing more
	if got := buf.refs.Load(); got != 1 {
		t.Fatalf("refs %d after two closes, want 1", got)
	}
	if n, err := h1.Read(make([]byte, 8)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read after Close: %d %v", n, err)
	}
	b, err := io.ReadAll(h2)
	if err != nil || string(b) != "payload" {
		t.Fatalf("live hop read %q %v", b, err)
	}
	_ = h2.Close()
	if got := buf.refs.Load(); got != 0 {
		t.Fatalf("refs %d after the last close, want 0", got)
	}
}

// TestExBufLastReference: unref reports the last reference, which returns
// the piece's charge; a kept buffer is still released once.
func TestExBufLastReference(t *testing.T) {
	b := getExBuf(16)
	b.ref()
	b.keep()
	if b.unref() {
		t.Fatal("first of two references reported as the last")
	}
	if !b.unref() {
		t.Fatal("last reference not reported")
	}
	if fresh := getExBuf(16); fresh.kept.Load() {
		t.Fatal("a kept buffer came back from the pool")
	}
}
