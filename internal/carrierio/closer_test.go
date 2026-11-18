package carrierio

import (
	"testing"
	"time"
)

type countCloser struct{ n int }

func (c *countCloser) Close() error { c.n++; return nil }

func TestCloserClosesEachHalfOnce(t *testing.T) {
	r, w := &countCloser{}, &countCloser{}
	c := NewCloser(r, w)
	c.Close()
	c.Close()
	if r.n != 1 || w.n != 1 {
		t.Fatalf("closes r=%d w=%d, want 1 each", r.n, w.n)
	}
	if c.Release(nil) {
		t.Fatal("released closed halves")
	}
}

func TestCloserOneValueClosedOnce(t *testing.T) {
	h := &countCloser{}
	NewCloser(h, h).Close()
	if h.n != 1 {
		t.Fatalf("closes %d, want 1", h.n)
	}
}

func TestCloserForwardsAfterRelease(t *testing.T) {
	h := &countCloser{}
	c := NewCloser(h, h)
	forwarded := 0
	if !c.Release(func() { forwarded++ }) {
		t.Fatal("release failed")
	}
	c.Close()
	if released := c.CloseUnreleased(); !released {
		t.Fatal("CloseUnreleased after a release reports none")
	}
	if h.n != 0 || forwarded != 1 {
		t.Fatalf("closes %d, forwarded %d; want 0 and 1", h.n, forwarded)
	}
}

// A Close that finds another one in progress returns only once the halves
// are closed.
func TestCloserWaitsForCloseInProgress(t *testing.T) {
	in, release := make(chan struct{}), make(chan struct{})
	c := NewCloser(blockCloser{in, release}, nil)
	go c.Close()
	<-in // the first Close is inside the half's Close
	returned := make(chan struct{})
	go func() { c.Close(); close(returned) }()
	select {
	case <-returned:
		t.Fatal("Close returned while the halves were still closing")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the halves closed")
	}
}

type blockCloser struct{ in, release chan struct{} }

func (b blockCloser) Close() error { close(b.in); <-b.release; return nil }
