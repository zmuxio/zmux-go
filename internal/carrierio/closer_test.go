package carrierio

import (
	"testing"
	"testing/synctest"
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
	synctest.Test(t, func(t *testing.T) {
		in, release := make(chan struct{}), make(chan struct{})
		c := NewCloser(blockCloser{in, release}, nil)
		first := make(chan struct{})
		go func() { c.Close(); close(first) }()
		<-in // the first Close is inside the half's Close
		returned := make(chan struct{})
		go func() { c.Close(); close(returned) }()
		synctest.Wait()
		select {
		case <-returned:
			close(release)
			<-first
			t.Fatal("Close returned while the halves were still closing")
		default:
		}
		close(release)
		<-first
		<-returned
	})
}

type blockCloser struct{ in, release chan struct{} }

func (b blockCloser) Close() error { close(b.in); <-b.release; return nil }
