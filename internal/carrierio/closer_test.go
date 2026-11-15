package carrierio

import "testing"

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
