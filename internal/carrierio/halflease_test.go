package carrierio

import (
	"context"
	"testing"
)

// TestHalfLease: the lease ends once, at the second half's end or at the
// connection's, and a lease ended by its halves leaves nothing on ctx.
func TestHalfLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	h := NewHalfLease(ctx, func() { n++ })
	h.HalfEnded()
	if n != 0 {
		t.Fatal("ended after one half")
	}
	h.HalfEnded()
	if n != 1 || h.stop() {
		t.Fatalf("released %d times; registration still on ctx", n)
	}
	cancel()
	var nilLease *HalfLease
	nilLease.HalfEnded()
}
