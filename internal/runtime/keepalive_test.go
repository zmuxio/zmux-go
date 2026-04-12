package runtime

import (
	"testing"
	"time"
)

func TestInitKeepaliveJitterStateReturnsNonZero(t *testing.T) {
	t.Parallel()

	if got := InitKeepaliveJitterState(0); got == 0 {
		t.Fatal("InitKeepaliveJitterState(0) = 0, want non-zero state")
	}
	if got := InitKeepaliveJitterState(123); got != 123 {
		t.Fatalf("InitKeepaliveJitterState(123) = %d, want 123", got)
	}
}

func TestInitKeepaliveJitterStateAllocatesDistinctDefaultSeeds(t *testing.T) {
	t.Parallel()

	first := InitKeepaliveJitterState(0)
	second := InitKeepaliveJitterState(0)
	if first == 0 || second == 0 {
		t.Fatalf("default jitter seeds = (%d, %d), want both non-zero", first, second)
	}
	if first == second {
		t.Fatalf("default jitter seeds = (%d, %d), want distinct per-session states", first, second)
	}
}

func TestNextKeepaliveJitterWithinWindow(t *testing.T) {
	t.Parallel()

	base := 80 * time.Millisecond
	state := uint64(1)
	window := base / 8

	for i := 0; i < 16; i++ {
		got := NextKeepaliveJitter(base, &state)
		if got < 0 || got > window {
			t.Fatalf("NextKeepaliveJitter(%v) = %v, want within [0,%v]", base, got, window)
		}
	}
}

func TestNextKeepaliveJitterAdvancesState(t *testing.T) {
	t.Parallel()

	base := 64 * time.Second
	state := uint64(1)

	first := NextKeepaliveJitter(base, &state)
	second := NextKeepaliveJitter(base, &state)
	if first == second {
		t.Fatalf("consecutive jitter values = %v and %v, want deterministic state advance", first, second)
	}
}
