package zmux

import (
	"testing"

	"time"
)

// The built-in backoff waits lie in [0.8 b, b] and spread over it.
func TestBackoffWaitJitter(t *testing.T) {
	const b = 10 * time.Second
	var low, high bool
	for range 1000 {
		w := Backoff{}.filled().Wait(b)
		if w < 8*time.Second || w > b {
			t.Fatalf("wait %v outside [8 s, 10 s]", w)
		}
		low = low || w < 8200*time.Millisecond
		high = high || w > 9800*time.Millisecond
	}
	if !low || !high {
		t.Fatalf("draws near the ends: %v %v", low, high)
	}
}
