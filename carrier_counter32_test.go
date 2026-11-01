package zmux

import "testing"

// counter32 carries a wrapping 32-bit count on into 64 bits.
func TestCounter32Widen(t *testing.T) {
	var c counter32
	for _, tc := range []struct {
		in   uint32
		want uint64
	}{{0, 0}, {10, 10}, {1<<32 - 1, 1<<32 - 1}, {5, 1<<32 + 5}, {5, 1<<32 + 5}, {3, 2<<32 + 3}} {
		if got := c.widen(tc.in); got != tc.want {
			t.Fatalf("widen(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
