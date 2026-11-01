package core

import (
	"math"
	"testing"
)

func TestMulDivCeil(t *testing.T) {
	for _, x := range []struct{ a, b, d, want uint64 }{
		{3, 10, 4, 8},
		{1 << 40, 1 << 40, 1 << 41, 1 << 39},                // the product overflows 64 bits
		{math.MaxUint64, math.MaxUint64, 1, math.MaxUint64}, // the quotient does not fit
		{0, 5, 7, 0},
	} {
		if got := mulDivCeil(x.a, x.b, x.d); got != x.want {
			t.Fatalf("mulDivCeil(%d, %d, %d) = %d, want %d", x.a, x.b, x.d, got, x.want)
		}
	}
}
