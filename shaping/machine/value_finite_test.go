package machine

import (
	"math"
	"testing"
)

// TestBetaSmallShapesFinite: a Beta variate of shapes so small that both
// Gamma variates underflow is still a number in [0, 1].
func TestBetaSmallShapesFinite(t *testing.T) {
	s := seededDrawer()
	d := Dist{Kind: DistBeta, Params: []float64{1e-3, 1e-3}}
	v := Value{Dist: &d}
	for range 10000 {
		if x := s.dist(&v); math.IsNaN(x) || x < 0 || x > 1 {
			t.Fatalf("Beta(1e-3, 1e-3) drew %v", x)
		}
	}
}

// TestBoundedConversions: NaN converts as 0 and infinities as the bounds.
func TestBoundedConversions(t *testing.T) {
	for _, x := range []struct{ in, want float64 }{
		{math.NaN(), 0}, {math.Inf(1), 10}, {math.Inf(-1), -10}, {3, 3},
	} {
		if got := bounded(x.in, 10); got != x.want {
			t.Fatalf("bounded(%v) = %v", x.in, got)
		}
	}
	if d := seconds(math.NaN()); d != 0 {
		t.Fatalf("seconds(NaN) = %v", d)
	}
	if d := seconds(math.Inf(-1)); d >= 0 {
		t.Fatalf("seconds(-Inf) = %v", d)
	}
}
