package machine

import (
	"math"
	"math/rand/v2"
	"testing"
)

// moments returns the sample mean and variance of n draws of f.
func moments(n int, f func() float64) (mean, v float64) {
	xs := make([]float64, n)
	for i := range xs {
		xs[i] = f()
		mean += xs[i]
	}
	mean /= float64(n)
	for _, x := range xs {
		v += (x - mean) * (x - mean)
	}
	return mean, v / float64(n-1)
}

func seededDrawer() *drawer { return &drawer{r: rand.New(rand.NewChaCha8([32]byte{7}))} }

func TestBinomialMoments(t *testing.T) {
	s := seededDrawer()
	for _, c := range []struct{ n, p float64 }{{10, 0.3}, {1000, 0.3}, {1e9, 0.01}, {1e15, 0.5}} {
		mean, v := moments(4000, func() float64 { return s.binomial(c.n, c.p) })
		m, want := c.n*c.p, c.n*c.p*(1-c.p)
		if math.Abs(mean-m) > 5*math.Sqrt(want/4000)+1e-9 || math.Abs(v/want-1) > 0.15 {
			t.Fatalf("binomial(%g, %g): mean %g, variance %g", c.n, c.p, mean, v)
		}
	}
	for range 100 {
		if x := s.binomial(1000, 0.3); x != math.Trunc(x) || x < 0 || x > 1000 {
			t.Fatalf("binomial(1000, 0.3) = %g", x)
		}
	}
	if x := s.binomial(maxCount, 1); x != maxCount {
		t.Fatalf("binomial(2^53, 1) = %g", x)
	}
	if x := s.binomial(maxCount, 0); x != 0 {
		t.Fatalf("binomial(2^53, 0) = %g", x)
	}
}

func TestPoissonMoments(t *testing.T) {
	s := seededDrawer()
	for _, mean := range []float64{0.5, 29, 1000, 1e12, maxCount} {
		m, v := moments(4000, func() float64 { return s.poisson(mean) })
		if math.Abs(m-mean) > 5*math.Sqrt(mean/4000)+1e-9 || math.Abs(v/mean-1) > 0.15 {
			t.Fatalf("poisson(%g): mean %g, variance %g", mean, m, v)
		}
	}
}

func TestCountBounds(t *testing.T) {
	for _, d := range []Dist{
		{Kind: DistBinomial, Params: []float64{maxCount * 2, 0.5}},
		{Kind: DistPoisson, Params: []float64{maxCount * 2}},
	} {
		v := Value{Dist: &d}
		if v.validate() == nil {
			t.Fatalf("%s %v accepted", d.Kind, d.Params)
		}
	}
	for _, d := range []Dist{
		{Kind: DistBinomial, Params: []float64{maxCount, 0.5}},
		{Kind: DistPoisson, Params: []float64{maxCount}},
	} {
		v := Value{Dist: &d}
		if err := v.validate(); err != nil {
			t.Fatalf("%s %v: %v", d.Kind, d.Params, err)
		}
	}
}
