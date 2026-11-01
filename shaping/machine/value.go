package machine

import (
	crand "crypto/rand"
	"math"
	"math/rand/v2"
)

// keeper holds sampled values per scope.
type keeper interface {
	kept(v *Value) (float64, bool)
	keep(v *Value, x float64)
}

// drawer samples Values from a random source; it holds the state of bags.
type drawer struct {
	r *rand.Rand
	keeper
	bags map[*Value][]float64
}

// newRand returns a generator on src, or on a ChaCha8 source seeded from
// crypto/rand when src is nil.
func newRand(src rand.Source) *rand.Rand {
	if src == nil {
		var seed [32]byte
		_, _ = crand.Read(seed[:])
		src = rand.NewChaCha8(seed)
	}
	return rand.New(src)
}

// sample returns v, kept per its scope.
func (s *drawer) sample(v *Value) float64 {
	if x, ok := s.kept(v); ok {
		return x
	}
	x := s.draw(v)
	s.keep(v, x)
	return x
}

// draw samples v and applies its bounds.
func (s *drawer) draw(v *Value) float64 {
	var x float64
	switch {
	case v.Const != nil:
		x = *v.Const
	case v.Range != nil:
		x = v.Range[0] + (v.Range[1]-v.Range[0])*s.r.Float64()
	default:
		x = s.dist(v)
	}
	if v.Min != nil {
		x = math.Max(x, *v.Min)
	}
	if v.Max != nil {
		x = math.Min(x, *v.Max)
	}
	return x
}

func (s *drawer) dist(v *Value) float64 {
	d, r := v.Dist, s.r
	p := d.Params
	u := func() float64 { return 1 - r.Float64() } // (0, 1]
	switch d.Kind {
	case DistUniform:
		return p[0] + (p[1]-p[0])*r.Float64()
	case DistLogUniform:
		return math.Exp(math.Log(p[0]) + (math.Log(p[1])-math.Log(p[0]))*r.Float64())
	case DistExponential:
		return r.ExpFloat64() / p[0]
	case DistNormal:
		return p[0] + p[1]*r.NormFloat64()
	case DistLogNormal:
		return math.Exp(p[0] + p[1]*r.NormFloat64())
	case DistWeibull:
		return p[0] * math.Pow(-math.Log(u()), 1/p[1])
	case DistPareto:
		return p[0] / math.Pow(u(), 1/p[1])
	case DistGamma:
		return s.gamma(p[0]) * p[1]
	case DistBeta:
		x, y := s.gamma(p[0]), s.gamma(p[1])
		return x / (x + y)
	case DistGeometric:
		if p[0] == 1 {
			return 1
		}
		return math.Ceil(math.Log(u()) / math.Log(1-p[0]))
	case DistBinomial:
		return s.binomial(p[0], p[1])
	case DistPoisson:
		return s.poisson(p[0])
	case DistEmpirical:
		return d.Points[s.pick(d.Weights)]
	case DistBag:
		b := s.bags[v]
		if len(b) == 0 {
			if s.bags == nil {
				s.bags = map[*Value][]float64{}
			}
			b = append(b[:0], d.Points...)
		}
		i := r.IntN(len(b))
		x := b[i]
		b[i] = b[len(b)-1]
		s.bags[v] = b[:len(b)-1]
		return x
	}
	return s.sample(&d.Of[s.pick(d.Weights)]) // mixture
}

// maxCount is the largest Binomial trial count and Poisson mean accepted:
// 2^53, beyond which counts are not exact in a float64.
const maxCount = 1 << 53

// directTrials is the most trials a binomial sample takes one by one;
// directMean the largest mean a Poisson sample takes by the product of
// uniforms.
const (
	directTrials = 64
	directMean   = 30
)

// binomial samples Binomial(n, p) for an integral n in [0, 2^53] (Knuth,
// TAOCP 3.4.1 F): while n is large, the median order statistic of n
// uniforms, a Beta variate, halves the trials; O(log n) steps, then at
// most directTrials Bernoulli trials.
func (s *drawer) binomial(n, p float64) float64 {
	k := 0.0
	for n > directTrials {
		// Exact at the ends, where a rounded Beta variate could skip trials.
		if p <= 0 {
			return k
		}
		if p >= 1 {
			return k + n
		}
		a := 1 + math.Floor(n/2)
		b := n + 1 - a
		x := s.gamma(a)
		x /= x + s.gamma(b)
		if x >= p {
			n, p = a-1, p/x
		} else {
			k += a
			n, p = b-1, (p-x)/(1-x)
		}
	}
	for i := 0.0; i < n; i++ {
		if s.r.Float64() < p {
			k++
		}
	}
	return k
}

// poisson samples Poisson(mean) for a mean in (0, 2^53] (Knuth, TAOCP
// 3.4.1 F): while the mean is large, a Gamma variate takes 7/8 of it at
// once, or the rest is binomial; O(log mean) steps, then the product of
// uniforms for a mean of at most directMean.
func (s *drawer) poisson(mean float64) float64 {
	k := 0.0
	for mean > directMean {
		m := math.Floor(mean * 7 / 8)
		x := s.gamma(m)
		if x >= mean {
			return k + s.binomial(m-1, mean/x)
		}
		k += m
		mean -= x
	}
	l, t := math.Exp(-mean), s.r.Float64()
	for t > l {
		k++
		t *= s.r.Float64()
	}
	return k
}

func (s *drawer) pick(w []float64) int {
	t := 0.0
	for _, x := range w {
		t += x
	}
	t *= s.r.Float64()
	for i, x := range w {
		if t < x {
			return i
		}
		t -= x
	}
	return len(w) - 1
}

// gamma samples Gamma(a, 1) (Marsaglia and Tsang).
func (s *drawer) gamma(a float64) float64 {
	if a < 1 {
		return s.gamma(a+1) * math.Pow(1-s.r.Float64(), 1/a)
	}
	d := a - 1.0/3
	c := 1 / math.Sqrt(9*d)
	for {
		x := s.r.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := 1 - s.r.Float64()
		if math.Log(u) < 0.5*x*x+d-d*v+d*math.Log(v) {
			return d * v
		}
	}
}
