package zmux

import (
	"errors"
	"math"
	"testing"
)

// TestQualityBandRatioRule: RateRatio 0 is off, a finite factor of at
// least 1 is valid, anything else is ErrInvalidConfig.
func TestQualityBandRatioRule(t *testing.T) {
	for _, r := range []float64{0, 1, 1.5, math.MaxFloat64} {
		if _, err := (QualityBand{RateRatio: r}).core(); err != nil {
			t.Fatalf("RateRatio %v: %v", r, err)
		}
	}
	for _, r := range []float64{-1, 0.5, math.Nextafter(1, 0), math.Inf(1), math.Inf(-1), math.NaN()} {
		if _, err := (QualityBand{RateRatio: r}).core(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("RateRatio %v: %v", r, err)
		}
	}
}
