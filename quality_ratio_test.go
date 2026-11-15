package zmux

import (
	"errors"
	"math"
	"testing"
)

// TestRateRatioValidation: a RateRatio is 0 or a finite factor of at least
// 1, in a CapacityFloor as in a QualityBand.
func TestRateRatioValidation(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, nil)
	for _, r := range []float64{0, 1, 1.25, 1e300} {
		if err := c.SetCapacityFloor(CapacityFloor{Rate: 1, RecoveryRatio: r}); err != nil {
			t.Errorf("SetCapacityFloor ratio %v: %v", r, err)
		}
		if err := c.SetQualityBand(QualityBand{RateRatio: r}); err != nil {
			t.Errorf("SetQualityBand ratio %v: %v", r, err)
		}
	}
	for _, r := range []float64{-1, 0.5, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := c.SetCapacityFloor(CapacityFloor{Rate: 1, RecoveryRatio: r}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("SetCapacityFloor ratio %v: %v", r, err)
		}
		if err := c.SetQualityBand(QualityBand{RateRatio: r}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("SetQualityBand ratio %v: %v", r, err)
		}
	}
}
