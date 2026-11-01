package core

import (
	"math"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// TestShapePauseTotalNoOverflow: a pause so long that it overflows the
// running total is still bounded by MaxPauseTotal.
func TestShapePauseTotalNoOverflow(t *testing.T) {
	s, _, _ := injectSession(t, shaping.Limits{MaxPauseTotal: time.Hour}, 0)
	s.shp.stats.PauseTotal = 30 * time.Minute
	if end := s.shapePause(Duration(math.MaxInt64 - int64(10*time.Minute))); end.Sub(s.now) > int64(30*time.Minute) {
		t.Fatalf("pause of %v past MaxPauseTotal", time.Duration(end.Sub(s.now)))
	}
	if s.shp.stats.PauseTotal != time.Hour {
		t.Fatalf("pause total %v", s.shp.stats.PauseTotal)
	}
}
