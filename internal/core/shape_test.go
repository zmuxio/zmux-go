package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
)

func TestShapeWindow(t *testing.T) {
	s := New(DefaultConfig(), 0)

	for i := range 16 {
		if !s.noEffect.available(Time(i)) {
			t.Fatalf("record %d refused", i)
		}
		s.SetNow(Time(i))
		s.recordNoEffect()
	}
	if s.noEffect.available(Time(Second - 1)) {
		t.Fatal("17th record within one second")
	}
	if !s.noEffect.available(Time(Second)) {
		t.Fatal("record after one second refused")
	}
}

// SetShapeBudget replaces the configuration's budget; the account carries over.
func (s *Session) SetShapeBudget(b shaping.Budget) bool {
	return s.setShapeLimits(s.cfg.ShapeLimits, b, false)
}

// SetPipeShaper installs a test machine directly on an outbound pipe.
func (s *Session) SetPipeShaper(pipe uint64, x shaping.Shaper) bool {
	p := s.pipe(pipe)
	if p == nil || p.out == nil {
		return false
	}
	s.shapeSess()
	s.shapeInstall(p, x)
	s.wakePipe(p)
	s.wake(p.chSig)
	return true
}

// Raw machine fixtures can exercise the protocol limits without a configured host policy.
func (s *Session) SetShaper(f Factory) { s.installFactory(f) }
