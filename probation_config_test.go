package zmux

import (
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// TestProbationSettings: Probation zero is the default and a negative
// Probation turns probation off, for the session's setting and a pipe's.
func TestProbationSettings(t *testing.T) {
	for _, x := range []struct {
		p    SchedulerParams
		want core.Duration // -1: the default
		bad  bool
	}{
		{SchedulerParams{}, -1, false},
		{SchedulerParams{Probation: -1}, 0, false},
		{SchedulerParams{Probation: 2 * time.Second}, core.Duration(2 * time.Second), false},
		{SchedulerParams{Probation: -time.Hour}, 0, false},
	} {
		cc, err := (&Config{SchedulerParams: x.p}).coreConfig(core.RoleInitiator, core.RoleInitiator)
		if x.bad {
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("%+v: %v", x.p, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%+v: %v", x.p, err)
		}
		if cc.Probation != x.want {
			t.Fatalf("%+v: probation %v", x.p, cc.Probation)
		}
	}
	for _, x := range []struct {
		want *core.Duration
		o    PipeOptions
		bad  bool
	}{
		{o: PipeOptions{}, want: nil, bad: false},
		{o: PipeOptions{Probation: Set(time.Duration(-1))}, want: new(core.Duration), bad: false},
		{o: PipeOptions{Probation: Set(time.Second)}, want: func() *core.Duration { d := core.Duration(time.Second); return &d }(), bad: false},
		{o: PipeOptions{Probation: Set(-time.Hour)}, want: new(core.Duration), bad: false},
	} {
		j, err := x.o.join(DirectionBidirectional)
		if x.bad {
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("%+v: %v", x.o, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%+v: %v", x.o, err)
		}
		if (j.Probation == nil) != (x.want == nil) || j.Probation != nil && *j.Probation != *x.want {
			t.Fatalf("%+v: probation %v", x.o, j.Probation)
		}
	}
}
