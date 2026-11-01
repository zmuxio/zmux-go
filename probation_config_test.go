package zmux

import (
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// TestProbationSettings: Probation zero is the default, NoProbation turns
// probation off, and a negative Probation or one set with NoProbation is
// invalid, for the session's setting and a pipe's.
func TestProbationSettings(t *testing.T) {
	for _, x := range []struct {
		p    SchedulerParams
		want core.Duration // -1: the default
		bad  bool
	}{
		{SchedulerParams{}, -1, false},
		{SchedulerParams{NoProbation: true}, 0, false},
		{SchedulerParams{Probation: 2 * time.Second}, core.Duration(2 * time.Second), false},
		{SchedulerParams{Probation: -1}, 0, true},
		{SchedulerParams{Probation: time.Second, NoProbation: true}, 0, true},
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
		o    PipeOptions
		want *core.Duration
		bad  bool
	}{
		{PipeOptions{}, nil, false},
		{PipeOptions{NoProbation: true}, new(core.Duration), false},
		{PipeOptions{Probation: time.Second}, func() *core.Duration { d := core.Duration(time.Second); return &d }(), false},
		{PipeOptions{Probation: -1}, nil, true},
		{PipeOptions{Probation: time.Second, NoProbation: true}, nil, true},
	} {
		j, err := x.o.join(DirBidirectional)
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
