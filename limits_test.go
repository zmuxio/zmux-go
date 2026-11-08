package zmux

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/shaping"
)

// Shaping limits outside their ranges, a session probe interval or
// keep-alive period above 0 and below 1 s, and an idle timeout above 0 and
// below 1 ms are configuration errors, not corrected.
func TestConfigLimitsValidation(t *testing.T) {
	bad := []Config{
		{ShapingLimits: shaping.Limits{Wire: shaping.Account{Initial: shaping.MaxAccountInitial + 1}}},
		{ShapingLimits: shaping.Limits{Wire: shaping.Account{PerMB: shaping.MaxAccountRate + 1}}},
		{ShapingLimits: shaping.Limits{Wire: shaping.Account{PerSec: math.MaxUint64}}},
		{SchedulerParams: SchedulerParams{ProbeInterval: time.Nanosecond}},
		{SchedulerParams: SchedulerParams{ProbeInterval: 999 * time.Millisecond}},
		{SchedulerParams: SchedulerParams{StandbyProbe: 500 * time.Millisecond}},
		{SchedulerParams: SchedulerParams{ProbeJitter: -0.1}},
		{KeepAlivePeriod: time.Nanosecond},
		{KeepAlivePeriod: 999 * time.Millisecond},
		{MaxIdleTimeout: time.Nanosecond},
		{MaxIdleTimeout: 999 * time.Microsecond},
		{SchedulerParams: SchedulerParams{OverflowDelay: -time.Second}},
		{MemoryBudget: -2},
		{MaxIncomingStreams: -1},
		{MaxIncomingUniStreams: -1},
	}
	for i := range bad {
		if _, err := bad[i].coreConfig(core.RoleInitiator, core.RoleInitiator); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%d: %+v: %v", i, bad[i].ShapingLimits, err)
		}
	}
	good := []Config{
		{ShapingLimits: shaping.Limits{Wire: shaping.Account{Initial: shaping.MaxAccountInitial, PerMB: shaping.MaxAccountRate, PerSec: shaping.MaxAccountRate}, MaxDelay: time.Second}},
		{ShapingLimits: shaping.Limits{MaxDelay: -1}},
		{SchedulerParams: SchedulerParams{ProbeInterval: time.Second}},
		{KeepAlivePeriod: time.Second, MaxIdleTimeout: time.Millisecond},
		{SchedulerParams: SchedulerParams{ProbeInterval: -1, StandbyProbe: -1, NoProbeJitter: true}},
		{ShapingLimits: shaping.Limits{NoPadding: true, NoInjection: true, NoPause: true}},
		{MemoryBudget: NoMemoryLimit, HandshakeTimeout: -1, KeepAlivePeriod: -1, MaxIdleTimeout: -1, NoIncomingStreams: true, NoIncomingUniStreams: true},
	}
	for i := range good {
		if _, err := good[i].coreConfig(core.RoleInitiator, core.RoleInitiator); err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
	// A negative per-pipe ProbeInterval turns the pipe's idle probes off.
	o := PipeOptions{ProbeInterval: -time.Second}
	if j, err := o.join(DirBidirectional); err != nil || j.Live.ProbeInterval >= 0 {
		t.Errorf("PipeOptions ProbeInterval negative: %+v %v", j, err)
	}
}

// The exported defaults are the values in effect when nothing is set.
func TestExportedDefaults(t *testing.T) {
	cc, err := (&Config{}).coreConfig(core.RoleInitiator, core.RoleInitiator)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"DefaultKeepAlivePeriod", DefaultKeepAlivePeriod, 15 * time.Second},
		{"keep-alive in effect", time.Duration(cc.KeepAlive), DefaultKeepAlivePeriod},
		{"DefaultMaxIdleTimeout", DefaultMaxIdleTimeout, 30 * time.Second},
		{"idle timeout in effect", time.Duration(cc.Local.MaxIdleTimeout) * time.Millisecond, DefaultMaxIdleTimeout},
		{"DefaultProbeJitter", DefaultProbeJitter, 0.25},
		{"probe jitter in effect", cc.ProbeJitter, DefaultProbeJitter},
		{"DefaultMaxRecordSize", DefaultMaxRecordSize, 16382},
		{"shaping.DefaultMaxDelay", shaping.DefaultMaxDelay, time.Second},
		{"shaping.MinMaxLen", shaping.MinMaxLen, 64},
		{"DefaultMinBackoff", DefaultMinBackoff, time.Second},
		{"DefaultMaxBackoff", DefaultMaxBackoff, 60 * time.Second},
		{"DefaultJoinTimeout", DefaultJoinTimeout, 10 * time.Second},
		{"join timeout in effect", time.Duration(cc.JoinTimeout), DefaultJoinTimeout},
	} {
		if c.got != c.want {
			t.Errorf("%s: %v (%T), want %v (%T)", c.name, c.got, c.got, c.want, c.want)
		}
	}
}
