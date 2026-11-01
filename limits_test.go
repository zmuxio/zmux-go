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
		{ShapingLimits: shaping.Limits{MaxPaddingRatio: -0.1}},
		{ShapingLimits: shaping.Limits{MaxPaddingRatio: 1.5}},
		{ShapingLimits: shaping.Limits{MaxPaddingRatio: math.NaN()}},
		{ShapingLimits: shaping.Limits{MaxPaddingRatio: math.Inf(1)}},
		{ShapingLimits: shaping.Limits{MaxPauseTotal: -time.Second}},
		{ShapingLimits: shaping.Limits{MaxDelay: -1}},
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
		{ShapingLimits: shaping.Limits{MaxPaddingRatio: 1, MaxPaddingBytes: 1 << 20, MaxDelay: time.Second}},
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
	if j, err := o.join(DirBidirectional); err != nil || j.ProbeInterval >= 0 {
		t.Errorf("PipeOptions ProbeInterval negative: %+v %v", j, err)
	}
}

// The exported defaults are the values in effect when nothing is set.
func TestExportedDefaults(t *testing.T) {
	cc, err := (&Config{}).coreConfig(core.RoleInitiator, core.RoleInitiator)
	if err != nil {
		t.Fatal(err)
	}
	if DefaultKeepAlivePeriod != 15*time.Second || time.Duration(cc.KeepAlive) != DefaultKeepAlivePeriod {
		t.Errorf("keep-alive %v / %v", DefaultKeepAlivePeriod, cc.KeepAlive)
	}
	if DefaultMaxIdleTimeout != 30*time.Second || time.Duration(cc.Local.MaxIdleTimeout)*time.Millisecond != DefaultMaxIdleTimeout {
		t.Errorf("idle %v / %v ms", DefaultMaxIdleTimeout, cc.Local.MaxIdleTimeout)
	}
	if DefaultProbeJitter != 0.25 || cc.ProbeJitter != DefaultProbeJitter {
		t.Errorf("probe jitter %v / %v", DefaultProbeJitter, cc.ProbeJitter)
	}
	if DefaultMaxRecordSize != 16382 || shaping.StartupAllowance != DefaultMaxRecordSize || shaping.MinMaxLen != 64 {
		t.Errorf("record %v, allowance %v, min max_len %v", DefaultMaxRecordSize, shaping.StartupAllowance, shaping.MinMaxLen)
	}
	if MaxRedialBackoff != 30*time.Second || DefaultMaxBackoff != 60*time.Second {
		t.Errorf("backoff %v %v", MaxRedialBackoff, DefaultMaxBackoff)
	}
}
