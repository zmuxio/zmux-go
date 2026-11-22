package zmux

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
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
		{Liveness: Liveness{ProbeInterval: time.Nanosecond}},
		{Liveness: Liveness{ProbeInterval: 999 * time.Millisecond}},
		{Liveness: Liveness{StandbyProbe: 500 * time.Millisecond}},
		{Liveness: Liveness{ProbeJitter: 1}},
		{Liveness: Liveness{KeepAlivePeriod: time.Nanosecond}},
		{Liveness: Liveness{KeepAlivePeriod: 999 * time.Millisecond}},
		{MaxIdleTimeout: time.Nanosecond},
		{MaxIdleTimeout: 999 * time.Microsecond},
		{SchedulerParams: SchedulerParams{OverflowDelay: -time.Second}},
	}
	for i := range bad {
		if _, err := bad[i].coreConfig(core.RoleInitiator, core.RoleInitiator); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%d: %+v: %v", i, bad[i].ShapingLimits, err)
		}
	}
	good := []Config{
		{ShapingLimits: shaping.Limits{Wire: shaping.Account{Initial: shaping.MaxAccountInitial, PerMB: shaping.MaxAccountRate, PerSec: shaping.MaxAccountRate}, MaxDelay: time.Second}},
		{ShapingLimits: shaping.Limits{MaxDelay: -1}},
		{Liveness: Liveness{ProbeInterval: time.Second}},
		{Liveness: Liveness{KeepAlivePeriod: time.Second}, MaxIdleTimeout: time.Millisecond},
		{Liveness: Liveness{ProbeInterval: -1, StandbyProbe: -1, ProbeJitter: -1}},
		{Liveness: Liveness{ProbeJitter: -0.1}},
		{ShapingLimits: shaping.Limits{NoPadding: true, NoInjection: true, NoPause: true}},
		{MemoryBudget: -1, HandshakeTimeout: -1, Liveness: Liveness{KeepAlivePeriod: -1}, MaxIdleTimeout: -1, MaxIncomingStreams: -1, MaxIncomingUniStreams: -1},
		{MemoryBudget: -2},
	}
	for i := range good {
		if _, err := good[i].coreConfig(core.RoleInitiator, core.RoleInitiator); err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
	// A negative per-pipe ProbeInterval turns the pipe's idle probes off.
	o := PipeOptions{Liveness: Set(PipeLiveness{ProbeInterval: -time.Second})}
	if j, err := o.join(DirectionBidirectional); err != nil || j.Live.ProbeInterval >= 0 {
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
		got, want any
		name      string
	}{
		{name: "DefaultKeepAlivePeriod", got: DefaultKeepAlivePeriod, want: 15 * time.Second},
		{name: "keep-alive in effect", got: time.Duration(cc.KeepAlive), want: DefaultKeepAlivePeriod},
		{name: "DefaultMaxIdleTimeout", got: DefaultMaxIdleTimeout, want: 30 * time.Second},
		{name: "idle timeout in effect", got: time.Duration(cc.Local.MaxIdleTimeout) * time.Millisecond, want: DefaultMaxIdleTimeout},
		{name: "DefaultProbeJitter", got: DefaultProbeJitter, want: 0.25},
		{name: "probe jitter in effect", got: cc.ProbeJitter, want: DefaultProbeJitter},
		{name: "DefaultMaxRecordSize", got: DefaultMaxRecordSize, want: 16382},
		{name: "shaping.DefaultMaxDelay", got: shaping.DefaultMaxDelay, want: time.Second},
		{name: "shaping.MinMaxLen", got: shaping.MinMaxLen, want: 64},
		{name: "DefaultMinBackoff", got: DefaultMinBackoff, want: time.Second},
		{name: "DefaultMaxBackoff", got: DefaultMaxBackoff, want: 60 * time.Second},
		{name: "DefaultJoinTimeout", got: DefaultJoinTimeout, want: 10 * time.Second},
		{name: "join timeout in effect", got: time.Duration(cc.JoinTimeout), want: DefaultJoinTimeout},
	} {
		if c.got != c.want {
			t.Errorf("%s: %v (%T), want %v (%T)", c.name, c.got, c.got, c.want, c.want)
		}
	}
}

// TestDatagramStreamBytesSetting: zero is 512 KiB, a negative value
// unlimited, a positive value itself.
func TestDatagramStreamBytesSetting(t *testing.T) {
	for v, want := range map[int64]uint64{0: 512 << 10, -1: 0, 4096: 4096} {
		cc, err := (&Config{DatagramStreamBytes: v}).coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Fatalf("%d: %v", v, err)
		}
		if cc.DgramStreamSize != want {
			t.Fatalf("%d: %d", v, cc.DgramStreamSize)
		}
	}
}

// A negative MaxIdleTimeout turns the idle timeout off: the parameter is
// not sent and the local value is none, so only the peer's applies.
func TestMaxIdleTimeoutOff(t *testing.T) {
	cc, err := (&Config{MaxIdleTimeout: -1}).coreConfig(core.RoleInitiator, core.RoleInitiator)
	if err != nil {
		t.Fatal(err)
	}
	if cc.Local.Has&wire.HasIdle != 0 || cc.Local.MaxIdleTimeout != 0 {
		t.Fatalf("idle timeout off: Has %v, MaxIdleTimeout %d", cc.Local.Has&wire.HasIdle != 0, cc.Local.MaxIdleTimeout)
	}
}
