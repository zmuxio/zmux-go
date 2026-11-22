package zmux

import (
	"errors"
	"math"
	"net"
	"testing"
	"time"
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

// TestQualityDefaults: the exported defaults hold the values of the
// specification, the zero MinInterval and RecoveryRatio resolve to them,
// and the zero QualityBand clears the session's band.
func TestQualityDefaults(t *testing.T) {
	for _, d := range []struct {
		got, want any
		name      string
	}{
		{name: "DefaultMaxLanes", got: DefaultMaxLanes, want: 258},
		{name: "DefaultRecoveryRatio", got: DefaultRecoveryRatio, want: 1.25},
		{name: "DefaultMinInterval", got: DefaultMinInterval, want: time.Second},
	} {
		if d.got != d.want {
			t.Errorf("%s = %v, want %v", d.name, d.got, d.want)
		}
	}
	if b, err := (QualityBand{RTT: time.Millisecond}).core(); err != nil || b.MinInterval != 0 {
		t.Fatalf("band %+v, %v", b, err) // 0 is the core's default
	}
	a, b := net.Pipe()
	c, _ := pair(t, a, b, nil, nil)
	if err := c.SetQualityBand(QualityBand{RTT: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	c.lock()
	set := c.s.DefaultQualityBand()
	c.unlock()
	if set == nil || set.MinInterval != 0 {
		t.Fatalf("band %+v", set) // kept as set; the core applies the default where it reads it
	}
	if got := c.QualityBand(); got != (QualityBand{RTT: time.Millisecond}) {
		t.Fatalf("QualityBand() = %+v", got)
	}
	if err := c.SetCapacityFloor(CapacityFloor{Rate: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if got := c.CapacityFloor(); got != (CapacityFloor{Rate: 1 << 20}) {
		t.Fatalf("CapacityFloor() = %+v", got)
	}
	if err := c.SetQualityBand(QualityBand{}); err != nil {
		t.Fatal(err)
	}
	c.lock()
	set = c.s.DefaultQualityBand()
	c.unlock()
	if set != nil {
		t.Fatalf("zero band kept %+v", set)
	}
}

// The stream getters read the settings back; a peer's change of the same
// setting is not among them, so the priority is what SetPriority set.
func TestStreamSettingGetters(t *testing.T) {
	cs, _ := streamPairT(t)
	if u, inc := cs.Priority(); u != UrgencyDefault || !inc {
		t.Fatalf("new stream priority %d %v", u, inc)
	}
	cs.SetPriority(UrgencyHighest, false)
	if u, inc := cs.Priority(); u != UrgencyHighest || inc {
		t.Fatalf("priority %d %v", u, inc)
	}
	if _, set := cs.Redundant(); set {
		t.Fatal("redundancy set on a new stream")
	}
	cs.SetRedundant(false)
	if on, set := cs.Redundant(); on || !set {
		t.Fatalf("redundant %v %v", on, set)
	}
	cs.ClearRedundant()
	if _, set := cs.Redundant(); set {
		t.Fatal("redundancy set after ClearRedundant")
	}
	_ = cs.Close()
	if u, _ := cs.Priority(); u != UrgencyHighest {
		t.Fatal("getter on a closed stream")
	}
}
