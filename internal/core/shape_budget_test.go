package core

import (
	"math"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// fixedPlan plans the same record every time and counts dropped injections.
type fixedPlan struct {
	plan     shaping.RecordPlan
	inject   int
	dropped  int
	injected int
	caps     shaping.Caps
	meters   shaping.Meters
}

func (f *fixedPlan) Capabilities() shaping.Caps { return shaping.CapPlan | f.caps }
func (f *fixedPlan) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	*p = f.plan
	f.meters = v.Meters
}
func (f *fixedPlan) OnEvent(e *shaping.Event, r *shaping.Effects) {
	switch e.Kind {
	case shaping.EventStart:
		if f.inject > 0 {
			r.Set, r.HasWake, r.Wake = shaping.EffectWake, true, e.Now+time.Microsecond
		}
	case shaping.EventTimer:
		if f.inject > 0 {
			r.Set = shaping.EffectInject
			r.Injection = shaping.Injection{ID: 1, At: e.Now, PadBytes: f.inject}
		}
	case shaping.EventInjectionAccepted:
		f.injected++
		r.Set = shaping.EffectInject
	case shaping.EventInjectionRejected:
		f.dropped++
		r.Set = shaping.EffectInject
	default:
	}
}

// mustShape is the shaper state of p, which a budgetSession pipe has.
func mustShape(t *testing.T, s *Session, p *Pipe) *pipeShape {
	t.Helper()
	ps := s.shapeOf(p)
	if ps == nil {
		t.Fatal("pipe has no shaper")
	}
	return ps
}

func budgetSession(t *testing.T, l shaping.Limits, b shaping.Budget, f *fixedPlan) (*Session, *Pipe, *Stream) {
	t.Helper()
	s, p := channelSession(t, nil, false)
	s.peerZmux = true
	s.cfg.ShapeLimits, s.cfg.ShapeBudget = l, b
	s.peer.MaxStreamDataBidiRem = 1 << 40
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 40
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return f })
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	return s, p, st
}

// Padding, splits and delays go in full however empty the account is; they
// meter WireSpent and leave the account alone.
func TestAccompanyingNeverRefused(t *testing.T) {
	f := &fixedPlan{plan: shaping.RecordPlan{MaxLen: 200}}
	f.plan.PadTo = 200
	s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{Wire: &shaping.Account{Ceiling: 1, Initial: 1}}, f)
	if _, err := s.Write(st, make([]byte, 50), false); err != nil {
		t.Fatal(err)
	}
	now := Time(Second)
	s.SetNow(now)
	sizes := poll(t, s, p, DefaultBatch, now)
	if len(sizes) == 0 || sizes[0] < 190 {
		t.Fatalf("sizes %v", sizes)
	}
	if got := s.ShapingStats(); got.WireSpent != 0 || s.ShapeMeters().ConsumedBytes == 0 || got.InjectionRejected != 0 {
		t.Fatalf("stats %+v", got)
	}
	if l := s.shp.acct.left(); l != 1 {
		t.Fatalf("account drawn on by padding: %d left", l)
	}
}

// An injection over the account is dropped at once, on both paths, and comes
// back to the machine; one within it goes and draws on it.
func TestInjectionCharged(t *testing.T) {
	now := Time(Second)
	// a record of PADDING only
	f := &fixedPlan{inject: 500}
	s, p, _ := budgetSession(t, shaping.Limits{}, shaping.Budget{Wire: &shaping.Account{Ceiling: 100, Initial: 100}}, f)
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if f.dropped != 1 || f.injected != 0 || s.shp.acct.left() != 100 || s.ShapingStats().WireSpent != 0 {
		t.Fatalf("dropped %d injected %d left %d", f.dropped, f.injected, s.shp.acct.left())
	}
	f = &fixedPlan{inject: 500}
	s, p, _ = budgetSession(t, shaping.Limits{}, shaping.Budget{Wire: &shaping.Account{Ceiling: 600, Initial: 600}}, f)
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if f.injected != 1 || s.shp.acct.left() >= 600 || s.ShapingStats().InjectionAccepted != 1 {
		t.Fatalf("injected %d left %d", f.injected, s.shp.acct.left())
	}
	// merged into the open record: refused before anything changes
	f = &fixedPlan{inject: 500}
	s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{Wire: &shaping.Account{Ceiling: 100, Initial: 100}}, f)
	if _, err := s.Write(st, make([]byte, 50), false); err != nil {
		t.Fatal(err)
	}
	s.SetNow(now)
	s.shapeTimeout(now)
	sizes := poll(t, s, p, DefaultBatch, now)
	if f.dropped != 1 || len(sizes) != 1 || sizes[0] > 200 {
		t.Fatalf("dropped %d sizes %v", f.dropped, sizes)
	}
	// merged and paid for: metered once
	f = &fixedPlan{inject: 500}
	s, p, st = budgetSession(t, shaping.Limits{}, shaping.Budget{Wire: &shaping.Account{Ceiling: 1000, Initial: 1000}}, f)
	if _, err := s.Write(st, make([]byte, 50), false); err != nil {
		t.Fatal(err)
	}
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if f.injected != 1 || s.shp.acct.left() != 499 || s.ShapingStats().WireSpent != 501 {
		t.Fatalf("injected %d left %d spent %d", f.injected, s.shp.acct.left(), s.ShapingStats().WireSpent)
	}
}

// A shaper that keeps a refused injection is not asked again at once.
type keepInject struct{ events int }

func (k *keepInject) Capabilities() shaping.Caps                    { return 0 }
func (k *keepInject) Plan(*shaping.RecordView, *shaping.RecordPlan) {}
func (k *keepInject) OnEvent(e *shaping.Event, r *shaping.Effects) {
	switch e.Kind {
	case shaping.EventStart:
		r.Set, r.HasWake, r.Wake = shaping.EffectWake, true, e.Now+time.Microsecond
	case shaping.EventTimer:
		r.Set = shaping.EffectInject
		r.Injection = shaping.Injection{ID: 1, At: e.Now, PadBytes: 100}
	default:
	}
	if e.Kind == shaping.EventInjectionRejected {
		k.events++
	}
}

func TestDroppedInjectionKeptDoesNotSpin(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peerZmux = true
	s.cfg.ShapeLimits.NoInjection = true
	k := &keepInject{}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return k })
	now := Time(Second)
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if k.events != 1 {
		t.Fatalf("%d drops at one instant", k.events)
	}
	if d := s.ShapingDeadline(); d <= now {
		t.Fatalf("deadline %d at %d", d, now)
	}
}

// Extreme answers from a Shaper bound what they ask and never panic.
func TestShapeExtremes(t *testing.T) {
	for _, md := range []time.Duration{0, 1, time.Second} {
		f := &fixedPlan{caps: shaping.CapBudget}
		f.plan.Delay = math.MaxInt64
		f.plan.Pad, f.plan.PadTo, f.plan.PadMultiple, f.plan.MaxLen = math.MaxInt32, math.MaxInt32, math.MaxInt32, 1
		f.plan.Effects = shaping.Effects{Set: shaping.EffectGate, PauseUntil: math.MaxInt64, Rate: 1, Burst: math.MaxUint64}
		s, p, st := budgetSession(t, shaping.Limits{Profile: &shaping.ProfileLimits{WindowSeconds: 10, MaxAddedDelay: md}}, shaping.Budget{Wire: &shaping.Account{Ceiling: math.MaxUint64, Initial: math.MaxUint64, PerSec: math.MaxUint64}}, f)
		if _, err := s.Write(st, make([]byte, 5000), false); err != nil {
			t.Fatal(err)
		}
		for i := range 50 {
			now := Time(i) * Time(100*time.Millisecond)
			s.SetNow(now)
			s.shapeTimeout(now)
			poll(t, s, p, DefaultBatch, now)
			_ = s.ShapingDeadline()
		}
		s.shp.stats.StallSpent = math.MaxInt64
		s.shp.addStall(time.Hour)
		if s.shp.stats.StallSpent != math.MaxInt64 {
			t.Fatal("stall wrapped")
		}
		s.SetShapeLimits(shaping.Limits{Wire: &shaping.Account{Ceiling: 0, PerSec: shaping.MaxAccountRate}})
	}
	b := tokenBucket{rate: 1, burst: 1 << 53}
	b.reset(0)
	b.tokens = -(1 << 62)
	if _, ok := b.release(); ok {
		t.Log("release in range")
	}
	var a wireAccount
	a.set(0, &shaping.Account{Ceiling: shaping.MaxAccountInitial, Initial: shaping.MaxAccountInitial, PerMB: shaping.MaxAccountRate, PerSec: shaping.MaxAccountRate})
	a.accrue(math.MaxUint64, math.MaxUint64)
	if a.level != shaping.MaxAccountInitial {
		t.Fatalf("level %d", a.level)
	}
}

// An unshaped session has no shaping state, and a shaped one with no limit
// allocates nothing per record for its meters.
func TestMetersNoAllocs(t *testing.T) {
	f := &fixedPlan{caps: shaping.CapBudget}
	f.plan.Pad = 10
	s, _, _ := budgetSession(t, shaping.Limits{}, shaping.Budget{}, f)
	var sink shaping.Meters
	n := testing.AllocsPerRun(100, func() {
		sink = s.shapeMeters()
		s.shp.acct.spend(10)
		_ = s.shp.acct.charge(1, 1)
	})
	if n != 0 {
		t.Fatalf("%v allocations", n)
	}
	if sink.WireLeft != 0 || sink.StallLeft != math.MaxUint64 {
		t.Fatalf("%+v", sink)
	}
}

// A first batch's new bytes are sent bytes: session credit and the retention count
// them once, and what the batch carried goes again as reinjected information.
func TestFirstBatchChargesCreditOnce(t *testing.T) {
	s := capacityMP()
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 10000
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(st, make([]byte, 6000), false); err != nil {
		t.Fatal(err)
	}
	out, own := s.firstBatch(4000)
	carried := st.snd.sent
	if len(out) == 0 || own != carried || carried == 0 || s.conn.sent != carried || s.mp.retained != carried {
		t.Fatalf("batch %d: sent %d, session %d, retained %d", len(out), carried, s.conn.sent, s.mp.retained)
	}
	if m := st.snd.mps; m == nil || m.resend.len() != 1 || m.resend.first().lo != 0 || m.resend.first().hi != carried {
		t.Fatalf("not owed again: %+v", st.snd.mps)
	}
	// A later batch carries the same bytes: only what is new counts.
	s.firstBatch(4000)
	if s.conn.sent != st.snd.sent || st.snd.sent != carried {
		t.Fatalf("charged again: session %d, sent %d, was %d", s.conn.sent, st.snd.sent, carried)
	}
	// New bytes stay within the session's credit.
	s.conn.sendMax = carried + 10
	s.firstBatch(8000)
	if s.conn.sent != carried+10 {
		t.Fatalf("beyond the credit: %d, limit %d", s.conn.sent, carried+10)
	}
}

func TestWireAccountCeilingLifecycle(t *testing.T) {
	var a wireAccount
	p := shaping.Account{Ceiling: 100, Initial: 20, PerSec: 80, PerMB: 500_000}
	a.set(0, &p)
	snapshot := a
	snapshot.accrue(10_000_000, 0)
	if snapshot.left() != 100 || a.left() != 20 || !a.charge(10_000_000, 70) {
		t.Fatalf("projected %+v, account %+v", snapshot, a)
	}
	a.accrue(10_250_000, 0)
	if a.left() != 50 {
		t.Fatalf("balance=%d", a.left())
	}
	p.Initial = 95
	a.set(10_250_000, &p)
	a.set(10_250_000, nil)
	a.accrue(20_250_000, 1000)
	a.set(20_250_000, &p)
	if a.left() != 50 {
		t.Fatalf("restored balance=%d", a.left())
	}
	zero := shaping.Account{}
	a.set(20_250_000, &zero)
	a.set(20_250_000, &p)
	a.accrue(20_250_000, 1)
	a.accrue(20_256_250, 0)
	if a.left() != 1 || a.rem != 0 {
		t.Fatalf("combined remainder: %+v", a)
	}
}
