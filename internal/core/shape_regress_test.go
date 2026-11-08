package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// driveShaped runs s on a virtual clock in steps of step until st has sent all
// it holds, preamble included; the test fails past limit.
func driveShaped(t *testing.T, s *Session, p *Pipe, st *Stream, step, limit Duration) {
	t.Helper()
	start := Time(Second)
	for now := start; now.Sub(start) <= limit; now = now.Add(step) {
		s.SetNow(now)
		s.shapeTimeout(now)
		b := GetBatch()
		s.PollTransmit(p.id, 0, b, DefaultBatch, now)
		s.WriteDone(b, nil, now)
		PutBatch(b)
		if st.idSet && st.pre == nil && st.snd.sent >= st.snd.q.end {
			return
		}
	}
	t.Fatalf("sent %d of %d in %v", st.snd.sent, st.snd.q.end, time.Duration(limit))
}

// A burst below a record's floor neither stalls the stream's first frame nor
// sends data a byte at a time, and repeating the answer counts one clamp.
func TestSmallBurstDoesNotStall(t *testing.T) {
	for _, c := range []struct {
		rate, burst uint64
		openInfo    int
	}{{1000, 1, 0}, {1000, 2, 0}, {1000, 4, 0}, {1, 1, 0}, {1000, 1, 300}, {1, 1, 300}} {
		s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{}, &fixedPlan{})
		if c.openInfo > 0 {
			s.feat.Preamble = true
			if err := s.SetOpenInfo(st, make([]byte, c.openInfo)); err != nil {
				t.Fatal(err)
			}
		}
		const n = 200
		if _, err := s.Write(st, make([]byte, n), false); err != nil {
			t.Fatal(err)
		}
		ps := mustShape(t, s, p)
		e := shaping.Effects{Set: shaping.EffectGate, Rate: c.rate, Burst: c.burst}
		base := s.ShapingStats().Clamped
		ps.apply(&e)
		ps.apply(&e)
		if ps.bucket.burst != shaping.MinMaxLen || ps.bucket.quantum() != shaping.MinMaxLen {
			t.Fatalf("%+v: burst %d quantum %d", c, ps.bucket.burst, ps.bucket.quantum())
		}
		// The rate floor under the default bound counts apart.
		want := uint64(1)
		if c.rate == 1 {
			want = 2
		}
		if got := s.ShapingStats().Clamped - base; got != want {
			t.Fatalf("%+v: %d clamps", c, got)
		}
		driveShaped(t, s, p, st, Duration(time.Millisecond), Duration(60*time.Second))
	}
}

// A preamble larger than what the open gate holds goes whole, in debt.
func TestPreambleOverdrawsOpenGate(t *testing.T) {
	s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{}, &fixedPlan{})
	s.feat.Preamble = true
	if err := s.SetOpenInfo(st, make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(st, []byte("x"), false); err != nil {
		t.Fatal(err)
	}
	ps := mustShape(t, s, p)
	ps.apply(&shaping.Effects{Set: shaping.EffectGate, Rate: 1000, Burst: 100})
	now := Time(0)
	s.SetNow(now)
	b := GetBatch()
	s.PollTransmit(p.id, 0, b, DefaultBatch, now)
	s.WriteDone(b, nil, now)
	PutBatch(b)
	if st.snd.sent < st.snd.preLen || st.snd.preLen <= 100 || ps.bucket.tokens >= 0 {
		t.Fatalf("sent %d preamble %d tokens %d", st.snd.sent, st.snd.preLen, ps.bucket.tokens)
	}
}

// A preamble larger than the plan's largest record still opens the stream.
func TestPreambleAboveMaxLen(t *testing.T) {
	f := &fixedPlan{plan: shaping.RecordPlan{MaxLen: shaping.MinMaxLen}}
	s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{}, f)
	s.feat.Preamble = true
	if err := s.SetOpenInfo(st, make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(st, make([]byte, 100), false); err != nil {
		t.Fatal(err)
	}
	driveShaped(t, s, p, st, Duration(time.Millisecond), Duration(time.Second))
}

// dropLoop answers each dropped injection with a new one a microsecond on, as a
// machine cycle whose inject after is small but not zero does.
type dropLoop struct{ drops int }

func (k *dropLoop) Capabilities() shaping.Caps                    { return 0 }
func (k *dropLoop) Plan(*shaping.RecordView, *shaping.RecordPlan) {}
func (k *dropLoop) OnEvent(e *shaping.Event, r *shaping.Effects) {
	switch e.Kind {
	case shaping.EventInjectDropped:
		k.drops++
	case shaping.EventStart:
	default:
		return
	}
	r.Set = shaping.EffectInject
	r.HasInject, r.InjectAt, r.InjectLen = true, e.Now+time.Microsecond, 100
}

// However short the answers' delays, drops come at most once a granularity.
func TestInjectDropRateBounded(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peerZmux = true
	s.cfg.ShapeLimits.NoInjection = true
	k := &dropLoop{}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return k })
	const span = 100 * Millisecond
	start := Time(Second)
	for now := start; now.Sub(start) < span; now = now.Add(Duration(10 * time.Microsecond)) {
		s.SetNow(now)
		s.shapeTimeout(now)
		poll(t, s, p, DefaultBatch, now)
	}
	if limit := int(span/limGranularity) + 1; k.drops == 0 || k.drops > limit {
		t.Fatalf("%d drops in %v, at most %d", k.drops, time.Duration(span), limit)
	}
}

// Replacing the rate settles the hold under the old one; a gate taken away
// settles it too, and neither counts the time after.
func TestHoldSettledOnGateChange(t *testing.T) {
	const ms = Duration(time.Millisecond)
	for _, next := range []shaping.Effects{
		{Set: shaping.EffectGate},
		{Set: shaping.EffectGate, Rate: 1 << 30},
		{Set: shaping.EffectGate, Rate: 1000, Burst: 1000},
	} {
		s, p, _ := budgetSession(t, shaping.Limits{MaxDelay: -1}, shaping.Budget{}, &fixedPlan{})
		ps := mustShape(t, s, p)
		ps.apply(&shaping.Effects{Set: shaping.EffectGate, Rate: 1000, Burst: 100})
		ps.bucket.tokens = -1000
		if !ps.held(s.now) {
			t.Fatal("not held")
		}
		base := s.ShapingStats().StallSpent
		s.SetNow(s.now.Add(10 * ms))
		ps.apply(&next)
		if ps.heldSince != Never {
			t.Fatalf("%+v: still held since %d", next, ps.heldSince)
		}
		if got := s.ShapingStats().StallSpent - base; got != 10*time.Millisecond {
			t.Fatalf("%+v: stall %v", next, got)
		}
		s.SetNow(s.now.Add(10 * ms))
		ps.held(s.now)
		if got := s.ShapingStats().StallSpent - base; got != 10*time.Millisecond {
			t.Fatalf("%+v: stall %v after the change", next, got)
		}
	}
}

// Two set accounts with nothing in common allow no injection, not any amount.
func TestDisjointAccountsAllowNoInjection(t *testing.T) {
	got := shaping.Limits{Wire: shaping.Account{Initial: 100}}.With(shaping.Budget{Wire: shaping.Account{PerSec: 7}})
	if !got.NoInjection || got.Wire != (shaping.Account{}) {
		t.Fatalf("%+v", got)
	}
}
