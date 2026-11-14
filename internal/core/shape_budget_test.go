package core

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

type vecAccount struct {
	Initial uint64 `json:"initial"`
	PerMB   uint64 `json:"per_mb"`
	PerSec  uint64 `json:"per_sec"`
}

func (a *vecAccount) go_() shaping.Account {
	if a == nil {
		return shaping.Account{}
	}
	return shaping.Account{Initial: a.Initial, PerMB: a.PerMB, PerSec: a.PerSec}
}

func TestBudgetVectors(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/shaping/budget.json")
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		Merge []struct {
			Local, Config struct {
				Wire     vecAccount `json:"wire"`
				MaxDelay int64      `json:"max_delay"`
			}
			Want struct {
				Wire        vecAccount `json:"wire"`
				NoInjection bool       `json:"no_injection"`
				MaxDelay    int64      `json:"max_delay"`
			} `json:"want"`
		} `json:"merge"`
		BurstFloor []struct {
			Burst     uint64 `json:"burst"`
			RecordMax int64  `json:"record_max"`
			Want      int64  `json:"want"`
			Clamped   bool   `json:"clamped"`
		} `json:"burst_floor"`
		Account []struct {
			Name    string      `json:"name"`
			Account *vecAccount `json:"account"`
			Steps   []struct {
				Account *vecAccount `json:"account"`
				Granted *bool       `json:"granted"`
				Level   *uint64     `json:"level"`
				Op      string      `json:"op"`
				Now     uint64      `json:"now"`
				Earn    uint64      `json:"earn"`
				N       uint64      `json:"n"`
				Left    uint64      `json:"left"`
				Spent   uint64      `json:"spent"`
			} `json:"steps"`
		} `json:"account"`
		RateFloor []struct {
			Rate     uint64 `json:"rate"`
			MaxDelay int64  `json:"max_delay"`
			Want     uint64 `json:"want"`
			Clamped  bool   `json:"clamped"`
		} `json:"rate_floor"`
		WirePPM []struct {
			S    uint64 `json:"s"`
			E    uint64 `json:"e"`
			Want uint64 `json:"want"`
		} `json:"wire_ppm"`
		Gate []struct {
			Name  string `json:"name"`
			Steps []struct {
				Release *uint64 `json:"release"`
				Now     uint64  `json:"now"`
				Take    int64   `json:"take"`
				Tokens  int64   `json:"tokens"`
				Held    bool    `json:"held"`
			} `json:"steps"`
			Rate  uint64 `json:"rate"`
			Burst int64  `json:"burst"`
		} `json:"gate"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Account) == 0 || len(d.Gate) == 0 || len(d.Merge) == 0 {
		t.Fatal("empty vectors")
	}
	for i, c := range d.Merge {
		got := shaping.Limits{Wire: c.Local.Wire.go_(), MaxDelay: time.Duration(c.Local.MaxDelay) * time.Microsecond}.With(
			shaping.Budget{Wire: c.Config.Wire.go_(), MaxDelay: time.Duration(c.Config.MaxDelay) * time.Microsecond})
		md := got.MaxDelay
		if md > 0 {
			md /= time.Microsecond
		}
		if got.Wire != c.Want.Wire.go_() || got.NoInjection != c.Want.NoInjection || int64(md) != c.Want.MaxDelay && c.Want.MaxDelay >= 0 || c.Want.MaxDelay < 0 && md >= 0 {
			t.Errorf("merge %d: %+v", i, got)
		}
	}
	for _, c := range d.Account {
		var a wireAccount
		a.set(0, 0, c.Account.go_())
		for i, st := range c.Steps {
			switch st.Op {
			case "spend":
				a.spend(st.N)
			case "charge":
				if g := a.charge(st.Now, st.Earn, st.N); st.Granted == nil || g != *st.Granted {
					t.Errorf("%s %d: granted %v", c.Name, i, g)
				}
			case "set":
				a.set(st.Now, st.Earn, st.Account.go_())
			}
			a.accrue(st.Now, st.Earn)
			if a.left() != st.Left || a.spent != st.Spent || (st.Level == nil) == a.on || st.Level != nil && *st.Level != a.level {
				t.Errorf("%s %d: left %d spent %d level %d on %v", c.Name, i, a.left(), a.spent, a.level, a.on)
			}
		}
	}
	for i, c := range d.RateFloor {
		if r, cl := shapeRateFloor(c.Rate, c.MaxDelay); r != c.Want || cl != c.Clamped {
			t.Errorf("rate_floor %d: %d %v", i, r, cl)
		}
	}
	for i, c := range d.BurstFloor {
		if b, cl := shapeBurstFloor(c.Burst, c.RecordMax); b != c.Want || cl != c.Clamped {
			t.Errorf("burst_floor %d: %d %v", i, b, cl)
		}
	}
	for i, c := range d.WirePPM {
		if g := shaping.WirePPM(c.S, c.E); g != c.Want {
			t.Errorf("wire_ppm %d: %d", i, g)
		}
	}
	for _, c := range d.Gate {
		b := tokenBucket{rate: c.Rate, burst: c.Burst}
		b.reset(0)
		for i, st := range c.Steps {
			b.fill(st.Now)
			if b.held() != st.Held || b.tokens != st.Tokens {
				t.Errorf("%s %d: held %v tokens %d", c.Name, i, b.held(), b.tokens)
			}
			if st.Release != nil {
				if us, ok := b.release(); !ok || us != *st.Release {
					t.Errorf("%s %d: release %d %v", c.Name, i, us, ok)
				}
			}
			b.tokens -= st.Take
		}
	}
}

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
			r.Set = shaping.EffectInject
			r.HasInject, r.InjectAt, r.InjectLen = true, e.Now, f.inject
		}
	case shaping.EventInjected:
		f.injected++
		r.Set = shaping.EffectInject
	case shaping.EventInjectDropped:
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
	s, p, st := budgetSession(t, shaping.Limits{Wire: shaping.Account{Initial: 1}}, shaping.Budget{}, f)
	if _, err := s.Write(st, make([]byte, 50), false); err != nil {
		t.Fatal(err)
	}
	now := Time(Second)
	s.SetNow(now)
	sizes := poll(t, s, p, DefaultBatch, now)
	if len(sizes) == 0 || sizes[0] < 190 {
		t.Fatalf("sizes %v", sizes)
	}
	if got := s.ShapingStats(); got.WireSpent == 0 || got.InjectDropped != 0 {
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
	s, p, _ := budgetSession(t, shaping.Limits{Wire: shaping.Account{Initial: 100}}, shaping.Budget{}, f)
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if f.dropped != 1 || f.injected != 0 || s.shp.acct.left() != 100 || s.ShapingStats().WireSpent != 0 {
		t.Fatalf("dropped %d injected %d left %d", f.dropped, f.injected, s.shp.acct.left())
	}
	f = &fixedPlan{inject: 500}
	s, p, _ = budgetSession(t, shaping.Limits{Wire: shaping.Account{Initial: 600}}, shaping.Budget{}, f)
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if f.injected != 1 || s.shp.acct.left() >= 600 || s.ShapingStats().Injected != 1 {
		t.Fatalf("injected %d left %d", f.injected, s.shp.acct.left())
	}
	// merged into the open record: refused before anything changes
	f = &fixedPlan{inject: 500}
	s, p, st := budgetSession(t, shaping.Limits{Wire: shaping.Account{Initial: 100}}, shaping.Budget{}, f)
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
	s, p, st = budgetSession(t, shaping.Limits{Wire: shaping.Account{Initial: 1000}}, shaping.Budget{}, f)
	if _, err := s.Write(st, make([]byte, 50), false); err != nil {
		t.Fatal(err)
	}
	s.SetNow(now)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if f.injected != 1 || s.shp.acct.left() != 502 || s.ShapingStats().WireSpent != 498 {
		t.Fatalf("injected %d left %d spent %d", f.injected, s.shp.acct.left(), s.ShapingStats().WireSpent)
	}
}

// A shaper that keeps a refused injection is not asked again at once.
type keepInject struct{ events int }

func (k *keepInject) Capabilities() shaping.Caps                    { return 0 }
func (k *keepInject) Plan(*shaping.RecordView, *shaping.RecordPlan) {}
func (k *keepInject) OnEvent(e *shaping.Event, r *shaping.Effects) {
	if e.Kind == shaping.EventStart {
		r.Set = shaping.EffectInject
		r.HasInject, r.InjectAt, r.InjectLen = true, e.Now, 100
	}
	if e.Kind == shaping.EventInjectDropped {
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

// A configuration cannot widen local limits.
func TestBudgetCannotWidenLimits(t *testing.T) {
	huge := shaping.Account{Initial: math.MaxUint64, PerMB: math.MaxUint64, PerSec: math.MaxUint64}
	got := shaping.Limits{Wire: shaping.Account{Initial: 5, PerMB: 6}, MaxDelay: 300 * time.Millisecond, NoInjection: true, NoPadding: true, NoPause: true}.
		With(shaping.Budget{Wire: huge, MaxDelay: time.Hour})
	if got.Wire != (shaping.Account{Initial: 5, PerMB: 6}) || got.MaxDelay != 300*time.Millisecond || !got.NoInjection || !got.NoPadding || !got.NoPause {
		t.Fatalf("%+v", got)
	}
	if got := (shaping.Limits{}).With(shaping.Budget{MaxDelay: time.Hour}); got.MaxDelay != time.Second {
		t.Fatalf("default bound widened: %v", got.MaxDelay)
	}
	if got := (shaping.Limits{}).With(shaping.Budget{Wire: huge, MaxDelay: -time.Hour}); got.MaxDelay != time.Second || got.Wire.PerSec != shaping.MaxAccountRate {
		t.Fatalf("%+v", got)
	}
}

// Extreme answers from a Shaper bound what they ask and never panic.
func TestShapeExtremes(t *testing.T) {
	for _, md := range []time.Duration{0, -1, 1, time.Second} {
		f := &fixedPlan{caps: shaping.CapBudget}
		f.plan.Delay = math.MaxInt64
		f.plan.Pad, f.plan.PadTo, f.plan.PadMultiple, f.plan.MaxLen = math.MaxInt32, math.MaxInt32, math.MaxInt32, 1
		f.plan.Effects = shaping.Effects{Set: shaping.EffectGate, PauseUntil: math.MaxInt64, Rate: 1, Burst: math.MaxUint64}
		s, p, st := budgetSession(t, shaping.Limits{MaxDelay: md}, shaping.Budget{Wire: shaping.Account{Initial: math.MaxUint64, PerSec: math.MaxUint64}}, f)
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
		s.SetShapeLimits(shaping.Limits{MaxDelay: -1, Wire: shaping.Account{PerSec: shaping.MaxAccountRate}})
	}
	b := tokenBucket{rate: 1, burst: 1 << 53}
	b.reset(0)
	b.tokens = -(1 << 62)
	if _, ok := b.release(); ok {
		t.Log("release in range")
	}
	var a wireAccount
	a.set(0, 0, shaping.Account{Initial: shaping.MaxAccountInitial, PerMB: shaping.MaxAccountRate, PerSec: shaping.MaxAccountRate})
	a.accrue(math.MaxUint64, math.MaxUint64)
	if a.level != math.MaxInt64 {
		t.Fatalf("level %d", a.level)
	}
}

// The default bound shortens a long delay and lifts a rate too low to move.
func TestDelayBoundsDefault(t *testing.T) {
	f := &fixedPlan{}
	f.plan.Delay = time.Hour
	s, _, _ := budgetSession(t, shaping.Limits{}, shaping.Budget{}, f)
	if d := s.shapePause(Duration(time.Hour)); d != Duration(time.Second) {
		t.Fatalf("pause %v", time.Duration(d))
	}
	s.SetShapeLimits(shaping.Limits{MaxDelay: -1})
	if d := s.shapePause(Duration(time.Hour)); d != Duration(time.Hour) {
		t.Fatalf("unbounded pause %v", time.Duration(d))
	}
	s, _, _ = budgetSession(t, shaping.Limits{}, shaping.Budget{MaxDelay: 100 * time.Millisecond}, &fixedPlan{})
	if d := s.shapePause(Duration(time.Hour)); d != Duration(100*time.Millisecond) {
		t.Fatalf("configured pause %v", time.Duration(d))
	}
}

func TestRateFloorInForce(t *testing.T) {
	s, p, _ := budgetSession(t, shaping.Limits{}, shaping.Budget{}, &fixedPlan{})
	ps := mustShape(t, s, p)
	ps.apply(&shaping.Effects{Set: shaping.EffectGate, Rate: 1})
	if ps.bucket.rate != 64 || s.ShapingStats().Clamped != 1 {
		t.Fatalf("rate %d stats %+v", ps.bucket.rate, s.ShapingStats())
	}
	s.SetShapeLimits(shaping.Limits{MaxDelay: -1})
	if ps.bucket.rate != 1 {
		t.Fatalf("rate %d with no bound", ps.bucket.rate)
	}
	s.SetShapeLimits(shaping.Limits{MaxDelay: 100 * time.Millisecond})
	if ps.bucket.rate != 640 {
		t.Fatalf("rate %d under 100 ms", ps.bucket.rate)
	}
}

// earnBytes rises only: padding, injections and copies earn nothing.
func TestEarnBytes(t *testing.T) {
	f := &fixedPlan{}
	f.plan.PadTo = 1000
	s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{}, f)
	if _, err := s.Write(st, make([]byte, 100), false); err != nil {
		t.Fatal(err)
	}
	now := Time(Second)
	s.SetNow(now)
	base := s.earnBytes
	poll(t, s, p, DefaultBatch, now)
	got := s.earnBytes - base
	if got < 100 || got > 200 {
		t.Fatalf("earned %d for 100 bytes of data padded to 1000", got)
	}
	s.earnSkip = 1 << 40
	before := s.earnBytes
	if _, err := s.Write(st, make([]byte, 100), false); err != nil {
		t.Fatal(err)
	}
	poll(t, s, p, DefaultBatch, now)
	if s.earnBytes != before || s.earnSkip == 1<<40 {
		t.Fatalf("resent first flight earned: %d", s.earnBytes-before)
	}
}

// An unshaped session has no shaping state, and a shaped one with no limit
// allocates nothing per record for its meters.
func TestMetersNoAllocs(t *testing.T) {
	f := &fixedPlan{caps: shaping.CapBudget}
	f.plan.Pad = 10
	s, _, _ := budgetSession(t, shaping.Limits{}, shaping.Budget{}, f)
	var sink shaping.Meters
	if n := testing.AllocsPerRun(100, func() {
		sink = s.shapeMeters()
		s.shp.acct.spend(10)
		_ = s.shp.acct.charge(1, 1, 1)
	}); n != 0 {
		t.Fatalf("%v allocations", n)
	}
	if sink.WireLeft != math.MaxUint64 || sink.StallLeft != math.MaxUint64 {
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
	out := s.firstBatch(4000)
	carried := st.snd.sent
	if len(out) == 0 || carried == 0 || s.conn.sent != carried || s.mp.retained != carried {
		t.Fatalf("batch %d: sent %d, session %d, retained %d", len(out), carried, s.conn.sent, s.mp.retained)
	}
	if m := st.snd.mps; m == nil || len(m.resend) != 1 || m.resend[0].lo != 0 || m.resend[0].hi != carried {
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

// A record that closed its batch defers a due injection to the next, uncharged.
func TestInjectionAfterFlushDeferred(t *testing.T) {
	f := &fixedPlan{}
	s, p, _ := budgetSession(t, shaping.Limits{Wire: shaping.Account{Initial: 1000}}, shaping.Budget{}, f)
	ps := mustShape(t, s, p)
	ps.injDue, ps.injLen = true, 100
	var w recWriter
	b := GetBatch()
	defer PutBatch(b)
	w.init(b, FramingBytes, 16<<10, 64<<10)
	w.sh, w.closed = ps, true
	s.shapeInject(&w)
	got := s.ShapingStats()
	if !ps.injDue || !ps.injFirst || got.InjectDropped != 0 || got.WireSpent != 0 {
		t.Fatalf("due %v first %v stats %+v", ps.injDue, ps.injFirst, got)
	}
}

// On a message carrier earnBytes counts what the carrier is given: no size field, and
// a record of padding adds nothing.
func TestEarnBytesOnMessages(t *testing.T) {
	var w recWriter
	b := GetBatch()
	defer PutBatch(b)
	w.init(b, FramingRecords, 16<<10, 64<<10)
	w.begin()
	w.pad(1000)
	w.end()
	if w.notEarned != 1000 {
		t.Fatalf("notEarned %d", w.notEarned)
	}
}
