package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// injectShaper asks for one injection of n frame bytes when installed.
type injectShaper struct {
	injected []int
	n        int
}

func (x *injectShaper) Capabilities() shaping.Caps                    { return 0 }
func (x *injectShaper) Plan(*shaping.RecordView, *shaping.RecordPlan) {}
func (x *injectShaper) OnEvent(e *shaping.Event, r *shaping.Effects) {
	switch e.Kind {
	case shaping.EventStart:
		r.Set = shaping.EffectInject
		r.HasInject, r.InjectAt, r.InjectLen = true, e.Now, wireOf(x.n, true, 0)
	case shaping.EventInjected:
		x.injected = append(x.injected, e.Len)
		r.Set = shaping.EffectInject
	case shaping.EventInjectDropped:
		r.Set = shaping.EffectInject
	default: // the other events need no reply
	}
}

// recordSizes returns the sizes of the records of b.
func recordSizes(b *Batch) []int {
	var all []byte
	for _, x := range b.Bufs() {
		all = append(all, x...)
	}
	var out []int
	for len(all) > 0 {
		size, n := varint.ConsumeVarint(all)
		out = append(out, int(size))
		all = all[n+int(size):]
	}
	return out
}

func injectSession(t *testing.T, lim shaping.Limits, n int) (*Session, *Pipe, *injectShaper) {
	t.Helper()
	s, p := channelSession(t, nil, false)
	s.peerZmux = true
	s.cfg.ShapeLimits = lim
	sh := &injectShaper{n: n}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return sh })
	return s, p, sh
}

func poll(t *testing.T, s *Session, p *Pipe, limit int, now Time) []int {
	t.Helper()
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, limit, now) {
		return nil
	}
	sizes := recordSizes(b)
	s.WriteDone(b, nil, now)
	return sizes
}

// NoInjection and NoPadding allow no injection at all.
func TestShapingNoInjection(t *testing.T) {
	for _, lim := range []shaping.Limits{{NoInjection: true}, {NoPadding: true}} {
		s, p, sh := injectSession(t, lim, 1000)
		now := Time(Second)
		s.shapeTimeout(now)
		poll(t, s, p, DefaultBatch, now)
		if len(sh.injected) != 0 {
			t.Fatalf("%+v: injected %v", lim, sh.injected)
		}
	}
}

// An injection that does not fit the room left in a batch that already
// holds records waits for the next batch, which it leads at full length.
func TestShapingInjectWaitsForBatchRoom(t *testing.T) {
	s, p, sh := injectSession(t, shaping.Limits{}, 4000)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(st, make([]byte, 64<<10), false); err != nil {
		t.Fatal(err)
	}
	now := Time(Second)
	s.shapeTimeout(now)
	poll(t, s, p, 6000, now)
	if len(sh.injected) != 0 {
		t.Fatalf("injected %v in a batch without room", sh.injected)
	}
	poll(t, s, p, 6000, now)
	if len(sh.injected) != 1 || sh.injected[0] != 4000 {
		t.Fatalf("injected %v, want [4000]", sh.injected)
	}
	if c := s.ShapingStats().Clamped; c != 0 {
		t.Fatalf("clamped %d", c)
	}
}

// SetShapeLimits carries the account over: what was spent stays spent.
func TestShapingSetLimitsRuntime(t *testing.T) {
	s, _, _ := injectSession(t, shaping.Limits{Wire: shaping.Account{Initial: 1000}}, 0)
	s.shp.acct.spend(500)
	s.SetShapeLimits(shaping.Limits{Wire: shaping.Account{Initial: 100}})
	if l := s.shp.acct.left(); l != 100 {
		t.Fatalf("left %d", l)
	}
	s.SetShapeLimits(shaping.Limits{Wire: shaping.Account{Initial: 300}})
	if l := s.shp.acct.left(); l != 300 || s.shp.acct.spent != 500 {
		t.Fatalf("left %d spent %d", l, s.shp.acct.spent)
	}
}
