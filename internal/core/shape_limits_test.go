package core

import (
	"math"
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// injectShaper asks for one injection of n bytes when installed.
type injectShaper struct {
	n        int
	injected []int
}

func (x *injectShaper) Capabilities() shaping.Caps                    { return 0 }
func (x *injectShaper) Plan(*shaping.RecordView, *shaping.RecordPlan) {}
func (x *injectShaper) OnEvent(e *shaping.Event, r *shaping.Reply) {
	switch e.Kind {
	case shaping.EventStart:
		r.Inject = &shaping.Inject{Len: x.n}
	case shaping.EventInjected:
		x.injected = append(x.injected, e.Len)
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

// Under a padding ratio limit, the first injection of a session, of a
// whole default-size record, goes out at its full length (the startup
// allowance), and no clamp is counted.
func TestShapingRatioStartupAllowance(t *testing.T) {
	s, p, sh := injectSession(t, shaping.Limits{MaxPaddingRatio: 0.01}, shaping.StartupAllowance)
	now := Time(Second)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if len(sh.injected) != 1 || sh.injected[0] != shaping.StartupAllowance {
		t.Fatalf("injected %v, want [%d]", sh.injected, shaping.StartupAllowance)
	}
	if st := s.ShapingStats(); st.Clamped != 0 || st.PaddingBytes != shaping.StartupAllowance {
		t.Fatalf("stats %+v", st)
	}
}

// NoPaddingRatio, a ratio of 0, still allows the startup allowance and
// no more: unlike NoPadding, the first injection goes out whole, and one
// past the allowance waits however much else is sent.
func TestShapingNoPaddingRatio(t *testing.T) {
	s, p, sh := injectSession(t, shaping.Limits{NoPaddingRatio: true, MaxPaddingRatio: 0.5}, shaping.StartupAllowance)
	now := Time(Second)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if len(sh.injected) != 1 || sh.injected[0] != shaping.StartupAllowance {
		t.Fatalf("injected %v, want [%d]", sh.injected, shaping.StartupAllowance)
	}
	if hard, ratio := s.padRoom(); hard != math.MaxUint64 || ratio != 0 {
		t.Fatalf("room %d, %d after the allowance", hard, ratio)
	}
	s.stats.BytesSent += 1 << 30
	if _, ratio := s.padRoom(); ratio != 0 {
		t.Fatalf("ratio room %d: the ratio grew past 0", ratio)
	}
}

// An injection beyond the ratio allowance waits and later goes out whole,
// without a clamp; one cut by MaxPaddingBytes is counted as clamped.
func TestShapingInjectDeferredNotCut(t *testing.T) {
	s, p, sh := injectSession(t, shaping.Limits{MaxPaddingRatio: 0.5}, 1000)
	s.shp.stats.PaddingBytes = shaping.StartupAllowance // the allowance is spent
	s.stats.BytesSent = shaping.StartupAllowance
	now := Time(Second)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if len(sh.injected) != 0 || !p.sh.injDue || s.shapeWants(p, 0) {
		t.Fatalf("injected %v, due %v: not deferred", sh.injected, p.sh.injDue)
	}
	// Other bytes sent raise the ratio allowance above the injection.
	s.stats.BytesSent += 2*shaping.StartupAllowance + 4000
	if !s.shapeWants(p, 0) {
		t.Fatal("covered injection not wanted")
	}
	poll(t, s, p, DefaultBatch, now)
	if len(sh.injected) != 1 || sh.injected[0] != 1000 {
		t.Fatalf("injected %v, want [1000]", sh.injected)
	}
	if st := s.ShapingStats(); st.Clamped != 0 {
		t.Fatalf("stats %+v: clamped without a hard limit", st)
	}

	s, p, sh = injectSession(t, shaping.Limits{MaxPaddingBytes: 300}, 1000)
	s.shapeTimeout(now)
	poll(t, s, p, DefaultBatch, now)
	if len(sh.injected) != 1 || sh.injected[0] != 300 {
		t.Fatalf("injected %v, want [300]", sh.injected)
	}
	if st := s.ShapingStats(); st.Clamped != 1 {
		t.Fatalf("stats %+v: hard cut not counted", st)
	}
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
