package zmux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// recConn records the bytes written to a carrier.
type recConn struct {
	net.Conn
	mu  sync.Mutex
	out []byte
	on  bool
}

func (c *recConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if c.on {
		c.out = append(c.out, b...)
	}
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *recConn) start() {
	time.Sleep(50 * time.Millisecond) // the session is idle: no batch is cut
	c.mu.Lock()
	c.on, c.out = true, nil
	c.mu.Unlock()
}

// records parses the recorded bytes as records: frames lengths and
// trailing zero bytes. An incomplete last record is left out.
func (c *recConn) records() (sizes, zeros []int) {
	c.mu.Lock()
	b := append([]byte(nil), c.out...)
	c.mu.Unlock()
	for len(b) > 0 {
		v, n := varint.ConsumeVarint(b)
		if n <= 0 || len(b) < n+int(v) {
			break // the tail of a batch still being written, buffer by buffer
		}
		r := b[n : n+int(v)]
		z := len(r) - len(bytes.TrimRight(r, "\x00"))
		sizes, zeros = append(sizes, int(v)), append(zeros, z)
		b = b[n+int(v):]
	}
	return
}

// fnShaper is a Shaper made of functions.
type fnShaper struct {
	caps  shaping.Caps
	plan  func(*shaping.RecordView, *shaping.RecordPlan)
	event func(*shaping.Event, *shaping.Effects)
}

func (f *fnShaper) Capabilities() shaping.Caps { return f.caps }
func (f *fnShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	if f.plan != nil {
		f.plan(v, p)
	}
}
func (f *fnShaper) OnEvent(e *shaping.Event, r *shaping.Effects) {
	if f.event != nil {
		f.event(e, r)
	}
}

func shapedPair(t *testing.T, cfg *Config) (*Conn, *Conn, *recConn) {
	t.Helper()
	a, b := tcpPair(t)
	rc := &recConn{Conn: a}
	c, s := pair(t, rc, b, cfg, nil)
	return c, s, rc
}

// shapedServerPair is shapedPair with the server's bytes recorded.
func shapedServerPair(t *testing.T, cfg *Config) (*Conn, *Conn, *recConn) {
	t.Helper()
	a, b := tcpPair(t)
	rs := &recConn{Conn: b}
	c, s := pair(t, a, rs, nil, cfg)
	return c, s, rs
}

// transfer writes n bytes on a new stream and checks that they arrive.
func transfer(t *testing.T, c, s *Conn, n int) {
	t.Helper()
	data := bytes.Repeat([]byte{7}, n)
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = st.Write(data)
		_ = st.Close()
	}()
	ctx := t.Context()
	in, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(in)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("data: %d bytes, %v", len(got), err)
	}
}

func planShaper(f func(*shaping.RecordPlan)) ShaperFactory {
	return func(ShapedPipe) shaping.Shaper {
		return &fnShaper{caps: shaping.CapPlan, plan: func(_ *shaping.RecordView, p *shaping.RecordPlan) { f(p) }}
	}
}

// recordCase shapes a transfer by one plan and bounds its records; lengths
// are wire bytes.
type recordCase struct {
	name     string
	limits   shaping.Limits
	plan     shaping.UnitPlan
	maxLen   int
	send     int
	repeat   int
	minLen   int
	wantMax  int
	mod, rem int
	records  int
	padBound uint64
	server   bool // the shaped sender is the server
	clamped  bool
	padding  bool
	minPad   int // wire bytes added by trailing PADDING to every record
}

// wireLen is the wire length of a record of n frame bytes on a byte stream.
func wireLen(n int) int { return n + varint.SizeVarint(uint64(n)) }

// paddedWire is the wire length that the trailing PADDING of a record of n
// frame bytes, z of them zero, adds.
func paddedWire(n, z int) int { return wireLen(n) - wireLen(n-z) }

func TestShapingRecords(t *testing.T) {
	for _, v := range []recordCase{
		{name: "pad_to", plan: shaping.UnitPlan{PadTo: 300}, send: 10, minLen: 300, padding: true},
		{name: "pad_multiple_and_pad", plan: shaping.UnitPlan{PadMultiple: 100, Pad: 3}, send: 1000, mod: 100, rem: 3},
		{name: "max_len", maxLen: 100, send: 5000, wantMax: 100, records: 50},
		{name: "clamp_to_limit", plan: shaping.UnitPlan{PadTo: 1 << 20}, send: 10, wantMax: wireLen(65535), clamped: true},
		{name: "account_does_not_bound_padding", limits: shaping.Limits{Wire: shaping.Account{Initial: 1}}, plan: shaping.UnitPlan{Pad: 300},
			send: 10, repeat: 2, minPad: 300, padding: true},
		{name: "server_pad_each_record", server: true, plan: shaping.UnitPlan{Pad: 16}, maxLen: 300, send: 20000,
			records: 60, minPad: 16},
	} {
		t.Run(v.name, func(t *testing.T) {
			cfg := &Config{ShapingLimits: v.limits}
			var c, s *Conn
			var rc *recConn
			if v.server {
				s, c, rc = shapedServerPair(t, cfg)
			} else {
				c, s, rc = shapedPair(t, cfg)
			}
			c.SetShaper(planShaper(func(p *shaping.RecordPlan) {
				p.UnitPlan, p.MaxLen = v.plan, v.maxLen
			}), nil)
			rc.start()
			for range max(v.repeat, 1) {
				transfer(t, c, s, v.send)
			}
			sizes, zeros := rc.records()
			if len(sizes) == 0 || len(sizes) < v.records {
				t.Fatalf("%d records", len(sizes))
			}
			for i, n := range sizes {
				w := wireLen(n)
				if w < v.minLen || v.wantMax > 0 && w > v.wantMax || v.mod > 0 && w%v.mod != v.rem || paddedWire(n, zeros[i]) < v.minPad {
					t.Fatalf("record of %d wire bytes, %d trailing zero bytes", w, zeros[i])
				}
			}
			st := c.ShapingStats()
			if v.padBound > 0 && st.WireSpent > v.padBound || v.clamped && st.Clamped == 0 ||
				v.padding && st.WireSpent == 0 {
				t.Fatalf("stats %+v", st)
			}
			if v.minPad > 0 && st.WireSpent < uint64(len(sizes)*(v.minPad-1)) { // a larger Size field takes a byte
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

func TestShapingInjectZmuxPeer(t *testing.T) {
	// Toward a zmux peer records of PADDING only have no rate limit.
	c, _, rc := shapedPair(t, nil)
	rc.start()
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Effects) {
			if e.Kind == shaping.EventStart || e.Kind == shaping.EventInjected {
				r.Set = shaping.EffectInject
				r.HasInject, r.InjectAt, r.InjectLen = true, e.Now+time.Millisecond, 20
			}
		}}
	}, nil)
	time.Sleep(500 * time.Millisecond)
	c.SetShaper(nil, nil)
	time.Sleep(50 * time.Millisecond)
	st := c.ShapingStats()
	sizes, zeros := rc.records()
	pad := 0
	for i := range sizes {
		if wireLen(sizes[i]) == 20 && zeros[i] == sizes[i] {
			pad++
		}
	}
	if st.Injected <= 16 || uint64(pad) != st.Injected || st.InjectDropped != 0 {
		t.Fatalf("stats %+v, %d records of PADDING only", st, pad)
	}
}

func TestShapingRate(t *testing.T) {
	c, s, _ := shapedPair(t, nil)
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Effects) {
			if e.Kind == shaping.EventStart {
				r.Set, r.Rate, r.Burst = shaping.EffectGate, 100000, 10000
			}
		}}
	}, nil)
	t0 := time.Now()
	transfer(t, c, s, 50000)
	if d := time.Since(t0); d < 300*time.Millisecond {
		t.Fatalf("50000 bytes in %v", d)
	}
}

func TestShapingRateCutsFrames(t *testing.T) {
	// The gate's room bounds a STREAM frame: no record exceeds the burst
	// by more than the frame overhead.
	c, s, rc := shapedPair(t, nil)
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Effects) {
			if e.Kind == shaping.EventStart {
				r.Set, r.Rate, r.Burst = shaping.EffectGate, 400000, 1000
			}
		}}
	}, nil)
	rc.start()
	transfer(t, c, s, 20000)
	sizes, _ := rc.records()
	for _, n := range sizes {
		if n > 1000+64 {
			t.Fatalf("record of %d bytes", n)
		}
	}
}

func TestShapingDelayResumes(t *testing.T) {
	// A record delayed by its plan carries stream data once the delay ends.
	c, s, _ := shapedPair(t, nil)
	c.SetShaper(planShaper(func(p *shaping.RecordPlan) {
		p.MaxLen, p.Delay = 1000, 5*time.Millisecond
	}), nil)
	t0 := time.Now()
	transfer(t, c, s, 10000)
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("10000 bytes in %v", d)
	}
}

func TestShapingTimerAndReplace(t *testing.T) {
	c, _, _ := shapedPair(t, nil)
	got := make(chan shaping.EventKind, 8)
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Effects) {
			switch e.Kind {
			case shaping.EventStart:
				r.Set, r.HasWake, r.Wake = shaping.EffectWake, true, e.Now+10*time.Millisecond
			case shaping.EventTimer:
				r.Set = shaping.EffectWake
			default: // the other events need no answer
			}
			got <- e.Kind
		}}
	}, nil)
	want := func(k shaping.EventKind) {
		select {
		case g := <-got:
			if g != k {
				t.Fatalf("event %d, want %d", g, k)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no event %d", k)
		}
	}
	want(shaping.EventStart)
	want(shaping.EventTimer)
	if c.SetPipeShaper(99, nil) {
		t.Fatal("unknown pipe accepted")
	}
	if !c.SetPipeShaper(0, nil) {
		t.Fatal("pipe 0 missing")
	}
	want(shaping.EventReplaced)
}

func TestShapingFault(t *testing.T) {
	c, s, _ := shapedPair(t, nil)
	c.SetShaper(planShaper(func(*shaping.RecordPlan) { panic("x") }), nil)
	transfer(t, c, s, 1000)
	if st := c.ShapingStats(); st.Faults != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// reachableWire is the wire length a record gets for the target l: the
// smallest reachable one, of at least min frame bytes.
func reachableWire(l, minFrames int) int {
	f := minFrames
	for wireLen(f) < l {
		f++
	}
	return wireLen(f)
}

// TestShapingExactSize checks that planned sizes are met exactly also when
// the last frame's Length field is what the plan's size depends on: a
// record cut at max_len and padded to the same pad_to, and pad_multiple
// targets, over targets and stream lengths whose records end on a frame
// boundary with no PADDING needed.
func TestShapingExactSize(t *testing.T) {
	type plan struct{ maxLen, padTo, mult, pad int }
	var plans []plan
	for _, n := range []int{64, 65, 66, 127, 128, 199, 200, 201, 998, 1000, 16383, 16384, 16385} {
		plans = append(plans, plan{maxLen: n, padTo: n})
	}
	for _, m := range []int{7, 50, 64, 100} {
		plans = append(plans, plan{mult: m}, plan{mult: m, pad: 1}, plan{maxLen: 3 * m, mult: m})
	}
	for _, p := range plans {
		t.Run(fmt.Sprintf("max%d_to%d_mult%d_pad%d", p.maxLen, p.padTo, p.mult, p.pad), func(t *testing.T) {
			c, s, rc := shapedPair(t, nil)
			c.SetShaper(planShaper(func(rp *shaping.RecordPlan) {
				rp.MaxLen, rp.PadTo, rp.PadMultiple, rp.Pad = p.maxLen, p.padTo, p.mult, p.pad
			}), nil)
			rc.start()
			for _, n := range []int{1, 60, 61, 62, 63, 195, 196, 197, 990, 995, 3000, 40000} {
				transfer(t, c, s, n)
			}
			sizes, _ := rc.records()
			if len(sizes) == 0 {
				t.Fatal("no records")
			}
			for _, n := range sizes {
				w := wireLen(n)
				switch {
				case p.padTo > 0 && w != reachableWire(p.padTo, shaping.MinMaxLen):
					t.Fatalf("record of %d wire bytes for %d (sizes %v)", w, p.padTo, sizes)
				case p.mult > 0 && !multipleReaches(w, p.mult, p.pad):
					t.Fatalf("record of %d wire bytes, want %d mod %d (sizes %v)", w, p.pad, p.mult, sizes)
				}
			}
		})
	}
}

// multipleReaches reports whether the wire length w is a multiple of mult plus
// pad, taken to the next reachable length.
func multipleReaches(w, mult, pad int) bool {
	for t := w - pad; t >= w-pad-2 && t >= 0; t-- {
		if t%mult == 0 && reachableWire(t+pad, 0) == w {
			return true
		}
	}
	return false
}

// A plan that sets only Pad adds that many wire bytes of PADDING to every
// record, data records included.
func TestShapingPadOnly(t *testing.T) {
	c, s, rc := shapedPair(t, nil)
	c.SetShaper(planShaper(func(p *shaping.RecordPlan) { p.Pad = 7 }), nil)
	rc.start()
	for _, n := range []int{1, 63, 64, 100, 5000} {
		transfer(t, c, s, n)
	}
	sizes, zeros := rc.records()
	if len(sizes) == 0 {
		t.Fatal("no records")
	}
	for i := range sizes {
		if paddedWire(sizes[i], zeros[i]) < 7 {
			t.Fatalf("record %d of %d bytes ends with %d PADDING bytes, want 7 wire bytes", i, sizes[i], zeros[i])
		}
	}
}

// Shaping deadlines are met without the host timer's lateness: PADDING
// records injected every 10 ms are about 10 ms apart, not a timer tick
// more.
func TestShapingTimerPrecision(t *testing.T) {
	c, _, _ := shapedPair(t, &Config{ShapingPreciseTimer: true})
	const every = 10 * time.Millisecond
	var mu sync.Mutex
	var at []time.Time
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{caps: shaping.CapPlan, event: func(e *shaping.Event, r *shaping.Effects) {
			if e.Kind == shaping.EventInjected {
				mu.Lock()
				at = append(at, time.Now())
				mu.Unlock()
			}
			if e.Kind == shaping.EventStart || e.Kind == shaping.EventInjected {
				r.Set = shaping.EffectInject
				r.HasInject, r.InjectAt, r.InjectLen = true, e.Now+every, 50
			}
		}}
	}, nil)
	time.Sleep(400 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	var gaps []time.Duration
	for i := 1; i < len(at); i++ {
		gaps = append(gaps, at[i].Sub(at[i-1]))
	}
	if len(gaps) < 10 {
		t.Fatalf("%d injections", len(at))
	}
	slices.Sort(gaps)
	// The upper bound leaves room for a loaded shared runner; a coarse
	// timer still shows as a doubled gap.
	if med := gaps[len(gaps)/2]; med < every || med > 2*every {
		t.Fatalf("median gap %v for injections every %v (gaps %v)", med, every, gaps)
	}
}

// TestShapingHugePadding: padding targets beyond a record are bounded by
// it instead of overflowing.
func TestShapingHugePadding(t *testing.T) {
	for _, f := range []func(*shaping.RecordPlan){
		func(p *shaping.RecordPlan) { p.PadTo, p.PadMultiple = math.MaxInt, 3 },
		func(p *shaping.RecordPlan) { p.PadMultiple = math.MaxInt },
		func(p *shaping.RecordPlan) { p.Pad = math.MaxInt },
		func(p *shaping.RecordPlan) { p.PadTo, p.Pad = 300, math.MaxInt },
		func(p *shaping.RecordPlan) { p.PadTo, p.PadMultiple = 300, math.MaxInt },
	} {
		c, s, rc := shapedPair(t, &Config{ShapingLimits: shaping.Limits{Wire: shaping.Account{Initial: 1 << 20}}})
		c.SetShaper(planShaper(f), nil)
		rc.start()
		transfer(t, c, s, 100)
		sizes, _ := rc.records()
		for _, n := range sizes {
			if n > 1<<16 { // the largest max_record_size
				t.Fatalf("record of %d bytes", n)
			}
		}
	}
}

// SetShapingLimits before a shaper is installed bounds it once installed;
// an invalid value changes nothing; concurrent calls are safe.
func TestSetShapingLimits(t *testing.T) {
	c, s, rc := shapedPair(t, nil)
	if err := c.SetShapingLimits(shaping.Limits{Wire: shaping.Account{PerMB: shaping.MaxAccountRate + 1}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ppm above 10^6: %v", err)
	}
	if c.ShapingLimits() != (shaping.Limits{}) {
		t.Fatal("an invalid value changed the limits")
	}
	want := shaping.Limits{Wire: shaping.Account{Initial: 4096}}
	if err := c.SetShapingLimits(want); err != nil {
		t.Fatal(err)
	}
	if c.ShapingLimits() != want {
		t.Fatalf("limits %+v", c.ShapingLimits())
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 50 {
				_ = c.SetShapingLimits(want)
				_ = c.ShapingLimits()
				_ = c.ShapingStats()
			}
		})
	}
	c.SetShaper(planShaper(func(p *shaping.RecordPlan) { p.Pad = 1000 }), nil)
	rc.start()
	transfer(t, c, s, 100<<10)
	wg.Wait()
	if st := c.ShapingStats(); st.WireSpent == 0 {
		t.Fatalf("stats %+v: padding is metered", st)
	}
}
