package zmux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

func mustSetShaper(t testing.TB, c *Conn, f ShaperFactory, budget func() shaping.Budget) {
	t.Helper()
	if budget == nil && c.ShapingLimits().Profile != nil {
		profile := c.ShapingLimits().Profile
		budget = func() shaping.Budget { return shaping.Budget{Profile: profile} }
	}
	var policy *ShapingPolicy
	if f != nil || budget != nil {
		policy = &ShapingPolicy{Factory: f, Budget: budget}
	}
	if err := c.SetShaper(policy); err != nil {
		t.Fatal(err)
	}
}

// recConn captures writes from the beginning of the byte stream. A start mark
// can fall between buffers of one record, so decoding always starts at byte 0.
type recConn struct {
	net.Conn
	out  []byte
	mu   sync.Mutex
	from int
}

func (c *recConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.out = append(c.out, b...)
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *recConn) start() {
	c.mu.Lock()
	c.from = len(c.out)
	c.mu.Unlock()
}

// records reports the complete records that began at or after the start mark.
// A stream transfer can be accompanied by session maintenance records.
func (c *recConn) records(streamOnly bool) (sizes, zeros []int) {
	c.mu.Lock()
	b := append([]byte(nil), c.out...)
	from := c.from
	c.mu.Unlock()
	for offset := 0; len(b) > 0; {
		v, n := varint.ConsumeVarint(b)
		if n <= 0 || v > uint64(len(b)-n) {
			break // the rest of this record has not been written yet
		}
		end := n + int(v)
		r := b[n:end]
		if offset >= from && (!streamOnly || hasStreamFrame(r)) {
			z := len(r) - len(bytes.TrimRight(r, "\x00"))
			sizes, zeros = append(sizes, int(v)), append(zeros, z)
		}
		offset += end
		b = b[end:]
	}
	return sizes, zeros
}

func hasStreamFrame(b []byte) bool {
	for len(b) > 0 {
		var f wire.Frame
		n, err := wire.DecodeFrame(b, &f)
		if err != nil {
			return false
		}
		if f.Kind == wire.KindStream {
			return true
		}
		b = b[n:]
	}
	return false
}

// fnShaper is a Shaper made of functions.
type fnShaper struct {
	plan  func(*shaping.RecordView, *shaping.RecordPlan)
	event func(*shaping.Event, *shaping.Effects)
	caps  shaping.Caps
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

// shapedPair returns the sender (client or server), its peer and a recorder.
// Run it inside synctest so both sessions use the bubble's clock.
func shapedPair(t *testing.T, cfg *Config, server bool) (*Conn, *Conn, *recConn) {
	t.Helper()
	a, b := net.Pipe()
	rc := &recConn{Conn: a}
	cfg = cfg.Clone()
	cfg.ClockDomain = NewClockDomain(timeshim.Real)
	peer := &Config{ClockDomain: cfg.ClockDomain}
	var c, s *Conn
	if server {
		c, s = pair(t, b, rc, peer, cfg)
		c, s = s, c
	} else {
		c, s = pair(t, rc, b, cfg, peer)
	}
	synctest.Wait()
	return c, s, rc
}

func fundedShapePair(t *testing.T, limits shaping.Limits, server bool, baseline int) (*Conn, *Conn, *recConn) {
	t.Helper()
	limits.Profile = &shaping.ProfileLimits{WindowSeconds: 10, ExtraBytesPPM: 1000000, FloorBytes: 1 << 20, MaxAddedDelay: 2 * time.Millisecond}
	c, peer, rc := shapedPair(t, &Config{ShapingLimits: limits}, server)
	transfer(t, c, peer, baseline)
	synctest.Wait()
	return c, peer, rc
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

func timedCover(every time.Duration, samples int, accepted func()) ShaperFactory {
	return func(ShapedPipe) shaping.Shaper {
		count := 0
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Effects) {
			switch e.Kind {
			case shaping.EventStart:
				r.Set, r.HasWake, r.Wake = shaping.EffectWake, true, e.Now+every
			case shaping.EventTimer:
				r.Set = shaping.EffectInject
				r.Injection = shaping.Injection{ID: uint64(count + 1), At: e.Now, PadBytes: 19}
			case shaping.EventInjectionAccepted:
				count++
				if count < samples {
					r.Set, r.HasWake, r.Wake = shaping.EffectWake, true, e.Now+every
				}
				accepted()
			default: // Other events do not change the timer.
			}
		}}
	}
}

// recordCase shapes a transfer by one plan and bounds its records; lengths
// are wire bytes.
type recordCase struct {
	name     string
	limits   shaping.Limits
	plan     shaping.UnitPlan
	send     int
	repeat   int
	minLen   int
	wantMax  int
	mod, rem int
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
		{name: "clamp_to_limit", plan: shaping.UnitPlan{PadTo: 1 << 20}, send: 10, wantMax: wireLen(65535), clamped: true},
		{name: "account_does_not_bound_padding", limits: shaping.Limits{Wire: &shaping.Account{Ceiling: 1, Initial: 1}}, plan: shaping.UnitPlan{Pad: 300},
			send: 10, repeat: 2, minPad: 300, padding: true},
		{name: "server_pad_each_record", server: true, plan: shaping.UnitPlan{Pad: 16}, send: 20000, minPad: 16},
	} {
		t.Run(v.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, s, rc := fundedShapePair(t, v.limits, v.server, 32<<10)
				mustSetShaper(t, c, planShaper(func(p *shaping.RecordPlan) {
					p.UnitPlan = v.plan
				}), nil)
				rc.start()
				for range max(v.repeat, 1) {
					transfer(t, c, s, v.send)
				}
				synctest.Wait()
				sizes, zeros := rc.records(true)
				if len(sizes) == 0 {
					t.Fatalf("%d records", len(sizes))
				}
				for i, n := range sizes {
					w := wireLen(n)
					if w < v.minLen || v.wantMax > 0 && w > v.wantMax || v.mod > 0 && w%v.mod != v.rem || paddedWire(n, zeros[i]) < v.minPad {
						t.Fatalf("record of %d wire bytes, %d trailing zero bytes", w, zeros[i])
					}
				}
				st := c.ShapingStats()
				if v.clamped && st.Clamped == 0 || v.padding && c.ShapingMeters().ConsumedBytes == 0 {
					t.Fatalf("stats %+v", st)
				}
				if v.minPad > 0 && c.ShapingMeters().ConsumedBytes < uint64(len(sizes)*(v.minPad-1)) { // a larger Size field takes a byte
					t.Fatalf("stats %+v", st)
				}
			})
		})
	}
}

func TestShapingInjectZmuxPeer(t *testing.T) {
	// A zmux peer permits standalone cover beyond the plain QMux rate limit.
	synctest.Test(t, func(t *testing.T) {
		const count = 32
		limits := shaping.Limits{Wire: &shaping.Account{Ceiling: 1024, Initial: 1024}}
		c, _, rc := fundedShapePair(t, limits, false, 1024)
		rc.start()
		accepted := 0
		done := make(chan struct{})
		mustSetShaper(t, c, timedCover(time.Millisecond, count, func() {
			accepted++
			if accepted == count {
				close(done)
			}
		}), func() shaping.Budget { return shaping.Budget{Wire: limits.Wire, Profile: c.ShapingLimits().Profile} })
		waitTestSignal(t, done, "padding injections")
		synctest.Wait()
		st := c.ShapingStats()
		sizes, zeros := rc.records(false)
		pad := 0
		for i := range sizes {
			if sizes[i] == 19 && zeros[i] == sizes[i] {
				pad++
			}
		}
		if pad <= 16 || st.InjectionAccepted != count || st.InjectionSentBytes != count*19 || st.InjectionRejected != 0 {
			t.Fatalf("stats %+v, %d records of PADDING only", st, pad)
		}
	})
}

func TestShapingProfileAppliesFeasibleRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer, _ := shapedPair(t, nil, false)
		mustSetShaper(t, c, func(ShapedPipe) shaping.Shaper {
			return &fnShaper{event: func(e *shaping.Event, r *shaping.Effects) {
				if e.Kind == shaping.EventStart {
					r.Set, r.Rate, r.Burst = shaping.EffectGate, 100000, 10000
				}
			}}
		}, nil)
		transfer(t, c, peer, 50000)
		synctest.Wait()
		if stats := c.ShapingStats(); stats.RuntimeReasons[shaping.ReasonUnsupportedCapability] != 0 || stats.MaxAddedDelay > shaping.Lightweight().MaxAddedDelay {
			t.Fatalf("feasible rate gate: %+v", stats)
		}
	})
}

func TestShapingDelayResumes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s, _ := shapedPair(t, nil, false)
		const delay = 5 * time.Millisecond
		mustSetShaper(t, c, planShaper(func(p *shaping.RecordPlan) {
			p.MaxLen, p.Delay = 1000, delay
		}), nil)
		transfer(t, c, s, 10000)
		synctest.Wait()
		if got := c.ShapingMeters().MaxAddedDelay; got != 2*time.Millisecond {
			t.Fatalf("Lightweight added delay = %v, want 2 ms", got)
		}
	})
}

func TestShapingTimerAndReplace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _, _ := shapedPair(t, nil, false)
		got := make(chan shaping.EventKind, 8)
		mustSetShaper(t, c, func(ShapedPipe) shaping.Shaper {
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
		if err := c.SetShaper(nil); err != nil {
			t.Fatal(err)
		}
		want(shaping.EventReplaced)
	})
}

func TestShapingFault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s, _ := shapedPair(t, nil, false)
		mustSetShaper(t, c, planShaper(func(*shaping.RecordPlan) { panic("x") }), nil)
		transfer(t, c, s, 1000)
		if st := c.ShapingStats(); st.Faults != 1 {
			t.Fatalf("stats %+v", st)
		}
	})
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

// TestShapingExactSize exercises byte-framing boundaries for padding targets.
// The selected unshaped record can already exceed a PadTo minimum.
func TestShapingExactSize(t *testing.T) {
	type plan struct{ maxLen, padTo, mult, pad int }
	plans := make([]plan, 0, 25)
	for _, n := range []int{64, 65, 66, 127, 128, 199, 200, 201, 998, 1000, 16383, 16384, 16385} {
		plans = append(plans, plan{maxLen: n, padTo: n})
	}
	for _, m := range []int{7, 50, 64, 100} {
		plans = append(plans, plan{mult: m}, plan{mult: m, pad: 1}, plan{maxLen: 3 * m, mult: m})
	}
	for _, p := range plans {
		t.Run(fmt.Sprintf("max%d_to%d_mult%d_pad%d", p.maxLen, p.padTo, p.mult, p.pad), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, s, rc := fundedShapePair(t, shaping.Limits{}, false, 256<<10)
				mustSetShaper(t, c, planShaper(func(rp *shaping.RecordPlan) {
					rp.MaxLen, rp.PadTo, rp.PadMultiple, rp.Pad = p.maxLen, p.padTo, p.mult, p.pad
				}), nil)
				rc.start()
				for _, n := range []int{1, 60, 61, 62, 63, 195, 196, 197, 990, 995, 3000, 40000} {
					transfer(t, c, s, n)
				}
				sizes, _ := rc.records(true)
				if len(sizes) == 0 {
					t.Fatal("no records")
				}
				for _, n := range sizes {
					w := wireLen(n)
					switch {
					case p.padTo > 0 && w < reachableWire(p.padTo, 0):
						t.Fatalf("record of %d wire bytes for %d (sizes %v)", w, p.padTo, sizes)
					case p.mult > 0 && !multipleReaches(w, p.mult, p.pad):
						t.Fatalf("record of %d wire bytes, want %d mod %d (sizes %v)", w, p.pad, p.mult, sizes)
					}
				}
				if p.maxLen > 0 && c.ShapingStats().RuntimeReasons[shaping.ReasonUnsupportedCapability] == 0 {
					t.Fatal("unpriced segmentation was not reported")
				}
			})
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
	synctest.Test(t, func(t *testing.T) {
		c, s, rc := fundedShapePair(t, shaping.Limits{}, false, 256)
		mustSetShaper(t, c, planShaper(func(p *shaping.RecordPlan) { p.Pad = 7 }), nil)
		rc.start()
		for _, n := range []int{1, 63, 64, 100, 5000} {
			transfer(t, c, s, n)
		}
		synctest.Wait()
		sizes, zeros := rc.records(true)
		if len(sizes) == 0 {
			t.Fatal("no records")
		}
		for i := range sizes {
			if paddedWire(sizes[i], zeros[i]) < 7 {
				t.Fatalf("record %d of %d bytes ends with %d PADDING bytes, want 7 wire bytes", i, sizes[i], zeros[i])
			}
		}
	})
}

// TestShapingHugePadding: padding targets beyond a record are bounded by
// it instead of overflowing.
func TestShapingHugePadding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, f := range []func(*shaping.RecordPlan){
			func(p *shaping.RecordPlan) { p.PadTo, p.PadMultiple = math.MaxInt, 3 },
			func(p *shaping.RecordPlan) { p.PadMultiple = math.MaxInt },
			func(p *shaping.RecordPlan) { p.Pad = math.MaxInt },
			func(p *shaping.RecordPlan) { p.PadTo, p.Pad = 300, math.MaxInt },
			func(p *shaping.RecordPlan) { p.PadTo, p.PadMultiple = 300, math.MaxInt },
		} {
			c, s, rc := shapedPair(t, &Config{ShapingLimits: shaping.Limits{Wire: &shaping.Account{Ceiling: 1 << 20, Initial: 1 << 20}}}, false)
			mustSetShaper(t, c, planShaper(f), nil)
			rc.start()
			transfer(t, c, s, 100)
			sizes, _ := rc.records(true)
			if len(sizes) == 0 {
				t.Fatal("no stream records")
			}
			for _, n := range sizes {
				if n > 1<<16 { // the largest max_record_size
					t.Fatalf("record of %d bytes", n)
				}
			}
		}
	})
}

// SetShapingLimits before a shaper is installed bounds it once installed;
// an invalid value changes nothing; concurrent calls are safe.
func TestSetShapingLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s, rc := shapedPair(t, nil, false)
		if err := c.SetShapingLimits(shaping.Limits{Wire: &shaping.Account{Ceiling: 0, PerMB: shaping.MaxAccountRate + 1}}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("ppm above 10^6: %v", err)
		}
		if c.ShapingLimits() != (shaping.Limits{}) {
			t.Fatal("an invalid value changed the limits")
		}
		want := shaping.Limits{Wire: &shaping.Account{Ceiling: 4096, Initial: 4096}}
		if err := c.SetShapingLimits(want); err != nil {
			t.Fatal(err)
		}
		if got := c.ShapingLimits(); *got.Wire != *want.Wire {
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
		mustSetShaper(t, c, planShaper(func(p *shaping.RecordPlan) { p.Pad = 1000 }), nil)
		rc.start()
		transfer(t, c, s, 100<<10)
		wg.Wait()
		if st := c.ShapingMeters(); st.ConsumedBytes == 0 {
			t.Fatalf("stats %+v: padding is metered", st)
		}
	})
}

func TestRecordCaptureStartsInsideRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer func() { _ = a.Close(); _ = b.Close() }()
		go func() { _, _ = io.Copy(io.Discard, b) }()
		rc := &recConn{Conn: a}
		write := func(data ...byte) {
			t.Helper()
			if _, err := rc.Write(data); err != nil {
				t.Fatal(err)
			}
		}
		// Pause after the Size and first frame byte of a prior record.
		write(3, 0x30)
		rc.start()
		write(0xff, 0xff)
		// The next record is complete. The last is still being written.
		write(4, 0x08, 0, 7, 8, 5, 0)
		sizes, _ := rc.records(false)
		if len(sizes) != 1 || sizes[0] != 4 {
			t.Fatalf("record sizes %v, want [4]", sizes)
		}
	})
}
