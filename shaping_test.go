package zmux

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
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
	event func(*shaping.Event, *shaping.Reply)
}

func (f *fnShaper) Capabilities() shaping.Caps { return f.caps }
func (f *fnShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	if f.plan != nil {
		f.plan(v, p)
	}
}
func (f *fnShaper) OnEvent(e *shaping.Event, r *shaping.Reply) {
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

// contract is the language-independent data of testdata/shaping.
type contract struct {
	Records []struct {
		Name   string
		Side   string // the shaped sender: "client" (default) or "server"
		Limits struct {
			MaxPaddingBytes uint64 `json:"max_padding_bytes"`
		}
		Plan struct {
			MaxLen      int `json:"max_len"`
			PadTo       int `json:"pad_to"`
			PadMultiple int `json:"pad_multiple"`
			Pad         int
		}
		Send   int
		Repeat int
		Expect struct {
			MinLen          int `json:"min_len"`
			MaxLen          int `json:"max_len"`
			Mod, Rem        int
			MinRecords      int    `json:"min_records"`
			MaxPaddingBytes uint64 `json:"max_padding_bytes"`
			Clamped         bool
			Padding         bool
			MinPad          int `json:"min_pad"` // trailing PADDING bytes of every record
		}
	}
}

func TestShapingContractRecords(t *testing.T) {
	b, err := os.ReadFile("testdata/shaping/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var k contract
	if err := json.Unmarshal(b, &k); err != nil {
		t.Fatal(err)
	}
	for _, v := range k.Records {
		t.Run(v.Name, func(t *testing.T) {
			cfg := &Config{ShapingLimits: shaping.Limits{MaxPaddingBytes: v.Limits.MaxPaddingBytes}}
			var c, s *Conn
			var rc *recConn
			if v.Side == "server" {
				s, c, rc = shapedServerPair(t, cfg)
			} else {
				c, s, rc = shapedPair(t, cfg)
			}
			c.SetShaper(planShaper(func(p *shaping.RecordPlan) {
				p.MaxLen, p.PadTo, p.PadMultiple, p.Pad = v.Plan.MaxLen, v.Plan.PadTo, v.Plan.PadMultiple, v.Plan.Pad
			}))
			rc.start()
			for range max(v.Repeat, 1) {
				transfer(t, c, s, v.Send)
			}
			sizes, zeros := rc.records()
			e := v.Expect
			if len(sizes) == 0 || len(sizes) < e.MinRecords {
				t.Fatalf("%d records", len(sizes))
			}
			for i, n := range sizes {
				if n < e.MinLen || e.MaxLen > 0 && n > e.MaxLen || e.Mod > 0 && n%e.Mod != e.Rem || zeros[i] < e.MinPad {
					t.Fatalf("record of %d bytes, %d trailing zero bytes", n, zeros[i])
				}
			}
			st := c.ShapingStats()
			if e.MaxPaddingBytes > 0 && st.PaddingBytes > e.MaxPaddingBytes || e.Clamped && st.Clamped == 0 ||
				e.Padding && st.PaddingBytes == 0 || st.PaddingBytes < uint64(len(sizes)*e.MinPad) {
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
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Reply) {
			if e.Kind == shaping.EventStart || e.Kind == shaping.EventInjected {
				r.Inject = &shaping.Inject{After: time.Millisecond, Len: 20}
			}
		}}
	})
	time.Sleep(500 * time.Millisecond)
	c.SetShaper(nil)
	time.Sleep(50 * time.Millisecond)
	st := c.ShapingStats()
	sizes, zeros := rc.records()
	pad := 0
	for i := range sizes {
		if sizes[i] == 20 && zeros[i] == 20 {
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
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Reply) {
			if e.Kind == shaping.EventStart {
				r.Gate = &shaping.Gate{Rate: 100000, Burst: 10000}
			}
		}}
	})
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
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Reply) {
			if e.Kind == shaping.EventStart {
				r.Gate = &shaping.Gate{Rate: 400000, Burst: 1000}
			}
		}}
	})
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
	}))
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
		return &fnShaper{event: func(e *shaping.Event, r *shaping.Reply) {
			if e.Kind == shaping.EventStart {
				r.Timer[1] = &shaping.Timer{After: 10 * time.Millisecond}
			}
			if e.Kind == shaping.EventTimer && e.Timer != 1 {
				t.Errorf("slot %d", e.Timer)
			}
			got <- e.Kind
		}}
	})
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
	c.SetShaper(planShaper(func(*shaping.RecordPlan) { panic("x") }))
	transfer(t, c, s, 1000)
	if st := c.ShapingStats(); st.Faults != 1 {
		t.Fatalf("stats %+v", st)
	}
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
			}))
			rc.start()
			for _, n := range []int{1, 60, 61, 62, 63, 195, 196, 197, 990, 995, 3000, 40000} {
				transfer(t, c, s, n)
			}
			sizes, _ := rc.records()
			if len(sizes) == 0 {
				t.Fatal("no records")
			}
			for _, n := range sizes {
				switch {
				case p.padTo > 0 && n != p.padTo:
					t.Fatalf("record of %d bytes, want %d (sizes %v)", n, p.padTo, sizes)
				case p.mult > 0 && n%p.mult != p.pad:
					t.Fatalf("record of %d bytes, want %d mod %d (sizes %v)", n, p.pad, p.mult, sizes)
				}
			}
		})
	}
}

// A plan that sets only Pad appends that many PADDING bytes to every
// record, data records included.
func TestShapingPadOnly(t *testing.T) {
	c, s, rc := shapedPair(t, nil)
	c.SetShaper(planShaper(func(p *shaping.RecordPlan) { p.Pad = 7 }))
	rc.start()
	for _, n := range []int{1, 63, 64, 100, 5000} {
		transfer(t, c, s, n)
	}
	sizes, zeros := rc.records()
	if len(sizes) == 0 {
		t.Fatal("no records")
	}
	for i := range sizes {
		if zeros[i] < 7 {
			t.Fatalf("record %d of %d bytes ends with %d PADDING bytes, want 7", i, sizes[i], zeros[i])
		}
	}
}

// Shaping deadlines are met without the host timer's lateness: PADDING
// records injected every 10 ms are about 10 ms apart, not a timer tick
// more.
func TestShapingTimerPrecision(t *testing.T) {
	c, _, _ := shapedPair(t, nil)
	const every = 10 * time.Millisecond
	var mu sync.Mutex
	var at []time.Time
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{caps: shaping.CapPlan, event: func(e *shaping.Event, r *shaping.Reply) {
			if e.Kind == shaping.EventInjected {
				mu.Lock()
				at = append(at, time.Now())
				mu.Unlock()
			}
			if e.Kind == shaping.EventStart || e.Kind == shaping.EventInjected {
				r.Inject = &shaping.Inject{Len: 50, After: every}
			}
		}}
	})
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
	if med := gaps[len(gaps)/2]; med < every || med > every+every/20 {
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
		c, s, rc := shapedPair(t, &Config{ShapingLimits: shaping.Limits{MaxPaddingBytes: 1 << 20}})
		c.SetShaper(planShaper(f))
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
