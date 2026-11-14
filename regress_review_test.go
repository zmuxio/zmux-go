package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/shaping"
)

// A JoinEarlyHello above what a Hello's ciphertext holds does not fail the join.
func TestJoinEarlyHelloBeyondCiphertext(t *testing.T) {
	e := newAtkEnv(t, atkOpts{client: func(c *Config) { c.JoinEarlyHello = 70000 }})
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.suspend()
	if _, err := st.Write(make([]byte, 128<<10)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	if _, err := e.c.AddPipe(ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// gatedWrite blocks the nth write and CloseWrite until the gate opens and records
// its closing.
type gatedWrite struct {
	net.Conn
	gate    chan struct{}
	blocked chan struct{}
	once    sync.Once
	nth     int32
	writes  atomic.Int32
	closed  atomic.Bool
}

func (g *gatedWrite) Write(b []byte) (int, error) {
	if g.writes.Add(1) == g.nth {
		g.once.Do(func() { close(g.blocked) })
		<-g.gate
	}
	return g.Conn.Write(b)
}

func (g *gatedWrite) Close() error {
	g.closed.Store(true)
	return g.Conn.Close()
}

func (g *gatedWrite) CloseWrite() error {
	g.once.Do(func() { close(g.blocked) })
	<-g.gate
	return nil
}

// Ending the half a direction does not use holds no session lock.
func TestUnusedHalfOutsideLock(t *testing.T) {
	e := newAtkEnv(t, atkOpts{})
	g := &gatedWrite{Conn: e.dial(e.addr), gate: make(chan struct{}), blocked: make(chan struct{}), nth: -1}
	done := make(chan error, 1)
	go func() {
		_, err := e.c.AddPipe(e.ctx, PipeIO{R: g, W: g}, PipeOptions{Direction: DirectionReceiveOnly})
		done <- err
	}()
	<-g.blocked
	st := make(chan struct{})
	go func() { _ = e.c.ConnectionStats(); close(st) }()
	select {
	case <-st:
	case <-time.After(5 * time.Second):
		close(g.gate)
		t.Fatal("the session is blocked by the carrier's CloseWrite")
	}
	close(g.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Closing the session or the listener closes an Unconfirmed carrier at once.
func TestCloseEndsUnconfirmed(t *testing.T) {
	for _, what := range []string{"session", "listener"} {
		t.Run(what, func(t *testing.T) {
			e := newAtkEnv(t, atkOpts{})
			h, _ := e.forge(20, join.Sealed, nil, nil)
			nc, ok := e.offer(h)
			if !ok {
				t.Fatal("no Reply")
			}
			if what == "session" {
				_ = e.s.Close()
			} else {
				_ = e.lst.Close()
			}
			_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.Copy(io.Discard, nc); err != nil {
				t.Fatalf("carrier still open: %v", err)
			}
		})
	}
}

// The end of the session or of the attachment while the Confirm is written starts
// nothing and closes the carrier.
func TestConfirmWriteRacesEnd(t *testing.T) {
	for _, what := range []string{"close", "abort"} {
		t.Run(what, func(t *testing.T) {
			e := newAtkEnv(t, atkOpts{})
			// Writes: the Hello, then the Confirm.
			g := &gatedWrite{Conn: e.dial(e.addr), gate: make(chan struct{}), blocked: make(chan struct{}), nth: 2}
			before := len(e.c.Pipes())
			done := make(chan error, 1)
			go func() {
				_, err := e.c.AddPipe(e.ctx, PipeIO{R: g, W: g}, PipeOptions{})
				done <- err
			}()
			<-g.blocked
			if what == "close" {
				_ = e.c.Close()
			} else {
				ps := e.c.Pipes()
				if len(ps) != before+1 {
					t.Fatalf("%d pipes", len(ps))
				}
				if err := ps[len(ps)-1].Abort(errors.New("test")); err != nil {
					t.Fatal(err)
				}
			}
			close(g.gate)
			if err := <-done; err == nil {
				t.Fatal("AddPipe succeeded after the end")
			}
			if !g.closed.Load() {
				t.Fatal("carrier left open")
			}
			if what == "close" {
				_ = e.c.Wait()
			}
		})
	}
}

// halves of a type that cannot be compared.
type sliceHalf struct {
	net.Conn
	_ []byte
}

// A Bound Hello on a carrier of such halves is refused, without a panic.
func TestUncomparableHalves(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	p := PipeIO{R: sliceHalf{Conn: a}, W: sliceHalf{Conn: a}}
	if _, err := joinBinding(p); err == nil {
		t.Fatal("Bound without a declaration")
	}
	closePipeIO(p)
	q := PipeIO{R: secureSlice{sliceHalf{Conn: b}}, W: secureSlice{sliceHalf{Conn: b}}}
	if _, err := joinBinding(q); err == nil {
		t.Fatal("Bound without an exporter")
	}
}

type secureSlice struct{ sliceHalf }

func (secureSlice) CarrierSecure() bool { return true }

// A pair keeps what describes its write half.
func TestPairedPipeKeepsWriteOverhead(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	p := PairedPipe(PipeIO{W: a, WriteOverhead: 5, FateGroup: 3, Variant: "v"}, PipeIO{R: b, WriteOverhead: 9})
	if p.WriteOverhead != 5 || p.FateGroup != 3 || p.Variant != "v" {
		t.Fatalf("%+v", p)
	}
}

// A pairing on the listener keeps the same of the half this end writes on and
// of its uplink.
func TestHandlePairKeepsCarrierFields(t *testing.T) {
	var got PipeIO
	seen := make(chan struct{})
	l := mustNewListener(&ListenerConfig{Config: atkConfig(), WrapPipe: WrapPipeFunc(func(_ context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
		got = p
		close(seen)
		return p, errors.New("enough")
	})})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, b := net.Pipe()
	c, d := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close(); _ = c.Close(); _ = d.Close() }()
	pair := "AAAAAAAAAAAAAAAAAAAAAA"
	go func() {
		_ = l.HandlePipe(ctx, PipeIO{R: a, FateGroup: 3, Variant: "v", WriteOverhead: 9}, PipeHint{Pair: pair, Uplink: true})
	}()
	go func() { _ = l.HandlePipe(ctx, PipeIO{W: c, WriteOverhead: 5}, PipeHint{Pair: pair}) }()
	<-seen
	if got.WriteOverhead != 5 || got.FateGroup != 3 || got.Variant != "v" {
		t.Fatalf("%+v", got)
	}
}

// A session's end during the wait for a Reply's plan is no authentication failure.
func TestJoinPlanCancelIsNoReject(t *testing.T) {
	var rejects atomic.Int32
	gate := make(chan struct{})
	e := newAtkEnv(t, atkOpts{
		listener: func(l *ListenerConfig) {
			l.OnReject = func(RejectInfo) RejectAction { rejects.Add(1); return 0 }
		},
		server: func(c *Config) {
			c.ShapingLimits.MaxDelay = time.Hour
			c.HandshakeShaper = func(_ *shaping.HandshakeView, p *shaping.UnitPlan) bool {
				select {
				case <-gate:
				default:
					close(gate)
				}
				p.Delay = time.Hour
				return true
			}
		},
	})
	h, _ := e.forge(20, join.Sealed, nil, nil)
	nc := e.dial(e.addr)
	if _, err := nc.Write(h); err != nil {
		t.Fatal(err)
	}
	<-gate
	time.Sleep(20 * time.Millisecond)
	_ = e.s.Close()
	_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.Copy(io.Discard, nc); err != nil {
		t.Fatalf("carrier still open: %v", err)
	}
	if n := rejects.Load(); n != 0 {
		t.Fatalf("OnReject called %d times", n)
	}
}

// The bytes classification read back do not hide a half's exporter.
func TestPrefixKeepsExporter(t *testing.T) {
	a, b := kmPair("x")
	defer func() { _ = a.Close(); _ = b.Close() }()
	if _, ok := exporterOf(&prefixReader{pre: []byte{1}, r: a}); !ok {
		t.Fatal("no exporter behind a prefix")
	}
}

// A zero plan is a plan; only a hook with none for the message counts as unplanned.
func TestHookZeroPlanIsPlanned(t *testing.T) {
	for _, c := range []struct {
		planned bool
		want    uint64
	}{{true, 0}, {false, 1}} {
		e := newAtkEnv(t, atkOpts{client: func(cfg *Config) {
			cfg.HandshakeShaper = func(v *shaping.HandshakeView, _ *shaping.UnitPlan) bool {
				return c.planned || v.Message != "hello"
			}
		}})
		if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
			t.Fatal(err)
		}
		if got := e.c.ShapingStats().HandshakeUnplanned; got != c.want {
			t.Fatalf("planned %v: unplanned %d", c.planned, got)
		}
	}
}
