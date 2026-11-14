package seal_test

import (
	"bytes"
	"context"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
	"github.com/zmuxio/zmux-go/shaping"
)

type writesConn struct {
	net.Conn
	n  []int
	mu sync.Mutex
}

func (c *writesConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.n = append(c.n, len(b))
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *writesConn) writes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.n...)
}

func byName(want map[string]int) shaping.HandshakeShaper {
	return func(v *shaping.HandshakeView, p *shaping.UnitPlan) bool {
		p.PadTo = want[v.Handshake+"/"+v.Message]
		return true
	}
}

// The three messages of an Establish reach their lengths on the carrier, under
// the name of the profile's handshake.
func TestEstablishWireLengths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sh := byName(map[string]int{
		proto.HandshakeEstablish + "/hello": 1500, proto.HandshakeEstablish + "/reply": 1700, proto.HandshakeEstablish + "/finish": 800,
	})
	for _, tc := range []struct {
		profile             proto.Profile
		hello, reply, final int
	}{
		{proto.ProfileEstablishPSK, 1500, 1700, 800},
		{proto.ProfileEstablishPSKAES, 1500, 1700, 800},
	} {
		f := newFixture(t)
		f.client.AllowedProfiles = []proto.Profile{tc.profile}
		f.client.Shaper, f.server.Shaper = sh, sh
		a, b := net.Pipe()
		ca, cb := &writesConn{Conn: a}, &writesConn{Conn: b}
		cp, sp, cerr, serr := establishPair(t, ctx, &f.client, &f.server, zmux.NetPipe(ca), zmux.NetPipe(cb))
		if cerr != nil || serr != nil {
			t.Fatal(tc.profile, cerr, serr)
		}
		if cp.HandshakePadding == 0 || sp.HandshakePadding == 0 {
			t.Fatalf("%v: padding reported: client %d, server %d", tc.profile, cp.HandshakePadding, sp.HandshakePadding)
		}
		_ = cp.W.Close()
		_ = sp.W.Close()
		cw, sw := ca.writes(), cb.writes()
		if len(cw) != 2 || cw[0] != tc.hello || cw[1] != tc.final || len(sw) != 1 || sw[0] != tc.reply {
			t.Fatalf("%v: client writes %v, server writes %v", tc.profile, cw, sw)
		}
	}
}

// An Establish is asked under the name "establish" only: a plan for another
// name is never applied to it.
func TestEstablishHandshakeName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	var names []string
	sh := func(v *shaping.HandshakeView, p *shaping.UnitPlan) bool {
		mu.Lock()
		names = append(names, v.Handshake+"/"+v.Message)
		mu.Unlock()
		if v.Handshake != "future_handshake" {
			return false
		}
		p.PadTo = 5000
		return true
	}
	f := newFixture(t)
	f.client.Shaper, f.server.Shaper = sh, sh
	a, b := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &f.client, &f.server, zmux.NetPipe(a), zmux.NetPipe(b))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	if cp.HandshakePadding != 0 || sp.HandshakePadding != 0 {
		t.Fatalf("padded: client %d, server %d", cp.HandshakePadding, sp.HandshakePadding)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, n := range names {
		if !strings.HasPrefix(n, "establish/") {
			t.Fatalf("asked %v", names)
		}
	}
	if len(names) != 3 {
		t.Fatalf("asked %v", names)
	}
}

// A failing hook is no plan, and limits apply to a plan.
func TestEstablishHookPanicAndLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run := func(mod func(*fixture)) [2]int {
		f := newFixture(t)
		mod(f)
		a, b := net.Pipe()
		ca := &writesConn{Conn: a}
		cp, sp, cerr, serr := establishPair(t, ctx, &f.client, &f.server, zmux.NetPipe(ca), zmux.NetPipe(b))
		if cerr != nil || serr != nil {
			t.Fatal(cerr, serr)
		}
		_ = cp.W.Close()
		_ = sp.W.Close()
		w := ca.writes()
		return [2]int{w[0], w[1]}
	}
	base := run(func(*fixture) {})
	if got := run(func(f *fixture) {
		f.client.Shaper = func(*shaping.HandshakeView, *shaping.UnitPlan) bool { panic("x") }
	}); got != base {
		t.Fatalf("panic: %v, want %v", got, base)
	}
	if got := run(func(f *fixture) {
		f.client.Shaper = byName(map[string]int{proto.HandshakeEstablish + "/hello": 900})
		f.client.ShapingLimits.NoPadding = true
	}); got != base {
		t.Fatalf("no padding: %v, want %v", got, base)
	}
}

// Hello, Reply and Confirm of a Join reach their lengths on the carrier; the
// Reply stays within 3 times what it received where that applies.
func TestJoinWireLengths(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         zmux.RecordMode
		within       bool
		reply, wantR int
	}{
		{"sealed", zmux.RecordSealed, false, 1500, 1500},
		{"trusted", zmux.RecordTrusted, false, 1500, 1500},
		{"bound", zmux.RecordBound, false, 1500, 1500},
		{"sealed-limit", zmux.RecordSealed, true, 60000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			f := newFixture(t)
			f.server.AllowRecordMode = anyMode
			exp := bytes.Repeat([]byte{7}, 32)
			hello := 1300
			if tc.within {
				hello = 0
			}
			sh := byName(map[string]int{"join/hello": hello, "join/hello_early": hello, "join/reply": tc.reply, "join/confirm": 800})
			var mu sync.Mutex
			var accepted []*writesConn
			wrapConn := func(nc net.Conn) zmux.PipeIO {
				if tc.mode == zmux.RecordBound {
					return zmux.NetPipe(&secureConn{Conn: nc, exp: exp})
				}
				return zmux.NetPipe(nc)
			}
			wrap := func(ctx context.Context, p zmux.PipeIO, h zmux.PipeHint) (zmux.PipeIO, error) {
				wc := &writesConn{Conn: p.W.(net.Conn)}
				mu.Lock()
				accepted = append(accepted, wc)
				mu.Unlock()
				return f.server.WrapPipe(ctx, wrapConn(wc), h)
			}
			l, addr := listen(t, ctx, &zmux.ListenerConfig{
				Config:   &zmux.Config{MaxPipes: 4, AllowRecordMode: anyMode, HandshakeShaper: sh},
				WrapPipe: zmux.WrapPipeFunc(wrap),
			})
			cc := f.client
			cc.RecordMode = tc.mode
			p, err := seal.Establish(ctx, wrapConn(dial(t, addr)), &cc)
			if err != nil {
				t.Fatal(err)
			}
			c, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{MaxPipes: 4, HandshakeShaper: sh,
				RecordPolicy: func(zmux.RecordPolicyInput) zmux.RecordMode { return tc.mode }})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			s, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			echo(t, ctx, c, s)
			jc := &writesConn{Conn: dial(t, addr)}
			if _, err := c.AddPipe(ctx, wrapConn(jc), zmux.PipeOptions{}); err != nil {
				t.Fatalf("join: %v", err)
			}
			echo(t, ctx, c, s)
			cw := jc.writes()
			mu.Lock()
			sw := accepted[len(accepted)-1].writes()
			mu.Unlock()
			if len(cw) < 1 || len(sw) < 1 {
				t.Fatalf("writes %v %v", cw, sw)
			}
			if tc.within {
				if sw[0] > 3*cw[0] || sw[0] < 3*cw[0]-3 {
					t.Fatalf("hello %d, reply %d", cw[0], sw[0])
				}
				return
			}
			if cw[0] != hello || sw[0] != tc.wantR {
				t.Fatalf("client %v, server %v", cw, sw)
			}
			if tc.mode != zmux.RecordBound && (len(cw) < 2 || cw[1] != 800) {
				t.Fatalf("confirm: client %v", cw)
			}
			if st := c.ShapingStats(); st.WireSpent == 0 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

// Shaping counts application data in the length of each message: with one
// plan, data of 0, 1, 100 and 256 bytes gives the same lengths on the carrier.
func TestEstablishShapingHidesAppData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sh := byName(map[string]int{
		proto.HandshakeEstablish + "/hello": 1536, proto.HandshakeEstablish + "/reply": 1536, proto.HandshakeEstablish + "/finish": 512,
	})
	var first []int
	for _, n := range []int{0, 1, 100, 256} {
		f := newFixture(t)
		f.client.Shaper, f.server.Shaper = sh, sh
		data := func(context.Context, seal.AppDataRequest) ([]byte, error) { return bytes.Repeat([]byte{1}, n), nil }
		f.client.AppData, f.server.AppData = data, data
		a, b := net.Pipe()
		ca, cb := &writesConn{Conn: a}, &writesConn{Conn: b}
		cp, sp, cerr, serr := establishPair(t, ctx, &f.client, &f.server, zmux.NetPipe(ca), zmux.NetPipe(cb))
		if cerr != nil || serr != nil {
			t.Fatal(n, cerr, serr)
		}
		_ = cp.W.Close()
		_ = sp.W.Close()
		got := append(ca.writes(), cb.writes()...)
		t.Logf("app data %d: writes %v", n, got)
		if first == nil {
			first = got
		} else if !slices.Equal(got, first) {
			t.Fatalf("app data %d: writes %v, with none %v", n, got, first)
		}
	}
}
