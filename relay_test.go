package zmux_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping/machine"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

// Tests of carrier loss through relays that behave as intermediaries on
// a path: idle limits, black holes, refused connections, connection age
// limits. Each runs in parallel with the others and in a few seconds.

func relayConfig(resume time.Duration) *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)},
		ResumeTimeout:   resume,
		SchedulerParams: zmux.SchedulerParams{Probation: new(time.Duration)}}
}

// relayEnv is a TCP listener of a server, a relay in front of it and a
// client whose bootstrap carrier goes direct or through the relay.
type relayEnv struct {
	ln     net.Listener
	relay  *zmuxtest.Relay
	client *zmux.Conn
	server *zmux.Conn
}

func newRelayEnv(t *testing.T, ccfg, scfg *zmux.Config, ro zmuxtest.RelayOptions, viaRelay bool) *relayEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = lst.HandlePipe(ctx, zmux.NetPipe(c), zmux.JoinHint{})
			}()
		}
	}()
	rl, err := zmuxtest.NewRelay(ln.Addr().String(), ro)
	if err != nil {
		t.Fatal(err)
	}
	e := &relayEnv{ln: ln, relay: rl}
	addr := ln.Addr().String()
	if viaRelay {
		addr = rl.Addr()
	}
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if e.client, err = zmux.Client(dctx, nc, ccfg); err != nil {
		t.Fatal(err)
	}
	if e.server, err = lst.Accept(dctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.client.Close()
		_ = e.server.Close()
		_ = rl.Close()
		_ = ln.Close()
		_ = lst.Close()
		cancel()
		wg.Wait()
	})
	return e
}

// dialer dials the relay and records when each dial started.
type dialer struct {
	addr string
	mu   sync.Mutex
	at   []time.Time
}

func (d *dialer) dial(ctx context.Context) (zmux.PipeIO, error) {
	d.mu.Lock()
	d.at = append(d.at, time.Now())
	d.mu.Unlock()
	var nd net.Dialer
	c, err := nd.DialContext(ctx, "tcp", d.addr)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	return zmux.NetPipe(c), nil
}

func (d *dialer) times() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Time(nil), d.at...)
}

// maintain runs a pool on c until the test ends; the channel gets its
// result once.
func maintain(t *testing.T, c *zmux.Conn, pool zmux.PipePool) <-chan error {
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		res <- c.MaintainPipes(ctx, pool)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("pool did not stop")
		}
	})
	return res
}

func poll(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, d)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// activePool returns the IDs of the client's active pipes other than 0.
func activePool(c *zmux.Conn) []uint64 {
	var ids []uint64
	for _, st := range c.PipeStats() {
		if st.Pipe != 0 && st.Outbound == zmux.OutboundActive {
			ids = append(ids, st.Pipe)
		}
	}
	return ids
}

// An idle limit of 2.5 s on the path (scaled from a NAT's or a load
// balancer's): probes at k = 1 s with j = 0.3 keep the pipe through it; at
// k = 4 s the relay ends the connection, and the pool redials at once.
func TestRelayIdleLimit(t *testing.T) {
	t.Parallel()
	t.Run("probes keep the pipe", func(t *testing.T) {
		t.Parallel()
		e := newRelayEnv(t, relayConfig(0), relayConfig(0), zmuxtest.RelayOptions{IdleCut: 2500 * time.Millisecond}, false)
		d := &dialer{addr: e.relay.Addr()}
		maintain(t, e.client, zmux.PipePool{Dial: d.dial,
			Options: zmux.PipeOptions{ProbeInterval: time.Second, ProbeJitter: 0.3}})
		poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
		id := activePool(e.client)[0]
		time.Sleep(7 * time.Second)
		if n := e.relay.Cuts(); n != 0 {
			t.Fatalf("the relay ended %d idle connections", n)
		}
		if ids := activePool(e.client); len(ids) != 1 || ids[0] != id || len(d.times()) != 1 {
			t.Fatalf("pool pipes %v (was %d), %d dials", ids, id, len(d.times()))
		}
	})
	t.Run("too slow probes", func(t *testing.T) {
		t.Parallel()
		// The pool pipe also carries the redundant acknowledgement copies
		// of pipe 0's traffic (MULTIPATH.md 7.5), and pipe 0's first probe
		// on each end is drawn from [0, 15 s] with the default jitter: two
		// such copies, each up to 2.5 s after the previous byte, put the
		// cut anywhere up to 3 x 2.5 s after the join. A fixed schedule
		// (j = 0) puts pipe 0's probes and keep-alives at 15 s, so the
		// pool pipe is silent after its join until its own probe at 4 s.
		ccfg, scfg := relayConfig(0), relayConfig(0)
		ccfg.SchedulerParams.NoProbeJitter, scfg.SchedulerParams.NoProbeJitter = true, true
		e := newRelayEnv(t, ccfg, scfg, zmuxtest.RelayOptions{IdleCut: 2500 * time.Millisecond}, false)
		d := &dialer{addr: e.relay.Addr()}
		maintain(t, e.client, zmux.PipePool{Dial: d.dial, Options: zmux.PipeOptions{ProbeInterval: 4 * time.Second}})
		poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
		// The cut falls due IdleCut (2.5 s) after the join's last byte,
		// plus the relay's 10 ms check, well before the probe at 4 s; the
		// window is the probe interval plus one IdleCut, the latest a cut
		// can come if the probe went at all (and kept the pipe for one
		// more IdleCut), which this test would then report as a failure
		// of the window rather than a pass.
		poll(t, 4*time.Second+2500*time.Millisecond, "idle cut", func() bool { return e.relay.Cuts() > 0 })
		cut := time.Now()
		poll(t, 2*time.Second, "redial", func() bool { return len(d.times()) >= 2 })
		// The pool acts on the failure, not at its 250 ms periodic pass;
		// its previous dial is older than the 1 s backoff.
		if lag := d.times()[1].Sub(cut); lag > 200*time.Millisecond {
			t.Fatalf("redial %v after the cut", lag)
		}
		poll(t, 3*time.Second, "replacement", func() bool { return len(activePool(e.client)) == 1 })
	})
}

// A black hole that drops everything without ending the connection: the
// idle pipe fails within max(k, the server's T_fail) + T_fail (below) and
// the pool replaces it.
func TestRelayBlackHole(t *testing.T) {
	t.Parallel()
	e := newRelayEnv(t, relayConfig(0), relayConfig(0), zmuxtest.RelayOptions{}, false)
	d := &dialer{addr: e.relay.Addr()}
	maintain(t, e.client, zmux.PipePool{Dial: d.dial, Options: zmux.PipeOptions{ProbeInterval: time.Second}})
	poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
	id := activePool(e.client)[0]
	p, _ := e.client.Pipe(id)
	// The probe goes at most k after the pipe's last ack-eliciting byte
	// and the pipe fails T_fail_p = max(2 s, 8 x PTO_p) after t_ref
	// (another pipe is active; queue_p is 0 on an idle pipe; MULTIPATH.md
	// 7.1). t_ref also moves with an advance of the acknowledged position:
	// bytes the relay passed just before the black hole (a probe) are
	// acknowledged in a quiet session on their own pipe pair only (6.5),
	// so their acknowledgement reaches the client over pipe 0 once the
	// server's outbound direction of the pipe failed, T_fail_p of the
	// server after its last byte, and the client's T_fail_p counts from
	// there. No RTT sample arrives in the black hole, so each end's PTO_p
	// stays the one taken here; under load (-race, a busy host) RTTVar
	// can lift T_fail_p above 2 s. The margin of 1 s covers the lateness
	// of the timers and of this poll on a loaded host.
	tFail := func(c *zmux.Conn) time.Duration {
		q, ok := c.Pipe(id)
		if !ok {
			t.Fatalf("pipe %d gone", id)
		}
		st, _ := q.Stats()
		pto := st.SmoothedRTT + max(4*st.RTTVar, time.Millisecond) + 25*time.Millisecond // default zmux_max_ack_delay
		return max(2*time.Second, 8*pto)
	}
	k := time.Second
	bound := max(k, tFail(e.server)) + tFail(e.client) + time.Second
	e.relay.Blackhole(true, true)
	start := time.Now()
	poll(t, bound+time.Second, "failure", func() bool {
		st, ok := p.Stats()
		return !ok || st.Outbound == zmux.OutboundFailed || st.Outbound == zmux.OutboundClosed
	})
	if el := time.Since(start); el > bound {
		t.Fatalf("failed after %v, bound %v", el, bound)
	}
	poll(t, 3*time.Second, "replacement", func() bool {
		ids := activePool(e.client)
		return len(ids) == 1 && ids[0] != id
	})
}

// echoAll copies every stream of s back.
func echoAll(ctx context.Context, s *zmux.Conn) {
	for {
		st, err := s.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}()
	}
}

// events collects the client's events.
type events struct {
	mu  sync.Mutex
	got []zmux.Event
	at  []time.Time
}

func watchEvents(t *testing.T, c *zmux.Conn) *events {
	ev := &events{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			e, err := c.NextEvent(ctx)
			if err != nil {
				return
			}
			ev.mu.Lock()
			ev.got = append(ev.got, e)
			ev.at = append(ev.at, time.Now())
			ev.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return ev
}

func (ev *events) first(k zmux.EventKind) (time.Time, bool) {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	for i, e := range ev.got {
		if e.Kind == k {
			return ev.at[i], true
		}
	}
	return time.Time{}, false
}

// The relay cuts every pipe while a transfer runs, the bootstrap pipe
// included; with resume the pool redials by its policy (dial_after with
// dial_jitter) as soon as the session suspends, the session resumes, and
// the echoed bytes arrive whole, without loss or duplication.
func TestRelayLastPipeCutResumes(t *testing.T) {
	t.Parallel()
	e := newRelayEnv(t, relayConfig(120*time.Second), relayConfig(120*time.Second), zmuxtest.RelayOptions{}, true)
	pol, err := machine.PoolPolicy([]byte(`{"dial_after":[{"const":0.3},{"const":0.6}],"dial_jitter":0.3}`), machine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ev := watchEvents(t, e.client)
	d := &dialer{addr: e.relay.Addr()}
	maintain(t, e.client, zmux.PipePool{Dial: d.dial, Policy: pol})
	poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go echoAll(ctx, e.server)
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 8<<20)
	_, _ = rand.Read(msg)
	var got bytes.Buffer
	readDone := make(chan error, 1)
	var read atomic.Int64
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := st.Read(buf)
			got.Write(buf[:n])
			read.Add(int64(n))
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				readDone <- err
				return
			}
		}
	}()
	go func() {
		for off := 0; off < len(msg); off += 32 << 10 {
			if _, err := st.Write(msg[off : off+32<<10]); err != nil {
				return
			}
			if off == len(msg)/4 {
				time.Sleep(200 * time.Millisecond)
			}
		}
		_ = st.Close()
	}()
	poll(t, 10*time.Second, "first quarter echoed", func() bool { return read.Load() >= int64(len(msg)/8) })
	dials := len(d.times())
	e.relay.CutAll()
	cut := time.Now()
	if read.Load() >= int64(len(msg)) {
		t.Fatal("the transfer ended before the cut")
	}
	poll(t, 5*time.Second, "suspension", func() bool { _, ok := ev.first(zmux.EventSessionSuspended); return ok })
	poll(t, 5*time.Second, "redial", func() bool { return len(d.times()) > dials })
	if lag := d.times()[dials].Sub(cut); lag > 300*time.Millisecond {
		t.Fatalf("pool redialled %v after the cut", lag)
	}
	poll(t, 10*time.Second, "resume", func() bool { _, ok := ev.first(zmux.EventSessionResumed); return ok })
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("echo incomplete: %d of %d bytes", read.Load(), len(msg))
	}
	if !bytes.Equal(got.Bytes(), msg) {
		t.Fatalf("echoed %d bytes, want %d intact", got.Len(), len(msg))
	}
}

// The relay refuses every connection after cutting them all: the pool
// keeps dialling while the session is suspended, the grace period ends
// with ZMUX_RESUME_TIMEOUT, and the pool stops with the session's error.
func TestRelayRefusedUntilResumeTimeout(t *testing.T) {
	t.Parallel()
	e := newRelayEnv(t, relayConfig(2*time.Second), relayConfig(2*time.Second), zmuxtest.RelayOptions{}, true)
	d := &dialer{addr: e.relay.Addr()}
	done := maintain(t, e.client, zmux.PipePool{Dial: d.dial, MinBackoff: 200 * time.Millisecond})
	poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
	e.relay.Refuse(true)
	e.relay.CutAll()
	cut := time.Now()
	var err error
	select {
	case err = <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("pool still running past the grace period")
	}
	if !errors.Is(err, zmux.ErrResumeTimeout) {
		t.Fatalf("pool returned %v", err)
	}
	if el := time.Since(cut); el < 1800*time.Millisecond {
		t.Fatalf("session ended %v after the cut", el)
	}
	if !errors.Is(e.client.Wait(), zmux.ErrResumeTimeout) {
		t.Fatalf("session ended with %v", e.client.Wait())
	}
	if n := e.relay.Refused(); n < 3 {
		t.Fatalf("%d dials refused during the grace period", n)
	}
}

// httpRelayEnv serves zmux over HTTP behind a relay; its client dials the
// relay.
type httpRelayEnv struct {
	// ctx bounds the HTTP requests of the carriers: the session's life.
	ctx   context.Context
	relay *zmuxtest.Relay
	hc    *http.Client
	// relay2 and hc2: a second path to the server, for pipes that one
	// relay's faults must not reach.
	relay2 *zmuxtest.Relay
	hc2    *http.Client
	url    string
	client *zmux.Conn
	server *zmux.Conn
}

func newHTTPRelayEnv(t *testing.T, ccfg, scfg *zmux.Config, split bool) *httpRelayEnv {
	t.Helper()
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
	hh, err := zmux.NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hh)
	rl, err := zmuxtest.NewRelay(srv.Listener.Addr().String(), zmuxtest.RelayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", rl.Addr())
	}}
	rl2, err := zmuxtest.NewRelay(srv.Listener.Addr().String(), zmuxtest.RelayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tr2 := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", rl2.Addr())
	}}
	life, end := context.WithCancel(context.Background())
	e := &httpRelayEnv{ctx: life, relay: rl, hc: &http.Client{Transport: tr}, url: srv.URL,
		relay2: rl2, hc2: &http.Client{Transport: tr2}}
	ctx, cancel := context.WithTimeout(life, 5*time.Second)
	defer cancel()
	var p zmux.PipeIO
	if split {
		p, err = zmux.HTTPSplitPipe(life, e.hc, e.url, nil, nil)
	} else {
		p, err = zmux.HTTPPipe(life, e.hc, e.url, nil, zmux.DirBidirectional, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	if e.client, err = zmux.Start(ctx, p, zmux.RoleClient, ccfg); err != nil {
		t.Fatal(err)
	}
	if e.server, err = lst.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		end()
		_ = e.client.Close()
		_ = e.server.Close()
		_ = e.client.Wait()
		_ = e.server.Wait()
		_ = lst.Close()
		tr.CloseIdleConnections()
		tr2.CloseIdleConnections()
		_ = rl.Close()
		_ = rl2.Close()
		srv.Close()
	})
	return e
}

// Split carriers: the downlinks are silently cut (bytes toward the client
// dropped, connections kept). The split bootstrap pair and the pool's
// split member fail; the pool joins a new member, a downlink out of band
// and an uplink in band at the same time, and the session resumes. A
// downlink joined alone fails within the inbound silence limit.
func TestRelaySplitDownlinkSilenced(t *testing.T) {
	t.Parallel()
	t.Run("split pair", func(t *testing.T) {
		t.Parallel()
		cfg := func() *zmux.Config {
			c := relayConfig(30 * time.Second)
			c.SchedulerParams.ProbeInterval = time.Second
			return c
		}
		e := newHTTPRelayEnv(t, cfg(), cfg(), true)
		ev := watchEvents(t, e.client)
		var joins atomic.Int64
		maintain(t, e.client, zmux.PipePool{MinBackoff: 100 * time.Millisecond,
			Join: func(ctx context.Context, c *zmux.Conn, o zmux.PipeOptions) ([]*zmux.Pipe, error) {
				joins.Add(1)
				return zmux.HTTPSplitJoin(ctx, c, e.hc, e.url, nil, nil, o)
			}})
		member := func() (up, down bool) {
			for _, st := range e.client.PipeStats() {
				up = up || st.Pipe != 0 && st.Outbound == zmux.OutboundActive
				down = down || st.Pipe != 0 && st.Outbound == zmux.OutboundNone && st.Inbound == zmux.InboundOpen
			}
			return
		}
		poll(t, 5*time.Second, "pool member", func() bool { u, d := member(); return u && d })
		p0, _ := e.client.Pipe(0)
		e.relay.Blackhole(false, true)
		start := time.Now()
		poll(t, 6*time.Second, "split pair failure", func() bool {
			st, ok := p0.Stats()
			return !ok || st.Outbound == zmux.OutboundFailed || st.Outbound == zmux.OutboundClosed
		})
		// k = 1 s, T_dead = T_fail = 2 s with resume, and a margin.
		if el := time.Since(start); el > 4500*time.Millisecond {
			t.Fatalf("failed after %v", el)
		}
		poll(t, 25*time.Second, "new member", func() bool { return joins.Load() >= 2 })
		if _, ok := ev.first(zmux.EventSessionSuspended); ok {
			poll(t, 10*time.Second, "resume", func() bool { _, ok := ev.first(zmux.EventSessionResumed); return ok })
		}
		poll(t, 10*time.Second, "both directions", func() bool { u, d := member(); return u && d })
	})
	t.Run("split bootstrap refused as a join", func(t *testing.T) {
		t.Parallel()
		// The server takes the refused carrier's pair for a new session;
		// its handshake timeout ends that one soon.
		scfg := relayConfig(0)
		scfg.HandshakeTimeout = time.Second
		e := newHTTPRelayEnv(t, relayConfig(0), scfg, true)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p, err := zmux.HTTPSplitPipe(ctx, e.hc, e.url, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = p.W.Close(); _ = p.R.Close() }()
		if _, err := e.client.AddPipe(ctx, p, zmux.PipeOptions{}); !errors.Is(err, zmux.ErrInvalidConfig) {
			t.Fatalf("AddPipe of a split bootstrap: %v", err)
		}
	})
	t.Run("downlink alone", func(t *testing.T) {
		t.Parallel()
		e := newHTTPRelayEnv(t, relayConfig(0), relayConfig(0), false)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r, err := e.client.ReservePipe(zmux.DirReceiveOnly, zmux.PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// The downlink takes the second path, so that only it goes silent.
		dp, err := zmux.HTTPPipe(e.ctx, e.hc2, e.url, http.Header{zmux.HeaderJoin: {r.Header()}}, zmux.DirReceiveOnly, nil)
		if err != nil {
			t.Fatal(err)
		}
		p, err := r.Attach(ctx, dp)
		if err != nil {
			t.Fatal(err)
		}
		poll(t, 5*time.Second, "downlink open", func() bool {
			st, ok := p.Stats()
			return ok && st.Inbound == zmux.InboundOpen
		})
		e.relay2.Blackhole(false, true)
		start := time.Now()
		deadline := time.Now().Add(30 * time.Second)
		for {
			st, ok := p.Stats()
			if !ok || st.Inbound != zmux.InboundOpen {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("downlink open after 30 s: %+v; pipes %+v", st, e.client.PipeStats())
			}
			time.Sleep(5 * time.Millisecond)
		}
		// L_in = 15 s + n_in / 8 s + max(2 s, ...) = 17.25 s with two
		// inbound pipes; less the time since the last record.
		if el := time.Since(start); el > 19*time.Second {
			t.Fatalf("downlink failed after %v", el)
		}
	})
}

// A relay that ends every connection 3 s after it began, idle or not (a
// load balancer's limit on a connection's duration): a retire rule on age
// with jitter replaces each pipe before the limit, make before break,
// while a stream keeps flowing without a stall.
func TestRelayAgeLimitRotation(t *testing.T) {
	t.Parallel()
	e := newRelayEnv(t, relayConfig(0), relayConfig(0), zmuxtest.RelayOptions{MaxAge: 3 * time.Second}, false)
	pol, err := machine.PoolPolicy([]byte(`{"retire":[{"metric":"age","above":{"const":2},"jitter":0.2}]}`), machine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	d := &dialer{addr: e.relay.Addr()}
	ev := watchEvents(t, e.client)
	maintain(t, e.client, zmux.PipePool{Dial: d.dial, Policy: pol})
	poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
	// The bootstrap pipe, outside the relay and the pool, leaves: the
	// stream then rides on pool pipes only.
	p0, _ := e.client.Pipe(0)
	if err := p0.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go echoAll(ctx, e.server)
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		chunk := make([]byte, 4<<10)
		for {
			select {
			case <-stop:
				_ = st.Close()
				return
			case <-time.After(20 * time.Millisecond):
			}
			if _, err := st.Write(chunk); err != nil {
				return
			}
		}
	}()
	var gap time.Duration
	last := time.Now()
	end := last.Add(9 * time.Second)
	buf := make([]byte, 64<<10)
	for time.Now().Before(end) {
		if _, err := st.Read(buf); err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		gap = max(gap, now.Sub(last))
		last = now
	}
	close(stop)
	<-writeDone
	_, _ = io.Copy(io.Discard, st)
	// Each pipe closed after its replacement joined; none failed or
	// stalled. (The relay may still end the connection of a closed pipe
	// while it lingers.)
	ev.mu.Lock()
	joined := map[uint64]bool{}
	for _, x := range ev.got {
		switch x.Kind {
		case zmux.EventPipeFailed, zmux.EventPipeSuspect:
			t.Errorf("event %v on pipe %d", x.Kind, x.Pipe)
		case zmux.EventPipeJoined:
			joined[x.Pipe] = true
		case zmux.EventPipeClosed:
			if x.Pipe != 0 && !joined[x.Pipe+2] {
				t.Errorf("pipe %d closed before its replacement joined", x.Pipe)
			}
		case zmux.EventPipeAdded, zmux.EventPipeRecovered, zmux.EventPipePriorityChanged,
			zmux.EventTierSaturation, zmux.EventJoinAuthFailed, zmux.EventSessionSuspended,
			zmux.EventSessionResumed, zmux.EventGoAwayReceived, zmux.EventJoinSecurityUnavailable,
			zmux.EventPipeQualityChanged, zmux.EventCapacityBelow, zmux.EventCapacityRestored,
			zmux.EventCarrierCandidate, zmux.EventCarrierChosen, zmux.EventPipesRetired:
			// Not checked here.
		}
	}
	ev.mu.Unlock()
	if n := len(d.times()); n < 4 {
		t.Fatalf("%d dials in 9 s with pipes retired at 1.6 s to 2 s", n)
	}
	if gap > 500*time.Millisecond {
		t.Fatalf("the stream stalled for %v", gap)
	}
}

// A last pipe whose T_dead equals its T_suspect (DeadTimeout below
// T_suspect) fails in the same timer pass that makes it suspect: the
// session without resume ends with the pipes lost at once, not at its
// idle timeout.
func TestRelayDeadTimeoutAtSuspicion(t *testing.T) {
	t.Parallel()
	cfg := func() *zmux.Config {
		c := relayConfig(0)
		c.SchedulerParams.ProbeInterval = time.Second
		c.SchedulerParams.DeadTimeout = time.Second
		return c
	}
	e := newRelayEnv(t, cfg(), relayConfig(0), zmuxtest.RelayOptions{}, true)
	e.relay.Blackhole(true, true)
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- e.client.Wait() }()
	select {
	case err := <-done:
		if !errors.Is(err, zmux.ErrPipesLost) {
			t.Fatalf("session ended with %v", err)
		}
		// k = 1 s, then T_suspect = T_dead (about 2 s with the initial
		// RTT), and a margin; the idle timeout is 30 s.
		if el := time.Since(start); el > 6*time.Second {
			t.Fatalf("ended after %v", el)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("session still open: the failure deadline was lost")
	}
}

// The pool's dial context bounds only the setting up of an HTTP carrier:
// the requests outlive it, and the pipe stays without redials.
func TestPoolHTTPPipeOutlivesDialContext(t *testing.T) {
	t.Parallel()
	for _, dir := range []zmux.Direction{zmux.DirBidirectional, zmux.DirSendOnly} {
		e := newHTTPRelayEnv(t, relayConfig(0), relayConfig(0), false)
		var dials atomic.Int64
		maintain(t, e.client, zmux.PipePool{Options: zmux.PipeOptions{Direction: dir},
			Dial: func(ctx context.Context) (zmux.PipeIO, error) {
				dials.Add(1)
				return zmux.HTTPPipe(ctx, e.hc, e.url, nil, dir, nil)
			}})
		poll(t, 5*time.Second, "pool pipe", func() bool { return len(activePool(e.client)) == 1 })
		id := activePool(e.client)[0]
		time.Sleep(time.Second)
		if ids := activePool(e.client); len(ids) != 1 || ids[0] != id || dials.Load() != 1 {
			t.Fatalf("pool pipes %v (was %d) after %d dials", ids, id, dials.Load())
		}
	}
}

// The carriers a session gives up while their downlinks are silently cut
// end with it, whatever else contends for the session lock while their
// aborts are performed: Wait returns soon after Close, so the read loops
// of the dead downlinks do not outlive the session.
func TestRelaySplitDownlinkSilencedClose(t *testing.T) {
	t.Parallel()
	for i := range 4 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			cfg := func() *zmux.Config {
				c := relayConfig(30 * time.Second)
				c.SchedulerParams.ProbeInterval = time.Second
				return c
			}
			e := newHTTPRelayEnv(t, cfg(), cfg(), true)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						_ = e.client.PipeStats()
					}
				})
			}
			defer func() { close(stop); wg.Wait() }()
			var joins atomic.Int64
			maintain(t, e.client, zmux.PipePool{MinBackoff: 100 * time.Millisecond,
				Join: func(ctx context.Context, c *zmux.Conn, o zmux.PipeOptions) ([]*zmux.Pipe, error) {
					joins.Add(1)
					return zmux.HTTPSplitJoin(ctx, c, e.hc, e.url, nil, nil, o)
				}})
			// Blackhole affects the relay's current connections only: the
			// member's carriers must carry bytes before it, or a downlink
			// connected later escapes it and the member stays healthy.
			poll(t, 5*time.Second, "pool member", func() bool {
				up, down := false, false
				for _, st := range e.client.PipeStats() {
					up = up || st.Pipe != 0 && st.Outbound == zmux.OutboundActive
					down = down || st.Pipe != 0 && st.Outbound == zmux.OutboundNone && st.Inbound == zmux.InboundOpen
				}
				return up && down
			})
			var upID uint64
			for _, st := range e.client.PipeStats() {
				if st.Pipe != 0 && st.Outbound == zmux.OutboundActive {
					upID = st.Pipe
				}
			}
			up, _ := e.client.Pipe(upID)
			e.relay.Blackhole(false, true)
			// Its uplink works but is never acknowledged: T_fail of pipe 0
			// and then T_dead = T_fail (resume) of the uplink, 2 s each at
			// these round-trip times, and a margin.
			poll(t, 8*time.Second, "member uplink failure", func() bool {
				st, ok := up.Stats()
				return !ok || st.Outbound == zmux.OutboundFailed || st.Outbound == zmux.OutboundClosed
			})
			// The member fails and the pool joins a new one, on new
			// connections that the relay carries again.
			poll(t, 25*time.Second, "rejoin", func() bool { return joins.Load() >= 2 })
			time.Sleep(20 * time.Second)
			_ = e.client.Close()
			done := make(chan struct{})
			go func() { _ = e.client.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Wait did not return after Close")
			}
		})
	}
}
