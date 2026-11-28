package zmux_test

import (
	"bytes"
	"context"

	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx"
	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go"

	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

// Tests of carrier loss through relays that behave as intermediaries on
// a path: idle limits, black holes, refused connections, connection age
// limits. Each runs in parallel with the others and in a few seconds.

func relayConfig(resume time.Duration) *zmux.Config {
	return &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true},
		ResumeTimeout:   resume,
		SchedulerParams: zmux.SchedulerParams{Probation: -1}}
}

// relayEnv is a listener of a server, a relay in front of it and a
// client whose bootstrap carrier goes direct or through the relay: over
// TCP (newRelayEnv) or in memory (newMemRelayEnv, for tests in a
// testing/synctest bubble).
type relayEnv struct {
	ln     net.Listener
	relay  *zmuxtest.Relay
	client *zmux.Conn
	server *zmux.Conn
	// dialServer and dialRelay connect to the server and to the relay.
	dialServer, dialRelay func(context.Context) (net.Conn, error)
}

func newRelayEnv(t *testing.T, ccfg, scfg *zmux.Config, ro zmuxtest.RelayOptions, viaRelay bool) *relayEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcp := func(addr string) func(context.Context) (net.Conn, error) {
		return func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}
	}
	rl, err := zmuxtest.NewRelay(ln.Addr().String(), ro)
	if err != nil {
		t.Fatal(err)
	}
	return startRelayEnv(t, ccfg, scfg, viaRelay, &relayEnv{ln: ln, relay: rl,
		dialServer: tcp(ln.Addr().String()), dialRelay: tcp(rl.Addr())})
}

// newMemRelayEnv is newRelayEnv over in-memory pipes; both configurations
// take the host clock as Config.ClockDomain, which the bubble's fake clock
// drives on every platform (the session's own monotonic source on Windows
// is outside it).
func newMemRelayEnv(t *testing.T, ccfg, scfg *zmux.Config, viaRelay bool) *relayEnv {
	t.Helper()
	ccfg.ClockDomain, scfg.ClockDomain = zmux.NewClockDomain(timeshim.Real), zmux.NewClockDomain(timeshim.Real)
	sln, rln := netx.NewChanListener(nil, 0), netx.NewChanListener(nil, 0)
	dialServer := func(ctx context.Context) (net.Conn, error) { return sln.Dial(ctx, nil) }
	return startRelayEnv(t, ccfg, scfg, viaRelay, &relayEnv{ln: sln, relay: zmuxtest.NewRelayOn(rln, dialServer, zmuxtest.RelayOptions{}),
		dialServer: dialServer, dialRelay: func(ctx context.Context) (net.Conn, error) { return rln.Dial(ctx, nil) }})
}

// artifactsOnFailure writes, when t fails, the flight recorder's trace of
// the last seconds to t.ArtifactDir() (kept with go test -artifacts).
func artifactsOnFailure(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		name := filepath.Join(t.ArtifactDir(), "flight.trace")
		f, err := os.Create(name)
		if err != nil {
			t.Log(err)
			return
		}
		if err := zmux.WriteFlightTrace(f); err != nil {
			t.Logf("flight trace: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Log(err)
		}
		t.Logf("flight trace in %s", name)
	})
}

func startRelayEnv(t *testing.T, ccfg, scfg *zmux.Config, viaRelay bool, e *relayEnv) *relayEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	lst := mustNewListener(&zmux.ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := e.ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { _ = lst.HandlePipe(ctx, zmux.NetPipe(c), zmux.PipeHint{}) })
		}
	})
	dial := e.dialServer
	if viaRelay {
		dial = e.dialRelay
	}
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	nc, err := dial(dctx)
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
		_ = e.relay.Close()
		_ = e.ln.Close()
		_ = lst.Close()
		cancel()
		wg.Wait()
		<-e.client.Done()
		<-e.server.Done()
	})
	artifactsOnFailure(t)
	return e
}

// activePool returns the IDs of the client's active pipes other than 0.
func attachedPool(c *zmux.Conn) []uint64 {
	var ids []uint64
	for _, st := range c.PipeStats() {
		if st.PipeID != 0 && st.Outbound == zmux.OutboundActive && st.PeerAttached {
			ids = append(ids, uint64(st.PipeID))
		}
	}
	return ids
}

func waitAttachedAfter(t *testing.T, ctx context.Context, c *zmux.Conn, after zmux.PipeID) zmux.PipeID {
	t.Helper()
	for {
		for _, id := range attachedPool(c) {
			if zmux.PipeID(id) > after {
				return zmux.PipeID(id)
			}
		}
		if _, err := c.NextEvent(ctx); err != nil {
			t.Fatalf("waiting for attachment after pipe %d: %v", after, err)
		}
	}
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

type httpRelayEnv struct {
	// ctx bounds the HTTP requests of the carriers: the session's life.
	ctx    context.Context
	domain *zmux.ClockDomain
	relay  *zmuxtest.Relay
	hc     *http.Client
	// relay2 and hc2: a second path to the server, for pipes that one
	// relay's faults must not reach.
	relay2          *zmuxtest.Relay
	hc2             *http.Client
	client          *zmux.Conn
	server          *zmux.Conn
	waitConnections func()
	url             string
}

// newHTTPRelayEnv runs the HTTP transport and relay over memory connections
// so that synctest can drive liveness and the HTTP carrier's own timers.
func newHTTPRelayEnv(t *testing.T, ccfg, scfg *zmux.Config, split bool) *httpRelayEnv {
	t.Helper()
	ccfg.ClockDomain, scfg.ClockDomain = zmux.NewClockDomain(timeshim.Real), zmux.NewClockDomain(timeshim.Real)
	lst := mustNewListener(&zmux.ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
	hh, err := zmuxhttp.NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln := netx.NewChanListener(nil, 0)
	var connections sync.WaitGroup
	srv := &http.Server{Handler: hh, ConnState: func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			connections.Add(1)
		case http.StateClosed, http.StateHijacked:
			connections.Done()
		case http.StateActive, http.StateIdle:
		}
	}}
	var serving sync.WaitGroup
	serving.Go(func() { _ = srv.Serve(ln) })
	newPath := func() (*zmuxtest.Relay, *http.Transport) {
		path := netx.NewChanListener(nil, 0)
		rl := zmuxtest.NewRelayOn(path, func(ctx context.Context) (net.Conn, error) {
			return ln.Dial(ctx, nil)
		}, zmuxtest.RelayOptions{})
		tr := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return path.Dial(ctx, nil)
		}}
		return rl, tr
	}
	rl, tr := newPath()
	rl2, tr2 := newPath()
	life, end := context.WithCancel(t.Context())
	t.Cleanup(func() {
		end()
		tr.CloseIdleConnections()
		tr2.CloseIdleConnections()
		_ = rl.Close()
		_ = rl2.Close()
		_ = ln.Close()
		_ = srv.Close()
		serving.Wait()
		connections.Wait()
		_ = lst.Close()
	})
	e := &httpRelayEnv{domain: ccfg.ClockDomain, ctx: life, relay: rl, hc: &http.Client{Transport: tr}, url: "http://" + ln.Addr().String(),
		relay2: rl2, hc2: &http.Client{Transport: tr2}, waitConnections: connections.Wait}
	ctx, cancel := context.WithTimeout(life, time.Minute)
	defer cancel()
	var p zmux.PipeIO
	if split {
		p, err = zmuxhttp.Split{Client: e.hc, URL: e.url}.Pipe(life)
	} else {
		p, err = zmuxhttp.Streaming{Client: e.hc, URL: e.url}.Pipe(life, zmux.DirectionBidirectional)
	}
	if err != nil {
		t.Fatal(err)
	}
	if e.client, err = zmux.Start(ctx, p, zmux.RoleClient, ccfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.client.Close(); _ = e.client.Wait() })
	if e.server, err = lst.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.server.Close(); _ = e.server.Wait() })
	artifactsOnFailure(t)
	return e
}

func waitRelaySignal(t *testing.T, ctx context.Context, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, ctx.Err())
	}
}

func silencedRelayConfig() *zmux.Config {
	c := relayConfig(30 * time.Second)
	c.Liveness.ProbeInterval = time.Second
	c.Liveness.ProbeJitter = -1
	return c
}

func TestHTTPDuplexUsesOnePhysicalCarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{LocalCarriers: 1}}}
		e := newHTTPRelayEnv(t, cfg, cfg.Clone(), false)
		for _, c := range []*zmux.Conn{e.client, e.server} {
			v, err := c.Capacity()
			if err != nil || v.LocalCarriers != 1 {
				t.Fatalf("one duplex request: %+v, %v", v, err)
			}
		}
	})
}

// A split bootstrap and pool member lose their downlinks. The pool joins a
// new member over two new requests. A standalone downlink also retires
// after its inbound silence limit.
func TestRelaySplitDownlinkSilenced(t *testing.T) {
	t.Parallel()
	t.Run("split pair", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newHTTPRelayEnv(t, silencedRelayConfig(), silencedRelayConfig(), true)
			e.maintainSplit(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			old := waitAttachedAfter(t, ctx, e.client, 0)
			boot, ok := e.client.Pipe(0)
			if !ok {
				t.Fatal("missing bootstrap pipe")
			}
			e.relay.Blackhole(false, true)
			waitRelaySignal(t, ctx, boot.Done(), "silent bootstrap retirement")
			waitAttachedAfter(t, ctx, e.client, old)
		})
	})
	t.Run("split pair joins", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newHTTPRelayEnv(t, relayConfig(0), relayConfig(0), true)
			ctx, cancel := context.WithTimeout(e.ctx, time.Minute)
			defer cancel()
			p, err := zmuxhttp.Split{Client: e.hc, URL: e.url}.Pipe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.W.Close(); _ = p.R.Close() }()
			if _, err := e.client.AddPipe(ctx, p, zmux.PipeOptions{}); err != nil {
				t.Fatalf("AddPipe of a split pair: %v", err)
			}
		})
	})
	t.Run("downlink alone", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newHTTPRelayEnv(t, relayConfig(0), relayConfig(0), false)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			// Only the new downlink goes silent.
			p, err := zmuxhttp.Split{Client: e.hc2, URL: e.url}.Join(ctx, e.client, zmux.PipeOptions{Direction: zmux.DirectionReceiveOnly})
			if err != nil {
				t.Fatal(err)
			}
			if st, ok := p.Stats(); !ok || st.Inbound != zmux.InboundOpen {
				t.Fatalf("joined downlink is not open: %+v", st)
			}
			e.relay2.Blackhole(false, true)
			waitRelaySignal(t, ctx, p.Done(), "silent downlink retirement")
			if err := e.client.Context().Err(); err != nil {
				t.Fatalf("the healthy bootstrap did not keep the session alive: %v", err)
			}
		})
	})
}

func TestRelayDeadTimeoutAtSuspicion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cfg := func() *zmux.Config {
			c := relayConfig(0)
			c.Liveness.ProbeInterval = time.Second
			c.Liveness.DeadTimeout = time.Second
			return c
		}
		e := newMemRelayEnv(t, cfg(), relayConfig(0), true)
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
	})
}

// A silenced split pipe must release its carrier work when it is retired;
// closing the session must also release the replacement's HTTP requests.
func TestRelaySplitDownlinkSilencedClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newHTTPRelayEnv(t, silencedRelayConfig(), silencedRelayConfig(), true)
		ctl := e.maintainSplit(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		oldID := waitAttachedAfter(t, ctx, e.client, 0)
		old, ok := e.client.Pipe(oldID)
		if !ok {
			t.Fatal("attached pipe disappeared")
		}
		e.relay.Blackhole(false, true)
		waitAttachedAfter(t, ctx, e.client, oldID)
		waitRelaySignal(t, ctx, old.Done(), "old split pipe retirement")
		e.relay.Blackhole(false, false)
		queried := make(chan struct{})
		go func() {
			_ = e.client.PipeStats()
			close(queried)
		}()
		_ = e.client.Close()
		waitRelaySignal(t, ctx, queried, "pipe stats during close")
		waitRelaySignal(t, ctx, e.client.Done(), "client session end")
		_ = ctl.Close()
		waitRelaySignal(t, ctx, ctl.Done(), "controller work release")
		closedHTTP := make(chan struct{})
		go func() {
			e.waitConnections()
			close(closedHTTP)
		}()
		waitRelaySignal(t, ctx, closedHTTP, "HTTP connection cleanup")
	})
}

func (e *httpRelayEnv) maintainSplit(t *testing.T) *zmux.SessionController {
	t.Helper()
	candidate := (zmuxhttp.Split{Client: e.hc, URL: e.url}).Candidate()
	candidate.ID, candidate.CacheKey = "relay", "relay"
	ctl, err := zmux.NewSessionController(zmux.ControllerOptions{Domain: t.Name(),
		Candidates: []zmux.Candidate{candidate}, ClockDomain: e.domain})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ctl.Close()
		<-ctl.Done()
	})
	_, err = ctl.Manage(e.client, zmux.SessionTarget{Revision: 1, Groups: []zmux.GroupTarget{{
		ID: "relay", Quotas: []zmux.MemberQuota{{
			ID: "relay", Count: 1, Directions: zmux.Both, Candidates: []zmux.TargetCandidate{{ID: candidate.ID}},
		}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ctl
}
