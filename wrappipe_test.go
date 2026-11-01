package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// tlsWrapConfig is a session config whose join secret is pipe 0's TLS
// exporter: a session starts only when both ends see the same TLS.
func tlsWrapConfig() *Config {
	return &Config{MaxPipes: 4, PipeAuth: PipeAuth{TLS: true},
		SchedulerParams: SchedulerParams{NoProbation: true}}
}

// serverTLSWrap is a WrapPipe that runs a TLS server over every carrier.
func serverTLSWrap(cfg *tls.Config) func(context.Context, PipeIO, JoinHint) (PipeIO, error) {
	return func(ctx context.Context, p PipeIO, _ JoinHint) (PipeIO, error) {
		tc := tls.Server(PipeConn(p), cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = tc.Close()
			return PipeIO{}, err
		}
		return NetPipe(tc), nil
	}
}

// clientTLSWrap wraps a Candidate's carriers in TLS.
func clientTLSWrap(c Candidate, cfg *tls.Config) Candidate {
	dial := c.Dial
	c.Dial = func(ctx context.Context, cred Credential) (PipeIO, error) {
		p, err := dial(ctx, cred)
		if err != nil {
			return PipeIO{}, err
		}
		tc := tls.Client(PipeConn(p), cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = tc.Close()
			return PipeIO{}, err
		}
		return NetPipe(tc), nil
	}
	return c
}

// checkWrapped checks a session over wrapped carriers: data flows, the
// carrier kinds are unchanged, and a carrier-bound join over a further
// wrapped carrier succeeds.
func checkWrapped(t *testing.T, ctx context.Context, c, s *Conn, kind CarrierKind, more func() PipeIO) {
	t.Helper()
	echoOnce(t, ctx, c, s)
	for _, x := range []*Conn{c, s} {
		ps := x.Pipes()
		if len(ps) == 0 {
			t.Fatal("no pipes")
		}
		if st, _ := ps[0].Stats(); st.Carrier != kind {
			t.Fatalf("carrier kind %v, want %v", st.Carrier, kind)
		}
	}
	if _, err := c.AddPipe(ctx, more(), PipeOptions{CarrierBound: true}); err != nil {
		t.Fatalf("carrier-bound join: %v", err)
	}
	echoOnce(t, ctx, c, s)
}

func TestWrapPipeTLS(t *testing.T) {
	leakcheck.Check(t)
	scfg, ccfg := selfSignedTLS(t)
	scfg.MinVersion, ccfg.MinVersion = tls.VersionTLS13, tls.VersionTLS13
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("tcp", func(t *testing.T) {
		lst := NewListener(&ListenerConfig{Config: tlsWrapConfig(), WrapPipe: serverTLSWrap(scfg)})
		defer func() { _ = lst.Close() }()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		go func() { _ = lst.Serve(ctx, ln) }()
		dial := func() net.Conn {
			nc, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			tc := tls.Client(nc, ccfg)
			if err := tc.HandshakeContext(ctx); err != nil {
				t.Fatal(err)
			}
			return tc
		}
		c, err := Client(ctx, dial(), tlsWrapConfig())
		if err != nil {
			t.Fatal(err)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
		checkWrapped(t, ctx, c, s, 0, func() PipeIO { return NetPipe(dial()) })
	})

	t.Run("streaming", func(t *testing.T) {
		lst := NewListener(&ListenerConfig{Config: tlsWrapConfig(), WrapPipe: serverTLSWrap(scfg)})
		defer func() { _ = lst.Close() }()
		hh, err := NewHTTPHandler(lst, nil)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewUnstartedServer(hh)
		srv.EnableHTTP2 = true
		srv.StartTLS()
		defer srv.Close()
		cand := clientTLSWrap(StreamingCandidate(srv.Client(), srv.URL, nil), ccfg)
		c, err := DialMode(ctx, ExplicitMode(cand), tlsWrapConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
		checkWrapped(t, ctx, c, s, CarrierStreaming, func() PipeIO {
			p, err := cand.Dial(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			p.Carrier = CarrierStreaming
			return p
		})
	})
}

// A WrapPipe error, or one that outlasts the handshake timeout, closes the
// carrier and counts a setup failure.
func TestWrapPipeFailure(t *testing.T) {
	leakcheck.Check(t)
	for _, mode := range []string{"error", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			cfg := mpConfig()
			cfg.HandshakeTimeout = 200 * time.Millisecond
			wrap := func(_ context.Context, p PipeIO, _ JoinHint) (PipeIO, error) {
				if mode == "error" {
					return PipeIO{}, errors.New("refused")
				}
				var b [1]byte
				_, err := p.R.Read(b[:]) // ignores ctx: ends when p is closed
				return PipeIO{}, err
			}
			lst := NewListener(&ListenerConfig{Config: cfg, WrapPipe: wrap})
			defer func() { _ = lst.Close() }()
			a, b := net.Pipe()
			done := make(chan error, 1)
			go func() { done <- lst.HandlePipe(context.Background(), NetPipe(a), JoinHint{}) }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("HandlePipe succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("WrapPipe not bounded")
			}
			var x [1]byte
			if _, err := b.Read(x[:]); err == nil {
				t.Fatal("carrier not closed")
			}
			_ = b.Close()
			if st := lst.Stats(); st.SetupFailures != 1 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

// Each half of a split bootstrap reaches WrapPipe with its pairing hint,
// and passes through it unchanged.
func TestWrapPipeSplit(t *testing.T) {
	leakcheck.Check(t)
	var mu sync.Mutex
	var halves []JoinHint
	wrap := func(_ context.Context, p PipeIO, hint JoinHint) (PipeIO, error) {
		mu.Lock()
		halves = append(halves, hint)
		mu.Unlock()
		return p, nil
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), WrapPipe: wrap})
	defer func() { _ = lst.Close() }()
	hh, err := NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hh)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := HTTPSplitPipe(ctx, srv.Client(), srv.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Start(ctx, p, RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	mu.Lock()
	defer mu.Unlock()
	if len(halves) != 2 || halves[0].Pair == "" || halves[0].Pair != halves[1].Pair || halves[0].Uplink == halves[1].Uplink {
		t.Fatalf("halves %+v", halves)
	}
	if st, _ := s.Pipes()[0].Stats(); st.Carrier != CarrierSplit {
		t.Fatalf("carrier kind %v", st.Carrier)
	}
}

// DialMode refuses a mode with two candidates of one kind.
func TestDialModeDuplicateKind(t *testing.T) {
	dial := func(context.Context, Credential) (PipeIO, error) { return PipeIO{}, errors.New("unused") }
	m := AutoMode(Candidate{Kind: CarrierStreaming, Dial: dial}, Candidate{Kind: CarrierExchange, Dial: dial},
		Candidate{Kind: CarrierStreaming, Dial: dial})
	if _, err := DialMode(context.Background(), m, nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err %v, want ErrInvalidConfig", err)
	}
}
