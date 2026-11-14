package zmuxhttp

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestWrapPipeTLS(t *testing.T) {
	leakcheck.Check(t)
	scfg, ccfg := selfSignedTLS(t)
	scfg.MinVersion, ccfg.MinVersion = tls.VersionTLS13, tls.VersionTLS13
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("tcp", func(t *testing.T) {
		lst := mustNewListener(&zmux.ListenerConfig{Config: tlsWrapConfig(), WrapPipe: zmux.WrapPipeFunc(serverTLSWrap(scfg))})
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
		c, err := zmux.Client(ctx, dial(), tlsWrapConfig())
		if err != nil {
			t.Fatal(err)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
		checkWrapped(t, ctx, c, s, 0, func() zmux.PipeIO { return zmux.NetPipe(dial()) })
	})

	t.Run("streaming", func(t *testing.T) {
		lst := mustNewListener(&zmux.ListenerConfig{Config: tlsWrapConfig(), WrapPipe: zmux.WrapPipeFunc(serverTLSWrap(scfg))})
		defer func() { _ = lst.Close() }()
		hh, err := NewHandler(lst, nil)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewUnstartedServer(hh)
		srv.EnableHTTP2 = true
		srv.StartTLS()
		defer srv.Close()
		cand := clientTLSWrap(Streaming{Client: srv.Client(), URL: srv.URL}.Candidate(), ccfg)
		c, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(cand), tlsWrapConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
		checkWrapped(t, ctx, c, s, zmux.CarrierStream, func() zmux.PipeIO {
			p, err := cand.Dial(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			p.Carrier = zmux.CarrierStream
			return p
		})
	})
}

// Each half of a split bootstrap reaches WrapPipe with its pairing hint,
// and passes through it unchanged.
func TestWrapPipeSplit(t *testing.T) {
	leakcheck.Check(t)
	var mu sync.Mutex
	var halves []zmux.PipeHint
	wrap := func(_ context.Context, p zmux.PipeIO, hint zmux.PipeHint) (zmux.PipeIO, error) {
		mu.Lock()
		halves = append(halves, hint)
		mu.Unlock()
		return p, nil
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), WrapPipe: zmux.WrapPipeFunc(wrap)})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hh)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Split{Client: srv.Client(), URL: srv.URL}.Pipe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
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
	if len(halves) != 1 || halves[0].Pair != "" {
		t.Fatalf("WrapPipe runs once, on the pair: %+v", halves)
	}
	if st, _ := s.Pipes()[0].Stats(); st.Carrier != zmux.CarrierSplit {
		t.Fatalf("carrier kind %v", st.Carrier)
	}
}

// checkWrapped checks a session over wrapped carriers: data flows, the
// carrier kinds are unchanged, and a join over a further
// wrapped carrier succeeds.
func checkWrapped(t *testing.T, ctx context.Context, c, s *zmux.Conn, kind zmux.CarrierKind, more func() zmux.PipeIO) {
	t.Helper()
	echoOnce(t, ctx, c, s)
	for _, x := range []*zmux.Conn{c, s} {
		ps := x.Pipes()
		if len(ps) == 0 {
			t.Fatal("no pipes")
		}
		if st, _ := ps[0].Stats(); st.Carrier != kind {
			t.Fatalf("carrier kind %v, want %v", st.Carrier, kind)
		}
	}
	if _, err := c.AddPipe(ctx, more(), zmux.PipeOptions{}); err != nil {
		t.Fatalf("join: %v", err)
	}
	echoOnce(t, ctx, c, s)
}
