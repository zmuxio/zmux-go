package zmuxhttp

import (
	"context"

	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

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
