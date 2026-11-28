package zmux

import (
	"context"
	"io"
	"runtime"
	"runtime/trace"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// flight records the last seconds of the execution trace of the whole
// test binary, for tests to write out when they fail (WriteFlightTrace).
var flight struct {
	fr         *trace.FlightRecorder
	sync.Mutex // one WriteTo at a time
}

// TestMain starts the flight recorder and fails the binary on goroutines
// the tests leave behind.
func TestMain(m *testing.M) {
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: 5 * time.Second, MaxBytes: 32 << 20})
	if fr.Start() == nil {
		flight.fr = fr
	}
	leakcheck.Main(m)
}

// WriteFlightTrace writes the flight recorder's window, the last seconds
// of the execution trace, to w (for go tool trace).
func WriteFlightTrace(w io.Writer) error {
	flight.Lock()
	defer flight.Unlock()
	if flight.fr == nil {
		return io.ErrClosedPipe
	}
	_, err := flight.fr.WriteTo(w)
	return err
}

// mustNewListener is NewListener for a configuration the test knows valid.
func mustNewListener(cfg *ListenerConfig) *Listener {
	l, err := NewListener(cfg)
	if err != nil {
		panic(err)
	}
	return l
}

// startServerNoListener starts a server session as Start(RoleServer)
// does, without its check that a server without a Listener takes no
// joins: for tests of a multipath or resume bootstrap pipe alone (no join
// can reach it).
func startServerNoListener(ctx context.Context, p PipeIO, cfg *Config) (*Conn, error) {
	cfg = cfg.Clone()
	cc, err := cfg.coreConfig(core.RoleResponder, core.RoleResponder)
	if err != nil {
		ClosePipeIO(p)
		return nil, err
	}
	return startCore(ctx, p, cfg, cc, nil, nil, nil, nil)
}

func waitTestSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, ctx.Err())
	}
}

// waitStreamStop waits for the write-side error notification.
func waitStreamStop(ctx context.Context, s *Stream) error {
	defer runtime.KeepAlive(s)
	h := s.h
	_, err := h.waitOn(ctx, &h.wsig, nil, h.wwait, func() (bool, error) {
		return false, h.c.s.WriteErrOf(h.st)
	}, nil)
	return err
}

func waitTestEvent(t *testing.T, ctx context.Context, c *Conn, what string, match func(Event) bool) Event {
	t.Helper()
	for {
		ev, err := c.NextEvent(ctx)
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if match(ev) {
			return ev
		}
	}
}

func waitTestAttachment(t *testing.T, ctx context.Context, p *Pipe, peer *Conn) *Pipe {
	t.Helper()
	if err := p.WaitAttached(ctx); err != nil {
		t.Fatal(err)
	}
	joined, ok := peer.Pipe(p.ID())
	if !ok {
		t.Fatalf("peer acknowledged pipe %d but has no handle for it", p.ID())
	}
	return joined
}
