package zmux

import (
	"context"
	"io"
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
		closePipeIO(p)
		return nil, err
	}
	return startCore(ctx, p, cfg, cc, nil, nil)
}
