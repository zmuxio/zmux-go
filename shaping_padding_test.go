package zmux

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// A handshake's padding is metered once by the session its carrier becomes part
// of: at the start, and on either side of a join.
func TestHandshakePaddingOnEveryAttachment(t *testing.T) {
	const serverPad, clientPad = 700, 1000
	e := newMPEnvOn(t, mpConfig(), &ListenerConfig{Config: mpConfig(), InstanceID: []byte("instance"),
		WrapPipe: WrapPipeFunc(func(_ context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
			p.HandshakePadding = serverPad
			return p, nil
		})})
	if got := e.server.ShapingMeters().WireSpent; got != serverPad {
		t.Fatalf("server after start: %d", got)
	}
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NetPipe(nc)
	p.HandshakePadding = clientPad
	if _, err := e.client.AddPipe(ctx, p, PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := e.client.ShapingMeters().WireSpent; got != clientPad {
		t.Fatalf("client after join: %d", got)
	}
	for e.server.ShapingMeters().WireSpent != 2*serverPad {
		if ctx.Err() != nil {
			t.Fatalf("server after join: %d", e.server.ShapingMeters().WireSpent)
		}
		time.Sleep(time.Millisecond)
	}
}

// SetShaper without a budget function leaves no configuration budget; the
// spending stays.
func TestSetShaperNilBudgetIsUnset(t *testing.T) {
	e := newMPEnv(t, mpConfig(), mpConfig())
	e.client.SetShaper(func(ShapedPipe) shaping.Shaper { return nil }, func() shaping.Budget {
		return shaping.Budget{Wire: shaping.Account{Initial: 5}, NoInjection: true}
	})
	if got := e.client.ShapingMeters().WireLeft; got != 5 {
		t.Fatalf("left %d with a budget", got)
	}
	e.client.SetShaper(func(ShapedPipe) shaping.Shaper { return nil }, nil)
	e.client.lock()
	eff := e.client.s.ShapeLimitsInForce()
	e.client.unlock()
	if left := e.client.ShapingMeters().WireLeft; left != ^uint64(0) || eff.NoInjection {
		t.Fatalf("left %d, limits %+v", left, eff)
	}
}
