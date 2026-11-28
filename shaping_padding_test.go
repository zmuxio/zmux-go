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
			p.HandshakeWrites[0] = shaping.HandshakeWrite{At: time.Now(), Allowance: serverPad, Extra: serverPad}
			return p, nil
		})})
	if got := e.server.ShapingMeters().Handshake.ConsumedBytes; got != serverPad {
		t.Fatalf("server after start: %d", got)
	}
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NetPipe(nc)
	p.HandshakeWrites[0] = shaping.HandshakeWrite{At: time.Now(), Allowance: clientPad, Extra: clientPad}
	added, err := e.client.AddPipe(ctx, p, PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.client.ShapingMeters().Handshake.ConsumedBytes; got != clientPad {
		t.Fatalf("client after join: %d", got)
	}
	for {
		ev, err := e.server.NextEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Kind == EventPipeJoined && ev.PipeID == added.ID() {
			break
		}
	}
	if got := e.server.ShapingMeters().Handshake.ConsumedBytes; got != 2*serverPad {
		t.Fatalf("server after join: %d", got)
	}
}

// A join that fails before its pipe starts spends none of its carrier's
// handshake padding.
func TestHandshakePaddingNotSpentOnFailedJoin(t *testing.T) {
	e := newMPEnv(t, mpConfig(), mpConfig())
	before := e.client.ShapingMeters().Handshake.ConsumedBytes
	a, b := net.Pipe()
	_ = b.Close() // the join's Hello fails
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NetPipe(a)
	p.HandshakeWrites[0] = shaping.HandshakeWrite{At: time.Now(), Allowance: 1000, Extra: 1000}
	if _, err := e.client.AddPipe(ctx, p, PipeOptions{}); err == nil {
		t.Fatal("join on a dead carrier succeeded")
	}
	if got := e.client.ShapingMeters().Handshake.ConsumedBytes; got != before {
		t.Fatalf("spent %d after a failed join, want %d", got, before)
	}
}

// SetShaper without a budget function leaves no configuration budget; the
// spending stays.
func TestSetShaperNilBudgetIsUnset(t *testing.T) {
	e := newMPEnv(t, mpConfig(), mpConfig())
	mustSetShaper(t, e.client, func(ShapedPipe) shaping.Shaper { return nil }, func() shaping.Budget {
		return shaping.Budget{Wire: &shaping.Account{Ceiling: 5, Initial: 5}, NoInjection: true}
	})
	if got := e.client.ShapingMeters().WireLeft; got != 5 {
		t.Fatalf("left %d with a budget", got)
	}
	mustSetShaper(t, e.client, func(ShapedPipe) shaping.Shaper { return nil }, nil)
	e.client.lock()
	eff := e.client.s.ShapeLimitsInForce()
	e.client.unlock()
	if left := e.client.ShapingMeters().WireLeft; left != 0 || eff.Wire != nil || eff.NoInjection {
		t.Fatalf("left %d, limits %+v", left, eff)
	}
}
