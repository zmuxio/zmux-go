package zmuxquic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// gatedCred fails its first prefix once gate closes; later ones pass.
type gatedCred struct {
	entered chan struct{}
	gate    chan struct{}
	calls   atomic.Int32
}

var errGated = errors.New("gated credential failed")

func (c *gatedCred) Prefix(*zmux.CarrierInfo) ([]byte, error) {
	if c.calls.Add(1) == 1 {
		close(c.entered)
		<-c.gate
		return nil, errGated
	}
	return []byte{0}, nil
}

func (*gatedCred) Header(*zmux.CarrierInfo) (name, value string, err error) { return "", "", nil }

// fatePolicy reports the members of each decision.
type fatePolicy chan []uint64

func (p fatePolicy) Decide(v *carrier.ConnectionView, d *carrier.ConnectionDecision) error {
	fates := make([]uint64, 0, len(v.Members))
	for _, m := range v.Members {
		fates = append(fates, m.FateGroup)
	}
	d.Recheck = time.Hour
	select {
	case p <- fates:
	default:
	}
	return nil
}

// A call that waited for a connection whose credential then failed to go
// out opens its pipe on another connection, never on the failed one.
func TestQUICPoolCredentialFailure(t *testing.T) {
	e := newEnv(t, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cred := &gatedCred{entered: make(chan struct{}), gate: make(chan struct{})}
	pol := make(fatePolicy, 64)
	p := NewConnPool(e.ln.Addr().String(), e.cli, nil, cred, carrier.PoolLimits{Policy: pol})
	defer p.Shutdown()
	first := make(chan error, 1)
	go func() {
		_, err := p.OpenPipe(ctx)
		first <- err
	}()
	<-cred.entered
	<-pol // the first call's take
	<-pol // its connection joined
	type res struct {
		err error
		pio zmux.PipeIO
	}
	second := make(chan res, 1)
	go func() {
		pio, err := p.OpenPipe(ctx)
		second <- res{err, pio}
	}()
	failed := <-pol // the second call's take sees the first connection
	if len(failed) != 1 {
		t.Fatalf("members %v", failed)
	}
	close(cred.gate)
	if err := <-first; !errors.Is(err, errGated) {
		t.Fatalf("first: %v", err)
	}
	r := <-second
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.pio.W.(*qStream).Abort(errGated)
	if r.pio.FateGroup == failed[0] {
		t.Fatal("a pipe on the connection whose credential failed")
	}
}
