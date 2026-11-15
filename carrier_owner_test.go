package zmux

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingCarrier is one carrier used as both halves: Read blocks until it
// is closed, and Close counts its calls.
type countingCarrier struct {
	closes atomic.Int32
	once   sync.Once
	done   chan struct{}
}

func newCountingCarrier() *countingCarrier { return &countingCarrier{done: make(chan struct{})} }

func (c *countingCarrier) Read([]byte) (int, error) {
	<-c.done
	return 0, io.EOF
}

func (c *countingCarrier) Write(b []byte) (int, error) {
	select {
	case <-c.done:
		return 0, io.ErrClosedPipe
	default:
		return len(b), nil
	}
}

func (c *countingCarrier) Close() error {
	c.closes.Add(1)
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *countingCarrier) pipe() PipeIO { return PipeIO{R: c, W: c} }

func (c *countingCarrier) checkOnce(t *testing.T) {
	t.Helper()
	if n := c.closes.Load(); n != 1 {
		t.Fatalf("carrier closed %d times, want once", n)
	}
}

// The first-record timeout closes the carrier to end the read, and the
// rejection that follows does not close it again.
func TestFirstRecordTimeoutClosesCarrierOnce(t *testing.T) {
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), FirstRecordTimeout: 10 * time.Millisecond})
	defer func() { _ = lst.Close() }()
	cc := newCountingCarrier()
	if err := lst.HandlePipe(context.Background(), cc.pipe(), PipeHint{}); err == nil {
		t.Fatal("a silent carrier was taken")
	}
	cc.checkOnce(t)
}

// A WrapPipe layer that closes its PipeConn when the handshake bound ends
// it shares the carrier's closer with the bound and the failure path.
func TestWrapPipeBoundAndLayerCloseCarrierOnce(t *testing.T) {
	cfg := mpConfig()
	cfg.HandshakeTimeout = 10 * time.Millisecond
	lst := mustNewListener(&ListenerConfig{Config: cfg, FirstRecordTimeout: time.Second,
		WrapPipe: WrapPipeFunc(func(ctx context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
			nc := PipeConn(p)
			<-ctx.Done()
			_ = nc.Close()
			return PipeIO{}, ctx.Err()
		})})
	defer func() { _ = lst.Close() }()
	cc := newCountingCarrier()
	if err := lst.HandlePipe(context.Background(), cc.pipe(), PipeHint{}); err == nil {
		t.Fatal("a failed WrapPipe was taken")
	}
	cc.checkOnce(t)
}
