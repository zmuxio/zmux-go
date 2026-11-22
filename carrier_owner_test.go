package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/netx"
)

// countingCarrier is one carrier used as both halves: Read blocks until it
// is closed, and Close counts its calls.
type countingCarrier struct {
	done   chan struct{}
	closes atomic.Int32
	once   sync.Once
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

// Both calls of a split handoff finish while the carrier is live. Cancelling
// their setup contexts leaves the session able to use the bootstrap and join.
func TestHandlePipeHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	l := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = l.Close() }()
	setup, stop := context.WithCancel(ctx)
	defer stop()
	completed := make(chan error, 3)
	up, read := net.Pipe()
	write, down := net.Pipe()
	const token = "AAAAAAAAAAAAAAAAAAAAAA"
	go func() { completed <- l.HandlePipe(setup, PipeIO{R: read}, PipeHint{Pair: token, Uplink: true}) }()
	go func() { completed <- l.HandlePipe(setup, PipeIO{W: write}, PipeHint{Pair: token}) }()
	c, err := Start(ctx, PipeIO{R: down, W: up}, RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); waitDone(t, c) }()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); waitDone(t, s) }()
	waitHandoff := func() {
		t.Helper()
		select {
		case err := <-completed:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("HandlePipe still waiting on a live carrier")
		}
	}
	waitHandoff()
	waitHandoff()
	stop()
	if _, err := c.Ping(ctx); err != nil {
		t.Fatalf("bootstrap after handoff: %v", err)
	}

	// The pair stays reserved after both handlers return.
	dup := newCountingCarrier()
	if err := l.HandlePipe(ctx, PipeIO{R: dup}, PipeHint{Pair: token, Uplink: true}); !errors.Is(err, errRejected) {
		t.Fatalf("duplicate half: %v", err)
	}
	dup.checkOnce(t)

	joinCtx, stopJoin := context.WithCancel(ctx)
	defer stopJoin()
	a, b := net.Pipe()
	go func() { completed <- l.HandlePipe(joinCtx, NetPipe(b), PipeHint{}) }()
	if _, err := c.AddPipe(ctx, NetPipe(a), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	waitHandoff()
	stopJoin()
	boot, _ := c.Pipe(0)
	if err := boot.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ping(ctx); err != nil {
		t.Fatalf("joined pipe after handoff: %v", err)
	}
}

// Prefix replay leaves the close of a shared carrier on the write half,
// including for the record-framed carriers used by WebSocket.
func TestHandlePipeReplaySharesClose(t *testing.T) {
	for _, framing := range []Framing{FramingByteStream, FramingRecords} {
		cc := newCountingCarrier()
		p := cc.pipe()
		p.Framing = framing
		p = PrefixedPipe(p, []byte{1})
		_ = p.R.Close()
		if cc.closes.Load() != 0 {
			t.Fatalf("framing %v: the reader closed the shared carrier", framing)
		}
		_ = p.W.Close()
		cc.checkOnce(t)
	}
}

// Serve cancellation ends a pending setup and leaves the accepted session usable.
func TestServeCancelAfterHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ln := netx.NewChanListener(nil, 0)
	l := mustNewListener(nil)
	defer func() { _ = l.Close() }()
	serving, stop := context.WithCancel(ctx)
	defer stop()
	result := make(chan error, 1)
	go func() { result <- l.Serve(serving, ln) }()
	nc, err := ln.Dial(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Client(ctx, nc, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); waitDone(t, c) }()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); waitDone(t, s) }()
	pending, err := ln.Dial(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pending.Close() }()
	stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Serve did not stop")
	}
	deadline, _ := ctx.Deadline()
	_ = pending.SetReadDeadline(deadline)
	if _, err := pending.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("pending setup: %v", err)
	}
	if _, err := c.Ping(ctx); err != nil {
		t.Fatalf("accepted session: %v", err)
	}
}

// Take waits for the attached join even after its setup context is cancelled,
// then releases the runner while the bootstrap still carries the session.
func TestListenerTakeWaitsForJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	l := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = l.Close() }()
	a, b := net.Pipe()
	handed := make(chan error, 1)
	go func() { handed <- l.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
	c, err := Client(ctx, a, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); waitDone(t, c) }()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); waitDone(t, s) }()
	if err := <-handed; err != nil {
		t.Fatal(err)
	}
	arrival, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
	if err != nil {
		t.Fatal(err)
	}
	defer arrival.Release()
	a, b = net.Pipe()
	setup, stop := context.WithCancel(ctx)
	defer stop()
	run, err := arrival.Take(setup, NetPipe(b), PipeHint{})
	if err != nil {
		t.Fatal(err)
	}
	go func() { handed <- run(setup) }()
	p, err := c.AddPipe(ctx, NetPipe(a), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stop()
	if _, err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-handed:
		t.Fatalf("runner returned on live pipe: %v", err)
	default:
	}
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-handed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("runner waited for the entire session")
	}
	if _, err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
