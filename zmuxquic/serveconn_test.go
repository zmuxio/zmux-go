package zmuxquic

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"
	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// TestServeConnDialled serves connections that the listening side dialled,
// as hole punching gives them: the peer that accepted them starts a
// session on the first and joins it with a pipe on the second.
func TestServeConnDialled(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	var block atomic.Bool
	pending := make(chan struct{})
	ended := make(chan error, 1)
	wrap := zmux.WrapPipeFunc(func(ctx context.Context, p zmux.PipeIO, _ zmux.PipeHint) (zmux.PipeIO, error) {
		if block.CompareAndSwap(true, false) {
			close(pending)
			<-ctx.Done()
			ended <- ctx.Err()
			return zmux.PipeIO{}, ctx.Err()
		}
		return p, nil
	})
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), WrapPipe: wrap, InstanceID: []byte("instance")})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	served := make(chan error, 2)
	connect := func(sctx context.Context) *quic.Conn {
		t.Helper()
		type res struct {
			qc  *quic.Conn
			err error
		}
		acc := make(chan res, 1)
		go func() {
			qc, err := ln.Accept(ctx)
			acc <- res{qc, err}
		}()
		qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
		if err != nil {
			t.Fatal(err)
		}
		go func() { served <- ServeConn(sctx, lst, qc) }()
		r := <-acc
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r.qc
	}
	q1 := connect(ctx)
	defer func() { _ = q1.CloseWithError(0, "") }()
	c, err := Client(ctx, q1, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// The second connection stops being served when its context ends
	// while its joined pipe continues to carry traffic.
	sctx, scancel := context.WithCancel(ctx)
	q2 := connect(sctx)
	defer func() { _ = q2.CloseWithError(0, "") }()
	pio, err := ConnPipe(ctx, q2)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := c.AddPipe(ctx, pio, zmux.PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := joined.WaitAttached(ctx); err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 1<<16)
	if n := len(s.PipeStats()); n != 2 {
		t.Fatalf("server: %d pipes, want 2", n)
	}
	// Stop only the listener while a further stream on q2 is in setup.
	// The caller holds one ServeConn context for the entire connection.
	block.Store(true)
	extra, err := q2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = extra.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pending:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_ = lst.Close()
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("listener stop left setup pending")
	}
	extra.CancelRead(0)
	_ = extra.Close()
	scancel()
	boot, _ := c.Pipe(0)
	if err := boot.Abort(); err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 1<<16) // the session continues
	if q2.Context().Err() != nil {
		t.Fatal("ServeConn closed the connection when its context ended")
	}
	_ = c.Close()
	_ = q1.CloseWithError(0, "")
	for range 2 { // ServeConn returned once each connection was handed on
		if err := <-served; err != nil {
			t.Fatalf("ServeConn: %v", err)
		}
	}
}

// TestServeConnRefused closes a connection that a closed listener
// refuses, and reports net.ErrClosed.
func TestServeConnRefused(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	_ = lst.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := ServeConn(ctx, lst, qc); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("ServeConn on a closed listener: %v", err)
	}
	select {
	case <-qc.Context().Done():
	case <-ctx.Done():
		t.Fatal("refused connection still open")
	}
}

// HS-53, HS-57.
func TestQUICFirstReservationEndsWithServeScope(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, end := range []string{"context", "listener", "deadline", "connection"} {
		t.Run(end, func(t *testing.T) {
			qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = qc.CloseWithError(0, "") }()
			peer, err := ln.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = peer.CloseWithError(0, "") }()
			clk := timeshim.NewFake(time.Now())
			l := mustNewListener(&zmux.ListenerConfig{MaxPending: 1, Config: &zmux.Config{ClockDomain: zmux.NewClockDomain(clk), HandshakeTimeout: time.Second}})
			setup, stop := context.WithCancel(ctx)
			service, err := AdoptConn(setup, l, peer)
			if err != nil {
				t.Fatal(err)
			}
			if err := clk.WaitDeadline(ctx, clk.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			switch end {
			case "listener":
				_ = l.Close()
			case "deadline":
				clk.Advance(time.Second)
			case "connection":
				_ = qc.CloseWithError(0, "")
			default:
				stop()
			}
			if err := service.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if end != "connection" && qc.Context().Err() != nil {
				t.Fatal("accept stopped the connection")
			}
			if end != "listener" {
				next, err := l.Adapter().Arrive(netip.AddrPort{}, addrPort(peer.RemoteAddr()))
				if err != nil {
					t.Fatal(err)
				}
				next.Release()
			}
			stop()
			_ = l.Close()
		})
	}
}
