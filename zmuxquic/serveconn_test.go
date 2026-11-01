package zmuxquic

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

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
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), InstanceID: []byte("instance")})
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
		go func() { served <- ServeConn(sctx, qc, lst, nil) }()
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
	// (its pipe is aborted, as Serve's are); the connection and the
	// session continue.
	sctx, scancel := context.WithCancel(ctx)
	q2 := connect(sctx)
	defer func() { _ = q2.CloseWithError(0, "") }()
	pio, err := OpenPipe(ctx, q2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, pio, zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 1<<16)
	if n := len(s.PipeStats()); n != 2 {
		t.Fatalf("server: %d pipes, want 2", n)
	}
	scancel()
	if err := <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeConn after its context ended: %v", err)
	}
	echo(t, c, s, 1<<16) // the session continues
	if q2.Context().Err() != nil {
		t.Fatal("ServeConn closed the connection when its context ended")
	}
	// The first connection's ServeConn returns nil once it ends.
	_ = c.Close()
	_ = q1.CloseWithError(0, "")
	if err := <-served; err != nil {
		t.Fatalf("ServeConn after the connection ended: %v", err)
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
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig()})
	_ = lst.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := ServeConn(ctx, qc, lst, nil); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("ServeConn on a closed listener: %v", err)
	}
	select {
	case <-qc.Context().Done():
	case <-ctx.Done():
		t.Fatal("refused connection still open")
	}
}
