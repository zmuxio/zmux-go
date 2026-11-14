package zmuxquic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// TestCloseDelivery (N16) over QUIC, through Dial and through a pool
// connection (Candidate's): the peer reads the whole 1 MiB written before
// a coded close, ends with the code, and its QUIC connection ends with the
// same application code only after that; the plain close gives code 0.
func TestCloseDelivery(t *testing.T) {
	for _, via := range []string{"dial", "pool"} {
		for _, c := range []struct {
			code   zmux.ApplicationErrorCode
			reason string
		}{{0x2a, "bye"}, {0, ""}} {
			t.Run(fmt.Sprintf("%s/%#x", via, uint64(c.code)), func(t *testing.T) {
				closeDelivery(t, via, c.code, c.reason)
			})
		}
	}
}

func closeDelivery(t *testing.T, via string, code zmux.ApplicationErrorCode, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	e := newEnv(t, nil)
	var client *zmux.Conn
	var err error
	switch via {
	case "dial":
		client, err = Dial(ctx, e.ln.Addr().String(), e.cli, nil, nil)
	default:
		p := NewConnPool(e.ln.Addr().String(), e.cli, nil, nil, carrier.PoolLimits{MaxPipesPerConnectionLifetime: 1})
		var pio zmux.PipeIO
		if pio, err = p.OpenPipe(ctx); err == nil {
			client, err = zmux.Start(ctx, pio, zmux.RoleClient, nil)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	server, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sqc := e.tl.last()
	msg := make([]byte, 1<<20)
	for i := range msg {
		msg[i] = byte(i * 7)
	}
	read := make(chan error, 1)
	go func() {
		in, err := server.AcceptStream(ctx)
		if err == nil {
			var got []byte
			got, err = io.ReadAll(in)
			if err == nil && !bytes.Equal(got, msg) {
				err = fmt.Errorf("read %d bytes, not the 1 MiB written", len(got))
			}
		}
		read <- err
	}()
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(msg); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	open, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open.Write([]byte("left open")); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_ = client.CloseWithError(code, reason)
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("the close took %v", d)
	}
	t1 := time.Now()
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the close was neither delivered nor given up within 5 s")
	}
	if d := time.Since(t1); d >= 250*time.Millisecond {
		t.Fatalf("the wait took %v: the linger timeout, not the peer's end", d)
	}
	if err := <-read; err != nil {
		t.Fatalf("the peer's read: %v", err)
	}
	var ae *zmux.ApplicationError
	if err := server.Wait(); !errors.As(err, &ae) || ae.ErrorCode != code || ae.ErrorMessage != reason || !ae.Remote {
		t.Fatalf("the peer's session ended with %v", err)
	}
	if _, err := open.Write([]byte("x")); !errors.As(err, &ae) || ae.ErrorCode != code || ae.Remote {
		t.Fatalf("the open stream after the close: %v", err)
	}
	select {
	case <-sqc.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the QUIC connection was not closed")
	}
	var qe *quic.ApplicationError
	if err := context.Cause(sqc.Context()); !errors.As(err, &qe) || !qe.Remote || qe.ErrorCode != quic.ApplicationErrorCode(code) {
		t.Fatalf("the peer's QUIC connection ended with %v, want application code %#x", err, uint64(code))
	}
}

// pipeRelease withdraws its registration on the connection's context once
// the pipe ended: the connection's end calls release no more.
func TestPipeReleaseWithdraws(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int32
	ended := pipeRelease(ctx, func() { n.Add(1) })
	ended()
	cancel()
	time.Sleep(20 * time.Millisecond)
	if got := n.Load(); got != 1 {
		t.Fatalf("release called %d times", got)
	}
}
