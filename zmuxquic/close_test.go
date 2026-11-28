package zmuxquic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// TestCloseDelivery (N16) over QUIC, through Dial and through a pool
// connection (Candidate's): the peer reads the whole stream written before
// a coded close, ends with the code, and its QUIC connection ends with the
// same application code only after that; the plain close gives code 0.
func TestCloseDelivery(t *testing.T) {
	for _, via := range []string{"dial", "pool"} {
		for _, c := range []struct {
			reason string
			code   zmux.ApplicationErrorCode
		}{{code: 0x2a, reason: "bye"}, {code: 0, reason: ""}} {
			t.Run(fmt.Sprintf("%s/%#x", via, uint64(c.code)), func(t *testing.T) {
				closeDelivery(t, via, c.code, c.reason)
			})
		}
	}
}

func closeDelivery(t *testing.T, via string, code zmux.ApplicationErrorCode, reason string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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
	const prefix = 4 << 10
	msg := make([]byte, 2*prefix)
	for i := range msg {
		msg[i] = byte(i * 7)
	}
	read := make(chan error, 1)
	accepted := make(chan error, 1)
	go func() {
		in, err := server.AcceptStream(ctx)
		got := make([]byte, prefix)
		if err == nil {
			_, err = io.ReadFull(in, got)
		}
		accepted <- err
		if err != nil {
			return
		}
		tail, err := io.ReadAll(in)
		if err != nil || !bytes.Equal(got, msg[:prefix]) || !bytes.Equal(tail, msg[prefix:]) {
			if err == nil {
				err = errors.New("payload differs")
			}
			err = fmt.Errorf("read %d of %d bytes: %w", len(got)+len(tail), len(msg), err)
		}
		read <- err
	}()
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(msg[:prefix]); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := st.Write(msg[prefix:]); err != nil {
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
	_ = client.CloseWithError(code, reason)
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("the peer's read: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-server.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
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
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var qe *quic.ApplicationError
	if err := context.Cause(sqc.Context()); !errors.As(err, &qe) || !qe.Remote || qe.ErrorCode != quic.ApplicationErrorCode(code) {
		t.Fatalf("the peer's QUIC connection ended with %v, want application code %#x", err, uint64(code))
	}
}
