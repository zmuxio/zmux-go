package seal_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
)

// halfPipe is a pair of connections built from two net.Pipes, one per
// direction, so that CloseWrite ends only the sending direction (as TCP does).
type halfConn struct{ r, w net.Conn }

func halfPipe() (net.Conn, net.Conn) {
	ar, bw := net.Pipe()
	br, aw := net.Pipe()
	return &halfConn{r: ar, w: aw}, &halfConn{r: br, w: bw}
}

func (c *halfConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *halfConn) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *halfConn) CloseWrite() error           { return c.w.Close() }
func (c *halfConn) LocalAddr() net.Addr         { return c.r.LocalAddr() }
func (c *halfConn) RemoteAddr() net.Addr        { return c.r.RemoteAddr() }
func (c *halfConn) SetDeadline(t time.Time) error {
	_ = c.r.SetReadDeadline(t)
	return c.w.SetWriteDeadline(t)
}
func (c *halfConn) SetReadDeadline(t time.Time) error  { return c.r.SetReadDeadline(t) }
func (c *halfConn) SetWriteDeadline(t time.Time) error { return c.w.SetWriteDeadline(t) }
func (c *halfConn) Close() error {
	_ = c.w.Close()
	return c.r.Close()
}

// The server sends a stream and FIN, then closes the session with a code;
// the client reads the stream, writes its own FIN as the close arrives, and
// must end with the server's code. A carrier without CloseWrite under the
// Sealed record layer must not be closed whole by the server's write shutdown.
func TestSealedCloseCodeDelivered(t *testing.T) {
	for _, tc := range []struct {
		pipe func() (net.Conn, net.Conn)
		name string
		mode zmux.RecordMode
	}{
		{name: "sealed/netpipe", mode: zmux.RecordSealed, pipe: net.Pipe},
		{name: "sealed/halfpipe", mode: zmux.RecordSealed, pipe: halfPipe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			f := newFixture(t)
			f.client.RecordMode = tc.mode
			lst, err := zmux.NewListener(&zmux.ListenerConfig{WrapPipe: &f.server})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lst.Close() }()
			a, b := tc.pipe()
			go func() { _ = lst.HandlePipe(ctx, zmux.NetPipe(b), zmux.PipeHint{}) }()
			p, err := seal.Establish(ctx, zmux.NetPipe(a), &f.client)
			if err != nil {
				t.Fatal(err)
			}
			cl, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{})
			if err != nil {
				t.Fatal(err)
			}
			sv, err := lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			recv := make(chan int, 1)
			go func() {
				s, err := cl.AcceptStream(ctx)
				if err != nil {
					recv <- -1
					return
				}
				b, _ := io.ReadAll(s)
				_ = s.Close()
				recv <- len(b)
			}()
			payload := bytes.Repeat([]byte("data"), 16<<10)
			s, err := sv.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(payload); err != nil {
				t.Fatal(err)
			}
			_ = s.Close()
			_ = sv.CloseWithError(0x4e500003, "")
			select {
			case <-cl.Done():
			case <-ctx.Done():
				t.Fatal("client did not end")
			}
			var ae *zmux.ApplicationError
			if err := cl.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 0x4e500003 || !ae.Remote {
				t.Fatalf("client end: %v", err)
			}
			if n := <-recv; n != len(payload) {
				t.Fatalf("payload %d", n)
			}
		})
	}
}
