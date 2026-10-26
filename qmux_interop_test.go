package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// Interoperability with a plain QMux peer (no zmux parameters), a binary
// named by ZMUX_QMUX_PEER taking "server addr" or "client addr"; a
// bidirectional stream carries its command on its first line.

func TestQMuxInteropClient(t *testing.T) {
	p := startPeerEnv(t, "ZMUX_QMUX_PEER", "server", "127.0.0.1:0")
	addr := p.expect(t, "LISTEN ")
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(ctx, nc, &Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	qmuxScript(t, ctx, c, p)
}

func TestQMuxInteropServer(t *testing.T) {
	if os.Getenv("ZMUX_QMUX_PEER") == "" {
		t.Skip("ZMUX_QMUX_PEER not set")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	p := startPeerEnv(t, "ZMUX_QMUX_PEER", "client", ln.Addr().String())
	nc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Server(ctx, nc, &Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	qmuxScript(t, ctx, c, p)
}

func qmuxCommand(t *testing.T, ctx context.Context, c *Conn, cmd string, body []byte) string {
	t.Helper()
	return command(t, ctx, c, "", append([]byte(cmd+"\n"), body...))
}

// uniReport reads the next unidirectional stream the peer opens.
func uniReport(t *testing.T, ctx context.Context, c *Conn) string {
	t.Helper()
	rs, err := c.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rs)
	return string(b)
}

func qmuxScript(t *testing.T, ctx context.Context, c *Conn, p *peerProc) {
	p.expect(t, "ESTABLISHED")
	big := make([]byte, 8<<20)
	for i := range big {
		big[i] = byte(i * 7)
	}
	if r := qmuxCommand(t, ctx, c, "echo", big); r != string(big) {
		t.Fatalf("echo: %d bytes", len(r))
	}
	if r := qmuxCommand(t, ctx, c, "count", big[:100000]); r != "count=100000" {
		t.Fatalf("count: %q", r)
	}
	// Our reset reaches the peer with the bytes delivered before it.
	{
		st, _ := c.OpenStreamSync(ctx)
		st.Write([]byte("count\nabc"))
		st.SetReliableBoundary()
		st.CancelWrite(0x3fffffff)
		// With reset_stream_at the bytes before the reset are counted too.
		if r := uniReport(t, ctx, c); r != "reset:1073741823" && !strings.HasPrefix(r, "reset:1073741823:") {
			t.Fatalf("reset report: %q", r)
		}
		st.CancelRead(0)
	}
	if r := qmuxCommand(t, ctx, c, "stop:9", nil); r != "stopped" {
		t.Fatalf("stop: %q", r)
	}
	// Our STOP_SENDING stops the peer's flood.
	{
		st, _ := c.OpenStreamSync(ctx)
		st.Write([]byte("flood\n"))
		st.Close()
		buf := make([]byte, 4096)
		if _, err := io.ReadAtLeast(st, buf, 2048); err != nil {
			t.Fatal(err)
		}
		st.CancelRead(12)
		if r := uniReport(t, ctx, c); r != "stopped:12" {
			t.Fatalf("flood: %q", r)
		}
	}
	// The peer's reset.
	{
		st, _ := c.OpenStreamSync(ctx)
		st.Write([]byte("reset:33\n"))
		st.Close()
		_, err := io.ReadAll(st)
		var se *StreamError
		if !errors.As(err, &se) || se.ErrorCode != 33 {
			t.Fatalf("reset: %v", err)
		}
	}
	// Streams the peer opens.
	go func() {
		st, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		b, _ := io.ReadAll(st)
		st.Write(append([]byte("re:"), b...))
		st.Close()
	}()
	if r := qmuxCommand(t, ctx, c, "open-bidi:hi", nil); r != "opened=re:hi" {
		t.Fatalf("open-bidi: %q", r)
	}
	if r := qmuxCommand(t, ctx, c, "open-uni:u", nil); r != "sent" {
		t.Fatalf("open-uni: %q", r)
	}
	if r := uniReport(t, ctx, c); r != "u" {
		t.Fatalf("uni: %q", r)
	}
	// Unidirectional echo and datagrams.
	us, _ := c.OpenUniStreamSync(ctx)
	us.Write([]byte("ue"))
	us.Close()
	if r := uniReport(t, ctx, c); r != "ue" {
		t.Fatalf("uni echo: %q", r)
	}
	if err := c.SendDatagram([]byte("dg")); err != nil {
		t.Fatal(err)
	}
	if d, err := c.ReceiveDatagram(ctx); err != nil || string(d) != "dg" {
		t.Fatalf("datagram: %q %v", d, err)
	}
	// QX_PING round trip.
	if _, err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if r := qmuxCommand(t, ctx, c, "close:42:bye", nil); r != "closing" {
		t.Fatalf("close: %q", r)
	}
	var ae *ApplicationError
	if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 42 || ae.ErrorMessage != "bye" {
		t.Fatalf("close: %v", err)
	}
}
