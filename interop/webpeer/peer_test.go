package peer

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

type lockedBuf struct {
	b  bytes.Buffer
	mu sync.Mutex
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestServeEchoReportsLateJoin joins a pipe after the last echoed stream:
// MODE must still be printed.
func TestServeEchoReportsLateJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := &zmux.Config{MaxPipes: 2, PipeAuth: zmux.PipeAuth{PSK: []byte("an example pre-shared key, 32 B"), EndToEnd: true}}
	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	var out lockedBuf
	go serveEcho(ctx, l, &out)
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	go func() { _ = l.HandlePipe(ctx, zmux.NetPipe(b0), zmux.PipeHint{}) }()
	go func() { _ = l.HandlePipe(ctx, zmux.NetPipe(b1), zmux.PipeHint{}) }()
	c, err := zmux.Client(ctx, a0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = st.Write([]byte("ping"))
	_ = st.Close()
	for !strings.Contains(out.String(), "SESSION ping") {
		time.Sleep(5 * time.Millisecond)
		if ctx.Err() != nil {
			t.Fatal("no SESSION")
		}
	}
	if _, err := c.AddPipe(ctx, zmux.NetPipe(a1), zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	for !strings.Contains(out.String(), "MODE ") {
		time.Sleep(5 * time.Millisecond)
		if ctx.Err() != nil {
			t.Fatalf("no MODE after late join: %q", out.String())
		}
	}
}

func TestReadyReportsBoundAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	var out lockedBuf
	Ready(&out, ln)
	if want := "READY " + ln.Addr().String() + "\n"; out.String() != want || strings.HasSuffix(ln.Addr().String(), ":0") {
		t.Fatalf("ready line %q, want %q", out.String(), want)
	}
}
