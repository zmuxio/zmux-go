package peer

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

type outputLines chan string

func (l outputLines) Write(p []byte) (int, error) {
	l <- string(p)
	return len(p), nil
}

func (l outputLines) expect(t *testing.T, ctx context.Context, prefix string) {
	t.Helper()
	for {
		select {
		case line := <-l:
			if strings.HasPrefix(line, prefix) {
				return
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %q: %v", prefix, ctx.Err())
		}
	}
}

// TestServeEchoReportsLateJoin joins a pipe after the last echoed stream:
// MODE must still be printed.
func TestServeEchoReportsLateJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 2}}, PipeAuth: zmux.PipeAuth{PSK: []byte("an example pre-shared key, 32 B"), EndToEnd: true}}
	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	out := make(outputLines, 4)
	done := make(chan struct{})
	go func() { serveEcho(ctx, l, out); close(done) }()
	defer func() { cancel(); <-done }()

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
	out.expect(t, ctx, "SESSION ping")
	if _, err := c.AddPipe(ctx, zmux.NetPipe(a1), zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	out.expect(t, ctx, "MODE ")
}

func TestReadyReportsBoundAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	var out bytes.Buffer
	Ready(&out, ln)
	if want := "READY " + ln.Addr().String() + "\n"; out.String() != want || strings.HasSuffix(ln.Addr().String(), ":0") {
		t.Fatalf("ready line %q, want %q", out.String(), want)
	}
}
