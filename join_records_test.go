package zmux_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// A joined pipe takes the framing of its carrier: records over a message carrier.
func TestJoinOverMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := func() *zmux.Config {
		return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true}}
	}
	l := mustNewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = l.Close() }()
	ca, sa := memWebSocketPair()
	go func() { _ = l.HandlePipe(ctx, wsPipe(sa), zmux.PipeHint{}) }()
	c, err := zmux.Start(ctx, wsPipe(ca), zmux.RoleClient, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go echo(ctx, s)
	cb, sb := memWebSocketPair()
	go func() { _ = l.HandlePipe(ctx, wsPipe(sb), zmux.PipeHint{}) }()
	if _, err := c.AddPipe(ctx, wsPipe(cb), zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	msg := strings.Repeat("x", 256<<10)
	for range 4 {
		if got, err := roundTrip(ctx, c, msg); err != nil || got != msg {
			t.Fatalf("round trip: %d bytes, %v", len(got), err)
		}
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}
