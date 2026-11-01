package zmux_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// A pipe reserved for an out-of-band join takes the framing of the
// carrier it is attached to: records over a message carrier.
func TestReservedPipeOverMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := func() *zmux.Config {
		return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)}}
	}
	l := zmux.NewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = l.Close() }()
	ca, sa := memWebSocketPair()
	go func() { _ = l.HandlePipe(ctx, wsPipe(sa), zmux.JoinHint{}) }()
	c, err := zmux.Start(ctx, wsPipe(ca), zmux.RoleClient, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go echo(ctx, s)
	r, err := c.ReservePipe(zmux.DirBidirectional, zmux.PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cb, sb := memWebSocketPair()
	go func() { _ = l.HandlePipe(ctx, wsPipe(sb), zmux.JoinHint{Join: r.Header()}) }()
	if _, err := r.Attach(ctx, wsPipe(cb)); err != nil {
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
