package zmux

import (
	"context"
	"net"
	"testing"
	"time"
)

// twoPipeSession returns a client and server session with a standby pipe
// joined to pipe 0.
func twoPipeSession() (*Conn, *Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: []byte("an example pre-shared key, 32 B")}}
	l := NewListener(&ListenerConfig{Config: cfg})
	defer func() { _ = l.Close() }()
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	go func() { _ = l.HandlePipe(ctx, NetPipe(b0), JoinHint{}) }()
	go func() { _ = l.HandlePipe(ctx, NetPipe(b1), JoinHint{}) }()
	client, err := Client(ctx, a0, cfg)
	if err != nil {
		return nil, nil, err
	}
	server, err := l.Accept(ctx)
	if err != nil {
		return nil, nil, err
	}
	standby := true
	p, err := client.AddPipe(ctx, NetPipe(a1), PipeOptions{Label: "backup", SendStandby: &standby})
	if err != nil {
		return client, server, err
	}
	for {
		ev, err := client.NextEvent(ctx)
		if err != nil {
			return client, server, err
		}
		if ev.Kind == EventPipeJoined && ev.Pipe == p.ID() {
			break
		}
	}
	return client, server, nil
}

// TestCloseWhileJoinedPipeFails closes both ends of a two-pipe session at
// once, many times: a pipe that fails before it carried CONNECTION_CLOSE
// must not leave the other end CLOSING for ever.
func TestCloseWhileJoinedPipeFails(t *testing.T) {
	for i := 0; i < 200; i++ {
		c, s, err := twoPipeSession()
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		_ = s.Close()
		done := make(chan struct{})
		go func() { _ = c.Wait(); _ = s.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d: a session did not end after Close", i)
		}
	}
}
