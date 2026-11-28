package zmux

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"
)

// feed runs a session over one end of a pipe and writes data into the
// other: the session must end (no panic, no hang) once the input ends.
func feed(t *testing.T, data []byte, serve func(ctx context.Context, p PipeIO) (*Conn, error)) {
	t.Helper()
	a, b := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, b) }()
	go func() {
		_, _ = b.Write(data)
		_ = b.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := serve(ctx, NetPipe(a))
	if err != nil {
		_ = a.Close()
		return
	}
	go func() {
		for {
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(st, st); _ = st.Close() }()
		}
	}()
	go func() {
		for {
			if _, err := c.ReceiveDatagram(ctx); err != nil {
				return
			}
		}
	}()
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = c.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("the session did not end after Close")
		}
	}
}

func FuzzServerInput(f *testing.F) {
	cfg := &Config{EnableDatagrams: true, MaxIdleTimeout: 2 * time.Second}
	f.Fuzz(func(t *testing.T, data []byte) {
		feed(t, data, func(ctx context.Context, p PipeIO) (*Conn, error) {
			return Start(ctx, p, RoleServer, cfg)
		})
	})
}

func FuzzListenerInput(f *testing.F) {
	cfg := &Config{EnableDatagrams: true, SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 4}}, PipeAuth: PipeAuth{PSK: []byte("a pre-shared key of 32 bytes...."), EndToEnd: true},
		MaxIdleTimeout: 2 * time.Second}
	f.Fuzz(func(_ *testing.T, data []byte) {
		l := mustNewListener(&ListenerConfig{Config: cfg, FirstRecordTimeout: 500 * time.Millisecond})
		defer func() { _ = l.Close() }()
		a, b := net.Pipe()
		go func() { _, _ = io.Copy(io.Discard, b) }()
		go func() {
			_, _ = b.Write(data)
			_ = b.Close()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			if c, err := l.Accept(ctx); err == nil {
				_ = c.Close()
			}
		}()
		_ = l.HandlePipe(ctx, NetPipe(a), PipeHint{Source: netip.MustParseAddrPort("192.0.2.1:1")})
	})
}
