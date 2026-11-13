package zmux

import (
	"context"
	"io"
	"testing"
	"time"
)

// SinceLastTraffic grows while only probes and their answers flow and
// restarts with stream data, on both endpoints.
func TestSinceLastTraffic(t *testing.T) {
	t.Parallel()
	cfg := func() *Config {
		c := mpConfig()
		c.Liveness.ProbeInterval = time.Second
		return c
	}
	e := newMPEnv(t, cfg(), cfg())
	cp, _ := e.client.Pipe(0)
	sp, _ := e.server.Pipe(0)
	time.Sleep(2500 * time.Millisecond)
	if a, b := pipeStat(t, cp).PingsSent, pipeStat(t, sp).PingsSent; a+b < 2 {
		t.Fatalf("%d and %d probes in 2.5 s at a 1 s interval", a, b)
	}
	for _, p := range []*Pipe{cp, sp} {
		st := pipeStat(t, p)
		if st.SinceLastTraffic < 2*time.Second {
			t.Fatalf("idle pipe: since last traffic %v", st.SinceLastTraffic)
		}
		if st.SinceLastTraffic <= st.SinceLastReceive {
			t.Fatalf("since last traffic %v not beyond since last receive %v", st.SinceLastTraffic, st.SinceLastReceive)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(s, s)
			_ = s.Close()
		}
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("traffic")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 7)
	if _, err := io.ReadFull(s, buf); err != nil {
		t.Fatal(err)
	}
	for _, p := range []*Pipe{cp, sp} {
		if st := pipeStat(t, p); st.SinceLastTraffic > 500*time.Millisecond {
			t.Fatalf("after stream data: since last traffic %v", st.SinceLastTraffic)
		}
	}
	_ = s.Close()
}
