package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// mpEnv is a listener on TCP and a client session dialled to it.
type mpEnv struct {
	ln     net.Listener
	lst    *Listener
	client *Conn
	server *Conn
	cancel context.CancelFunc
}

func mpConfig() *Config {
	return &Config{MaxPipes: 4, PipeAuth: PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)},
		SchedulerParams: SchedulerParams{Probation: new(time.Duration)}}
}

func newMPEnv(t *testing.T, ccfg, scfg *Config) *mpEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &mpEnv{ln: ln, lst: NewListener(&ListenerConfig{Config: scfg}), cancel: cancel}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = e.lst.HandlePipe(ctx, NetPipe(c), JoinHint{}) }()
		}
	}()
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	e.client, err = Dial(dctx, nc, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	e.server, err = e.lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.client.Close()
		_ = e.server.Close()
		waitDone(t, e.client)
		waitDone(t, e.server)
		_ = ln.Close()
		_ = e.lst.Close()
		cancel()
	})
	return e
}

func (e *mpEnv) addPipe(t *testing.T, o PipeOptions) *Pipe {
	t.Helper()
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := e.client.AddPipe(ctx, NetPipe(nc), o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMultipathTransferAndFailover(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	if st := e.client.ConnectionState(); !st.Multipath {
		t.Fatal("multipath not negotiated")
	}
	p2 := e.addPipe(t, PipeOptions{})
	e.client.SetScheduler(SpreadRoundRobin())

	payload := make([]byte, 4<<20)
	rand.Read(payload)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan []byte, 1)
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err != nil {
			done <- nil
			return
		}
		b, _ := io.ReadAll(s)
		done <- b
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	half := len(payload) / 2
	if _, err := s.Write(payload[:half]); err != nil {
		t.Fatal(err)
	}
	// Both pipes carry data; then the added pipe fails in the middle and
	// its unacknowledged data is reinjected.
	time.Sleep(20 * time.Millisecond)
	if st, err := p2.Stats(); err != nil || st.BytesSent < 1<<16 {
		t.Fatalf("pipe 2 carried too little: %+v %v", st, err)
	}
	if err := p2.Abort(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(payload[half:]); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	got := <-done
	if !bytes.Equal(got, payload) {
		t.Fatalf("received %d bytes, want %d intact", len(got), len(payload))
	}
	var used int
	for _, ps := range e.client.Pipes() {
		if ps.BytesSent > 1<<16 {
			used++
		}
	}
	if used < 1 {
		t.Fatalf("no pipe carried data: %+v", e.client.Pipes())
	}
}

func TestMultipathEventsAndStats(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	w := uint64(3)
	p := e.addPipe(t, PipeOptions{Weight: &w, Label: "second"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := map[PipeEventKind]bool{}
	for !seen[PipeJoined] {
		ev, err := e.client.NextPipeEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen[ev.Kind] = true
	}
	st, err := p.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Label != "second" || st.Weight != 3 || st.State != "active" {
		t.Fatalf("stats %+v", st)
	}
	if err := p.SetSendPriority(5); err != nil {
		t.Fatal(err)
	}
	for {
		ev, err := e.client.NextPipeEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Kind == PipePriorityChanged {
			if ev.Pipe != p.ID() || ev.Priority != 5 {
				t.Fatalf("event %+v", ev)
			}
			break
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		ev, err := e.client.NextPipeEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Kind == PipeClosed && ev.Pipe == p.ID() {
			break
		}
	}
}
