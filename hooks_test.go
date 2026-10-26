package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestTracerSeesEstablishedAndClosed(t *testing.T) {
	leakcheck.Check(t)
	var mu sync.Mutex
	var got []string
	closed := make(chan error, 1)
	tr := &Tracer{
		Established: func() { mu.Lock(); got = append(got, "est"); mu.Unlock() },
		Closed:      func(err error) { closed <- err },
	}
	a, b := tcpPair(t)
	c, s := pair(t, a, b, &Config{Tracer: tr}, nil)
	_ = s
	c.CloseWithError(3, "bye")
	select {
	case err := <-closed:
		var ae *ApplicationError
		if err == nil || !errors.As(err, &ae) || ae.ErrorCode != 3 {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Closed not called")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("events %v", got)
	}
}

// pickSecond sends every chunk on the last pipe of the view.
type pickLast struct{ calls atomic.Int32 }

func (p *pickLast) Pick(reqs []PickRequest, pipes []PickPipe, out []PickAnswer) {
	p.calls.Add(1)
	for i := range out {
		out[i] = PickAnswer{Pipe: len(pipes) - 1}
	}
}

func TestCustomPolicy(t *testing.T) {
	leakcheck.Check(t)
	pol := &pickLast{}
	ccfg := mpConfig()
	ccfg.Scheduler = Custom(pol)
	e := newMPEnv(t, ccfg, mpConfig())
	p2 := e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err == nil {
			io.Copy(io.Discard, s)
		}
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 256<<10)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := p2.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if st.BytesSent > 200<<10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipe 2 carried %d bytes; policy called %d times", st.BytesSent, pol.calls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRedialAfterSuspension(t *testing.T) {
	leakcheck.Check(t)
	var ln net.Listener
	ccfg := mpConfig()
	ccfg.ResumeTimeout = 5 * time.Second
	var dials atomic.Int32
	ccfg.Redial = func(ctx context.Context, need Direction) (PipeIO, error) {
		dials.Add(1)
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return PipeIO{}, err
		}
		return NetPipe(nc), nil
	}
	scfg := mpConfig()
	scfg.ResumeTimeout = 5 * time.Second
	e := newMPEnv(t, ccfg, scfg)
	ln = e.ln
	// Break pipe 0 under the session: both directions are lost, the
	// session suspends and the hook rejoins.
	pd := e.client.pipes[0]
	pd.closeAll(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		ev, err := e.client.NextPipeEvent(ctx)
		if err != nil {
			t.Fatalf("no resumption: %v (dials %d)", err, dials.Load())
		}
		if ev.Kind == SessionResumed {
			break
		}
	}
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err == nil {
			io.Copy(s, s)
			s.Close()
		}
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { s.Write([]byte("after")); s.Close() }()
	b, err := io.ReadAll(s)
	if err != nil || string(b) != "after" {
		t.Fatalf("echo after resume %q %v", b, err)
	}
}
