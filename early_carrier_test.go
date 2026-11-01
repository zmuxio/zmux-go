package zmux

import (
	"bytes"
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

// earlyLayer is a client carrier layer with a first flight: what is
// written before the verdict is held, sent on acceptance and dropped on
// rejection, with whatever comes until RestartEarly (the server never sees
// it), as a transform whose first message the server may refuse does.
type earlyLayer struct {
	net.Conn
	verdict  chan bool
	mu       sync.Mutex
	state    int // 0 holding, 1 passing, 2 dropping
	held     [][]byte
	dropped  atomic.Int64
	restarts atomic.Int32
	late     bool // no early data this time: writes pass from the start
	waits    atomic.Int32
}

func (l *earlyLayer) SendsEarlyData() bool { return !l.late }

func (l *earlyLayer) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case 0:
		l.held = append(l.held, bytes.Clone(b))
		return len(b), nil
	case 2:
		l.dropped.Add(int64(len(b)))
		return len(b), nil
	}
	return l.Conn.Write(b)
}

// decide is the server's verdict on the first flight.
func (l *earlyLayer) decide(accept bool) {
	l.mu.Lock()
	for _, b := range l.held {
		if accept {
			_, _ = l.Conn.Write(b)
		} else {
			l.dropped.Add(int64(len(b)))
		}
	}
	l.held = nil
	l.state = 2
	if accept {
		l.state = 1
	}
	l.mu.Unlock()
	l.verdict <- accept
}

func (l *earlyLayer) EarlyAccepted(ctx context.Context) (bool, error) {
	l.waits.Add(1)
	select {
	case ok := <-l.verdict:
		return ok, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// RestartEarly returns the same carrier past its handshake: writes now go
// through.
func (l *earlyLayer) RestartEarly(context.Context) (PipeIO, error) {
	l.restarts.Add(1)
	l.mu.Lock()
	l.state = 1
	l.mu.Unlock()
	return PipeIO{R: l.Conn, W: l}, nil
}

func earlyFloorConfig() *Config {
	cfg := mpConfig()
	cfg.EarlyData = EarlyData{ZmuxPeer: true, Replayable: true}
	return cfg
}

// Floor early data waits for the carrier's verdict: accepted, the first
// flight stands; rejected, nothing of it reached the peer, the pipe
// restarts on what RestartEarly returned, and the early stream's data is
// sent again from offset 0 after the parameters, without failing.
func TestEarlyCarrierFloor(t *testing.T) {
	for _, accept := range []bool{true, false} {
		t.Run(map[bool]string{true: "accepted", false: "rejected"}[accept], func(t *testing.T) {
			leakcheck.Check(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			a, b := net.Pipe()
			layer := &earlyLayer{Conn: a, verdict: make(chan bool, 1)}
			type res struct {
				c   *Conn
				err error
			}
			ch := make(chan res, 1)
			c, err := Start(ctx, PipeIO{R: a, W: layer}, RoleClient, earlyFloorConfig())
			if err != nil {
				t.Fatal(err)
			}
			msg := []byte("early request")
			st, err := c.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.Write(msg); err != nil {
				t.Fatal(err)
			}
			_ = st.Close()
			waitUntil(t, 5*time.Second, "the first flight", func() bool {
				layer.mu.Lock()
				defer layer.mu.Unlock()
				return len(layer.held) > 0
			})
			// The server speaks only once the carrier decided, as one
			// behind such a carrier does.
			if !accept {
				layer.decide(false)
				waitUntil(t, 5*time.Second, "the restart", func() bool { return layer.restarts.Load() == 1 })
			}
			go func() {
				s, err := Start(ctx, NetPipe(b), RoleServer, mpConfig())
				ch <- res{s, err}
			}()
			if accept {
				go layer.decide(true)
			}
			r := <-ch
			if r.err != nil {
				t.Fatal(r.err)
			}
			s := r.c
			defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
			ss, err := s.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(ss)
			if err != nil || !bytes.Equal(got, msg) {
				t.Fatalf("server read %q, %v", got, err)
			}
			if accept != (layer.restarts.Load() == 0) || accept != (layer.dropped.Load() == 0) {
				t.Fatalf("restarts %d, dropped %d", layer.restarts.Load(), layer.dropped.Load())
			}
			echoOnce(t, ctx, c, s)
		})
	}
}

// Floor early data on a carrier with early data needs the client's consent
// to replay.
func TestEarlyCarrierNeedsReplayable(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	cfg := mpConfig()
	cfg.EarlyData = EarlyData{ZmuxPeer: true}
	_, err := Start(context.Background(), PipeIO{R: a, W: &earlyLayer{Conn: a}}, RoleClient, cfg)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error %v", err)
	}
}

// Floor early data and the carrier's own early data in each combination:
// the verdict is awaited, and Replayable required, only when the carrier
// sends early data this time; a carrier with the capability that sends
// nothing early (QUIC without 0-RTT) behaves as one without it.
func TestEarlyCarrierFloorCombinations(t *testing.T) {
	for _, x := range []struct {
		name               string
		carrier            int // 0 none, 1 capable but not early, 2 early
		replayable, refuse bool
		waits              int32
	}{
		{"plain", 0, false, false, 0},
		{"plain replayable", 0, true, false, 0},
		{"not early", 1, false, false, 0},
		{"not early replayable", 1, true, false, 0},
		{"early", 2, false, true, 0},
		{"early replayable", 2, true, false, 1},
	} {
		t.Run(x.name, func(t *testing.T) {
			leakcheck.Check(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			a, b := net.Pipe()
			cp := NetPipe(a)
			var layer *earlyLayer
			if x.carrier > 0 {
				layer = &earlyLayer{Conn: a, verdict: make(chan bool, 1), late: x.carrier == 1}
				if layer.late {
					layer.state = 1
				}
				cp = PipeIO{R: a, W: layer}
			}
			cfg := mpConfig()
			cfg.EarlyData = EarlyData{ZmuxPeer: true, Replayable: x.replayable}
			c, err := Start(ctx, cp, RoleClient, cfg)
			if x.refuse {
				_ = a.Close()
				_ = b.Close()
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("error %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			ch := make(chan *Conn, 1)
			go func() {
				s, err := Start(ctx, NetPipe(b), RoleServer, mpConfig())
				if err != nil {
					t.Error(err)
				}
				ch <- s
			}()
			if layer != nil && !layer.late {
				go layer.decide(true)
			}
			s := <-ch
			if s == nil {
				_ = c.Close()
				return
			}
			defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
			echoOnce(t, ctx, c, s)
			if x.waits > 0 {
				waitUntil(t, 5*time.Second, "the verdict", func() bool { return layer.waits.Load() == x.waits })
			}
			if layer != nil && (layer.waits.Load() != x.waits || layer.restarts.Load() != 0) {
				t.Fatalf("waits %d, restarts %d", layer.waits.Load(), layer.restarts.Load())
			}
		})
	}
}

// earlyServer is a server carrier that received early data.
type earlyServer struct {
	net.Conn
	done chan struct{}
}

func (e *earlyServer) UsedEarlyData() bool                { return true }
func (e *earlyServer) HandshakeComplete() <-chan struct{} { return e.done }

// On a server with Replayable, streams opened before the carrier's
// handshake completed are replayable, later ones are not.
func TestEarlyServerReplayable(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	es := &earlyServer{Conn: b, done: make(chan struct{})}
	scfg := mpConfig()
	scfg.EarlyData.Replayable = true
	ch := make(chan *Conn, 1)
	go func() {
		s, err := Start(ctx, PipeIO{R: b, W: es}, RoleServer, scfg)
		if err != nil {
			t.Error(err)
		}
		ch <- s
	}()
	c, err := Start(ctx, NetPipe(a), RoleClient, earlyFloorConfig())
	if err != nil {
		t.Fatal(err)
	}
	s := <-ch
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	open := func() *Stream {
		st, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		ss, err := s.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return ss
	}
	if !open().Replayable() {
		t.Fatal("stream before the handshake is not replayable")
	}
	close(es.done)
	waitUntil(t, 5*time.Second, "handshake done", func() bool { return !open().Replayable() })
}

// A STOP_SENDING and a window update sent in rejected carrier early data
// go again after the restart, behind the re-issued opening STREAM frame.
func TestEarlyCarrierRejectedStop(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	layer := &earlyLayer{Conn: a, verdict: make(chan bool, 1)}
	c, err := Start(ctx, PipeIO{R: a, W: layer}, RoleClient, earlyFloorConfig())
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("early request")
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(msg); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	waitUntil(t, 5*time.Second, "the first flight", func() bool {
		layer.mu.Lock()
		defer layer.mu.Unlock()
		return len(layer.held) > 0
	})
	st.SetReceiveWindow(1<<22, 1<<22)
	st.CancelRead(5)
	time.Sleep(20 * time.Millisecond) // let the frames join the held flight
	layer.decide(false)
	waitUntil(t, 5*time.Second, "the restart", func() bool { return layer.restarts.Load() == 1 })
	ch := make(chan *Conn, 1)
	go func() {
		s, err := Start(ctx, NetPipe(b), RoleServer, mpConfig())
		if err != nil {
			t.Error(err)
		}
		ch <- s
	}()
	s := <-ch
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(ss); err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("server read %q, %v", got, err)
	}
	waitUntil(t, 5*time.Second, "STOP_SENDING", func() bool {
		_, err := ss.Write([]byte("x"))
		return err != nil
	})
	echoOnce(t, ctx, c, s)
}

// failingRestart is an earlyLayer whose RestartEarly fails, the carrier
// itself staying writable.
type failingRestart struct{ *earlyLayer }

func (l failingRestart) RestartEarly(context.Context) (PipeIO, error) {
	l.restarts.Add(1)
	l.mu.Lock()
	l.state = 1
	l.mu.Unlock()
	return PipeIO{}, errors.New("no restart")
}

// A failed restart still writes the CONNECTION_CLOSE on the carrier: the
// peer sees why the session ended.
func TestEarlyCarrierRestartFailsClose(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	layer := failingRestart{&earlyLayer{Conn: a, verdict: make(chan bool, 1)}}
	c, err := Start(ctx, PipeIO{R: a, W: layer}, RoleClient, earlyFloorConfig())
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("early")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the first flight", func() bool {
		layer.mu.Lock()
		defer layer.mu.Unlock()
		return len(layer.held) > 0
	})
	ch := make(chan error, 1)
	go func() {
		s, err := Start(ctx, NetPipe(b), RoleServer, mpConfig())
		if err != nil {
			ch <- err
			return
		}
		ch <- s.Wait()
	}()
	layer.decide(false)
	select {
	case err := <-ch:
		var te *TransportError
		if !errors.As(err, &te) || !te.Remote || te.ErrorCode != TransportInternalError ||
			te.ErrorMessage != "early data restart failed" {
			t.Fatalf("server ended with %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the server never saw the close")
	}
	waitDone(t, c)
	_ = st
}
