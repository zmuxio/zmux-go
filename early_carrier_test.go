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
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// earlyLayer is a client carrier layer with a first flight: what is
// written before the verdict is held and then dropped on
// rejection, with whatever comes until RestartEarly (the server never sees
// it), as a transform whose first message the server may refuse does.
type earlyLayer struct {
	net.Conn
	verdict  chan bool
	first    chan struct{}
	held     [][]byte
	state    int // 0 holding, 1 passing, 2 dropping
	dropped  atomic.Int64
	mu       sync.Mutex
	restarts atomic.Int32
	waits    atomic.Int32
	late     bool // no early data this time: writes pass from the start
}

func (l *earlyLayer) SendsEarlyData() bool { return !l.late }

func (l *earlyLayer) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case 0:
		if l.first != nil && len(l.held) == 0 {
			close(l.first)
		}
		l.held = append(l.held, bytes.Clone(b))
		return len(b), nil
	case 2:
		l.dropped.Add(int64(len(b)))
		return len(b), nil
	}
	return l.Conn.Write(b)
}

// reject discards the flight and any writes before the restart.
func (l *earlyLayer) reject() {
	l.mu.Lock()
	for _, b := range l.held {
		l.dropped.Add(int64(len(b)))
	}
	l.held = nil
	l.state = 2
	l.mu.Unlock()
	l.verdict <- false
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

func plainEarlyConfig() *Config {
	cfg := &Config{}
	params := plainEarlyParams()
	block, err := wire.AppendParams(nil, &params)
	if err != nil {
		panic(err)
	}
	cfg.EarlyData = EarlyData{Remembered: block, PlainQMux: true, Replayable: true}
	return cfg
}

func plainEarlyParams() wire.Params {
	p := core.DefaultConfig().Local
	p.Has &^= wire.HasAnyZmux
	return p
}

func startPlainServer(ctx context.Context, p PipeIO) (*Conn, error) {
	cfg := &Config{}
	cc, err := cfg.coreConfig(core.RoleResponder, core.RoleResponder)
	if err != nil {
		return nil, err
	}
	cc.Local = plainEarlyParams()
	return startCore(ctx, p, cfg, cc, nil, nil, nil, nil)
}

// A STOP_SENDING and a window update sent in rejected carrier early data
// go again after the restart, behind the re-issued opening STREAM frame.
func TestEarlyCarrierRejectedStop(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		a, b := net.Pipe()
		layer := &earlyLayer{Conn: a, verdict: make(chan bool, 1)}
		cfg := plainEarlyConfig()
		cfg.ClockDomain = NewClockDomain(timeshim.Real)
		c, err := Start(ctx, PipeIO{R: a, W: layer}, RoleClient, cfg)
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
		synctest.Wait()
		if len(layer.held) == 0 {
			t.Fatal("the first flight was not handed to the carrier")
		}
		_ = st.SetReceiveWindow(1<<22, 1<<22)
		st.CancelRead(5)
		synctest.Wait() // let the frames join the held flight
		layer.reject()
		synctest.Wait()
		if n := layer.restarts.Load(); n != 1 {
			t.Fatalf("%d restarts, want 1", n)
		}
		ch := make(chan *Conn, 1)
		go func() {
			s, err := startPlainServer(ctx, NetPipe(b))
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
		var stop *StreamError
		if err := waitStreamStop(ctx, ss); !errors.As(err, &stop) || !stop.Remote || stop.ErrorCode != 5 {
			t.Fatalf("STOP_SENDING: %v", err)
		}
		echoOnce(t, ctx, c, s)
	})
}

// failingRestart is an earlyLayer whose RestartEarly fails, the carrier
// itself staying writable.
type failingRestart struct{ *earlyLayer }

func (l failingRestart) RestartEarly(context.Context) (PipeIO, error) {
	l.restarts.Add(1)
	l.mu.Lock()
	l.state = 1 //nolint:revive // state lives behind the embedded pointer
	l.mu.Unlock()
	return PipeIO{}, errors.New("no restart")
}

// deafWriter is a carrier whose writes never fail: the peer's session
// ends only through what it reads, never by a write racing the close.
type deafWriter struct{ net.Conn }

func (w deafWriter) Write(b []byte) (int, error) {
	_, _ = w.Conn.Write(b)
	return len(b), nil
}

// A failed restart still writes the CONNECTION_CLOSE on the carrier: the
// peer sees why the session ended. The server's writes cannot fail, so it
// ends on what it reads (the close comes before the carrier's end).
func TestEarlyCarrierRestartFailsClose(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	layer := failingRestart{&earlyLayer{Conn: a, verdict: make(chan bool, 1), first: make(chan struct{})}}
	c, err := Start(ctx, PipeIO{R: a, W: layer}, RoleClient, plainEarlyConfig())
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
	waitTestSignal(t, layer.first, "the first flight")
	ch := make(chan error, 1)
	go func() {
		s, err := startPlainServer(ctx, PipeIO{R: b, W: deafWriter{b}})
		if err != nil {
			ch <- err
			return
		}
		ch <- s.Wait()
	}()
	layer.reject()
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

// peerBytesSeen signals the first delivery from the carrier's read half.
type peerBytesSeen struct {
	net.Conn
	seen chan struct{}
	once sync.Once
}

func (r *peerBytesSeen) Read(b []byte) (int, error) {
	n, err := r.Conn.Read(b)
	if n > 0 {
		r.once.Do(func() { close(r.seen) })
	}
	return n, err
}

// The peer's first bytes may be read before the verdict is applied, a
// server speaking as soon as its carrier decided: they wait for the
// verdict, so a rejected flight restarts, or fails with a close the peer
// can read, whichever came first.
func TestEarlyCarrierPeerBytesBeforeVerdict(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		leakcheck.Check(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		r := &peerBytesSeen{Conn: a, seen: make(chan struct{})}
		layer := &earlyLayer{Conn: r, verdict: make(chan bool, 1)}
		c, err := Start(ctx, PipeIO{R: r, W: layer}, RoleClient, plainEarlyConfig())
		if err != nil {
			t.Fatal(err)
		}
		st, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		msg := []byte("early request")
		if _, err := st.Write(msg); err != nil {
			t.Fatal(err)
		}
		_ = st.Close()
		ch := make(chan *Conn, 1)
		go func() {
			s, err := startPlainServer(ctx, NetPipe(b))
			if err != nil {
				t.Error(err)
			}
			ch <- s
		}()
		waitTestSignal(t, r.seen, "the peer's bytes")
		layer.reject()
		s := <-ch
		if s == nil {
			_ = c.Close()
			waitDone(t, c)
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
		echoOnce(t, ctx, c, s)
	})
	t.Run("failed restart", func(t *testing.T) {
		leakcheck.Check(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		layer := failingRestart{&earlyLayer{Conn: a, verdict: make(chan bool, 1)}}
		r := &peerBytesSeen{Conn: a, seen: make(chan struct{})}
		c, err := Start(ctx, PipeIO{R: r, W: layer}, RoleClient, plainEarlyConfig())
		if err != nil {
			t.Fatal(err)
		}
		ch := make(chan error, 1)
		go func() {
			s, err := startPlainServer(ctx, PipeIO{R: b, W: deafWriter{b}})
			if err != nil {
				ch <- err
				return
			}
			ch <- s.Wait()
		}()
		waitTestSignal(t, r.seen, "the peer's bytes")
		layer.reject()
		select {
		case err := <-ch:
			var te *TransportError
			if !errors.As(err, &te) || !te.Remote || te.ErrorMessage != "early data restart failed" {
				t.Fatalf("server ended with %v", err)
			}
		case <-ctx.Done():
			t.Fatal("the server never saw the close")
		}
		waitDone(t, c)
	})
}

// AddPipe on a session that returned before establishment (early data)
// waits for the peer's parameters instead of failing with
// ErrNotNegotiated: multipath is decided only by them.
func TestAddPipeBeforeEstablishment(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, testAddPipeBeforeEstablishment)
}

func testAddPipeBeforeEstablishment(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := mpConfig()
	cfg.ClockDomain = NewClockDomain(timeshim.Real)
	l := mustNewListener(&ListenerConfig{Config: cfg})
	defer func() { _ = l.Close() }()
	a0, b0 := net.Pipe()
	defer func() { _ = b0.Close() }()
	ccfg := *cfg
	ccfg.ReturnBeforeEstablished = true
	c, err := Start(ctx, NetPipe(a0), RoleClient, &ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); waitDone(t, c) }()
	synctest.Wait()
	if c.ConnectionState().Established {
		t.Fatal("established without a peer")
	}
	// No peer yet: the call waits until its context ends.
	ap, bp := net.Pipe()
	defer func() { _ = bp.Close() }()
	openCtx, cancelOpen := context.WithCancel(ctx)
	defer cancelOpen()
	opened := make(chan error, 1)
	go func() {
		_, err := c.AddPipe(openCtx, NetPipe(ap), PipeOptions{})
		opened <- err
	}()
	synctest.Wait()
	select {
	case err := <-opened:
		t.Fatalf("AddPipe returned before establishment or cancellation: %v", err)
	default:
	}
	cancelOpen()
	if err := <-opened; !errors.Is(err, context.Canceled) {
		t.Fatalf("AddPipe before establishment: %v, want the context's error", err)
	}
	a1, b1 := net.Pipe()
	go func() { _ = l.HandlePipe(ctx, NetPipe(b0), PipeHint{}) }()
	go func() { _ = l.HandlePipe(ctx, NetPipe(b1), PipeHint{}) }()
	done := make(chan error, 1)
	go func() {
		_, err := c.AddPipe(ctx, NetPipe(a1), PipeOptions{})
		done <- err
	}()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); waitDone(t, s) }()
	if err := <-done; err != nil {
		t.Fatalf("AddPipe: %v", err)
	}
}
