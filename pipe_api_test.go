package zmux

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// Pipe.Wait and Done return once a pipe is gone: nil for a graceful removal,
// the cause for an abort; a pipe that is already gone answers at once.
func TestPipeWaitDone(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	closed := e.addPipe(t, PipeOptions{})
	aborted := e.addPipe(t, PipeOptions{})
	select {
	case <-closed.Done():
		t.Fatal("Done of a live pipe")
	default:
	}
	cause := errors.New("test abort")
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.Abort(cause); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		p    *Pipe
		want func(error) bool
	}{
		"close": {closed, func(err error) bool { return err == nil }},
		"abort": {aborted, func(err error) bool { return errors.Is(err, cause) }},
	} {
		done := make(chan error, 1)
		go func() { done <- c.p.Wait() }()
		select {
		case err := <-done:
			if !c.want(err) {
				t.Errorf("%s: Wait = %v", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Wait did not return", name)
		}
		select {
		case <-c.p.Done():
		case <-time.After(time.Second):
			t.Fatalf("%s: Done not closed after Wait", name)
		}
	}
}

// The session's end releases the waiters of its pipes with its error.
func TestPipeWaitSessionEnd(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{})
	done := make(chan error, 1)
	go func() { done <- p.Wait() }()
	e.client.Abort(7, "bye")
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Wait = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return at the session's end")
	}
}

// PipeOptions and PipeUpdate carry the pipe's quality band; Clear returns
// the pipe to the session's.
func TestPipeQualityBand(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	band := QualityBand{RTT: 50 * time.Millisecond}
	p := e.addPipe(t, PipeOptions{QualityBand: Set(band)})
	if err := p.Update(PipeUpdate{QualityBand: Set(QualityBand{RTT: -1})}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a negative band: %v", err)
	}
	if err := p.Update(PipeUpdate{QualityBand: Clear[QualityBand]()}); err != nil {
		t.Fatal(err)
	}
	if err := p.Update(PipeUpdate{QualityBand: Set(band), Weight: Set[uint64](2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.addPipeErr(PipeOptions{QualityBand: Set(QualityBand{RateRatio: math.NaN()})}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a NaN band at AddPipe: %v", err)
	}
}

func (e *mpEnv) addPipeErr(o PipeOptions) (*Pipe, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := e.dial(ctx)
	if err != nil {
		return nil, err
	}
	return e.client.AddPipe(ctx, NetPipe(nc), o)
}

// Every error of a session's end, the timeouts included, is a closed
// session and not a timeout of the call; the local close matches
// net.ErrClosed too.
func TestErrorClassesNotTimeouts(t *testing.T) {
	for _, err := range []error{&IdleTimeoutError{}, &HandshakeTimeoutError{}, &ResumeTimeoutError{}} {
		var ne net.Error
		if errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &ne) {
			t.Errorf("%T is a timeout", err)
		}
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("%T is not a closed session", err)
		}
	}
	if !errors.Is(ErrStreamClosed, net.ErrClosed) || errors.Is(ErrStreamClosed, os.ErrDeadlineExceeded) {
		t.Error("ErrStreamClosed does not match net.ErrClosed alone")
	}
}

// A Switch is the default at zero, off when negative and on when positive.
func TestSwitch(t *testing.T) {
	for s, want := range map[Switch]bool{0: true, SwitchOn: true, SwitchOff: false, -7: false, 7: true} {
		if s.On() != want {
			t.Errorf("Switch(%d).On() = %v", s, s.On())
		}
	}
}

// Backoff fills its defaults, takes a negative jitter as none and refuses
// bounds out of order.
func TestBackoff(t *testing.T) {
	f := Backoff{}.filled()
	if f.Min != DefaultMinBackoff || f.Max != DefaultMaxBackoff || f.Jitter != DefaultBackoffJitter {
		t.Fatalf("defaults %+v", f)
	}
	if j := (Backoff{Jitter: -1}).filled().Jitter; j != 0 {
		t.Fatalf("negative jitter %v", j)
	}
	for _, b := range []Backoff{{Min: -1}, {Max: -1}, {Jitter: 1}, {Jitter: math.NaN()},
		{Min: 2 * time.Second, Max: time.Second}, {Min: 2 * DefaultMaxBackoff}} {
		if err := b.validate("Backoff"); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: %v", b, err)
		}
	}
	if err := (Backoff{Min: time.Second, Max: time.Second, Jitter: 0.5}).validate("Backoff"); err != nil {
		t.Fatal(err)
	}
	b := Backoff{Jitter: -1}.filled()
	if w := b.wait(time.Second); w != time.Second {
		t.Fatalf("wait without jitter %v", w)
	}
}

// faultConn fails its reads and writes with err once broken.
type faultConn struct {
	net.Conn
	err    error
	broken chan struct{}
}

func (f *faultConn) Read(b []byte) (int, error) {
	n, err := f.Conn.Read(b)
	select {
	case <-f.broken:
		return 0, f.err
	default:
	}
	return n, err
}

func (f *faultConn) Write(b []byte) (int, error) {
	n, err := f.Conn.Write(b)
	select {
	case <-f.broken:
		return 0, f.err
	default:
	}
	return n, err
}

func (f *faultConn) brk() { close(f.broken); _ = f.Conn.Close() }

// A pipe whose carrier fails reports the carrier's error through Wait.
func TestPipeWaitCarrierFault(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := e.dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("test carrier fault")
	fc := &faultConn{Conn: nc, err: fault, broken: make(chan struct{})}
	p, err := e.client.AddPipe(ctx, NetPipe(fc), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fc.brk()
	done := make(chan error, 1)
	go func() { done <- p.Wait() }()
	select {
	case err := <-done:
		if !errors.Is(err, fault) {
			t.Fatalf("Wait = %v, want the carrier's error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after the carrier failed")
	}
}

// A quality band needs an outbound direction: AddPipe and Update refuse it
// on a receive-only pipe, and Update applies none of its fields then.
func TestPipeQualityBandReceiveOnly(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	band := QualityBand{RTT: 50 * time.Millisecond}
	if _, err := e.addPipeErr(PipeOptions{Direction: DirectionReceiveOnly, QualityBand: Set(band)}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("AddPipe of a receive-only pipe with a band: %v", err)
	}
	p := e.addPipe(t, PipeOptions{Direction: DirectionReceiveOnly})
	before, _ := p.Stats()
	err := p.Update(PipeUpdate{ReceivePriority: Set[uint64](before.ReceivePriority + 5), QualityBand: Set(band)})
	if !errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrPipeGone) {
		t.Fatalf("Update with a band on a receive-only pipe: %v", err)
	}
	if after, _ := p.Stats(); after.ReceivePriority != before.ReceivePriority {
		t.Fatal("a refused Update applied a field")
	}
}

// A handle of a pipe that is gone answers Done, Wait and Abort without
// touching the shared end record.
func TestGonePipeHandle(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := &Pipe{c: e.client, id: 1 << 40, end: goneEnd}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done of a gone pipe not closed")
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait = %v", err)
	}
	if err := p.Abort(errors.New("x")); !errors.Is(err, ErrPipeGone) {
		t.Fatalf("Abort = %v", err)
	}
	if goneEnd.err != nil || goneEnd.waited != closedChan {
		t.Fatal("the shared gone record changed")
	}
}
