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
// ErrPipeAborted for an abort; a pipe that is already gone answers at once.
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
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.Abort(); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		p    *Pipe
		want func(error) bool
	}{
		"close": {closed, func(err error) bool { return err == nil }},
		"abort": {aborted, func(err error) bool { return errors.Is(err, ErrPipeAborted) }},
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
	if err := e.addPipeErr(PipeOptions{QualityBand: Set(QualityBand{RateRatio: math.NaN()})}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a NaN band at AddPipe: %v", err)
	}
}

func (e *mpEnv) addPipeErr(o PipeOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := e.dial(ctx)
	if err != nil {
		return err
	}
	_, err = e.client.AddPipe(ctx, NetPipe(nc), o)
	return err
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

func (f *faultConn) brk() { close(f.broken); _ = f.Close() }

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
	if err := e.addPipeErr(PipeOptions{Direction: DirectionReceiveOnly, QualityBand: Set(band)}); !errors.Is(err, ErrInvalidConfig) {
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
	if err := p.Abort(); !errors.Is(err, ErrPipeGone) {
		t.Fatalf("Abort = %v", err)
	}
	if goneEnd.err != nil || goneEnd.waited != closedChan {
		t.Fatal("the shared gone record changed")
	}
}

// Options reads back what AddPipe was given with every Update applied, in the
// form it was set; a cleared field reads as the zero Change, and the last
// options stay readable once the pipe is gone.
func TestPipeOptionsReadBack(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	band := QualityBand{RTT: 50 * time.Millisecond}
	live := PipeLiveness{ProbeInterval: -5 * time.Second}
	p := e.addPipe(t, PipeOptions{Label: "a", Weight: Set[uint64](3), QualityBand: Set(band), Liveness: Set(live)})
	o := p.Options()
	if o.Label != "a" || o.Weight != Set[uint64](3) || o.QualityBand != Set(band) || o.Liveness != Set(live) ||
		o.Direction != DirectionBidirectional || o.RecordMode != Set(RecordSealed) {
		t.Fatalf("options at add: %+v", o)
	}
	upd := PipeUpdate{Weight: Clear[uint64](), Probation: Set(-time.Second), SendPriority: Set[uint64](4),
		QualityBand: Set(QualityBand{}), Liveness: Clear[PipeLiveness]()}
	if err := p.Update(upd); err != nil {
		t.Fatal(err)
	}
	o = p.Options()
	if o.Weight != (Change[uint64]{}) || o.Probation != Set(-time.Second) || o.SendPriority != Set[uint64](4) ||
		o.QualityBand != Set(QualityBand{}) || o.Liveness != (Change[PipeLiveness]{}) || o.ReceivePriority != (Change[uint64]{}) {
		t.Fatalf("options after the update: %+v", o)
	}
	if err := p.Update(PipeUpdate{Weight: Set[uint64](-1 + 1), QualityBand: Set(QualityBand{RTT: -1})}); err == nil {
		t.Fatal("an invalid band was accepted")
	}
	if got := p.Options(); got.Weight != (Change[uint64]{}) {
		t.Fatalf("a refused update changed the options: %+v", got)
	}
	_ = p.Abort()
	<-p.Done()
	if got := p.Options(); got.Label != "a" || got.SendPriority != Set[uint64](4) {
		t.Fatalf("options after the pipe ended: %+v", got)
	}
}

// The fields a pipe has from its creation read as the values it has: the
// key group the Join used when AddPipe left it zero, the direction from this
// endpoint's view, ReturnOnReply as AddPipe was given it; a pipe the peer
// joined reads the Join's direction, record mode and key group and the
// defaults elsewhere.
func TestPipeOptionsFixedFields(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	peerOf := func(p *Pipe) *Pipe {
		t.Helper()
		q, ok := e.server.Pipe(p.ID())
		if !ok {
			t.Fatalf("the server has no pipe %d", p.ID())
		}
		return q
	}
	def := e.addPipe(t, PipeOptions{Label: "default group"})
	two := e.addPipe(t, PipeOptions{KeyGroup: 2, Direction: DirectionSendOnly, ReturnOnReply: true, Weight: Set[uint64](5)})
	o := def.Options()
	if o.KeyGroup != 1 || o.Label != "default group" || o.Direction != DirectionBidirectional || o.RecordMode != Set(RecordSealed) || o.ReturnOnReply {
		t.Fatalf("the default pipe: %+v", o)
	}
	o = two.Options()
	if o.KeyGroup != 2 || o.Direction != DirectionSendOnly || !o.ReturnOnReply || o.Weight != Set[uint64](5) {
		t.Fatalf("the group 2 pipe: %+v", o)
	}
	// Options read back and handed to the AddPipe of a replacing pipe: the
	// same fixed fields, whatever they were given as.
	re := e.addPipe(t, two.Options())
	if r := re.Options(); r.KeyGroup != 2 || r.Direction != DirectionSendOnly || !r.ReturnOnReply {
		t.Fatalf("the replacing pipe: %+v", r)
	}

	// The peer's view of the same pipes.
	o = peerOf(def).Options()
	if o.KeyGroup != 1 || o.Direction != DirectionBidirectional || o.RecordMode != Set(RecordSealed) ||
		o.Label != "" || o.ReturnOnReply || o.Weight != (Change[uint64]{}) {
		t.Fatalf("the peer's default pipe: %+v", o)
	}
	o = peerOf(two).Options()
	if o.KeyGroup != 2 || o.Direction != DirectionReceiveOnly || o.RecordMode != Set(RecordSealed) || o.ReturnOnReply ||
		o.Weight != (Change[uint64]{}) {
		t.Fatalf("the peer's group 2 pipe: %+v", o)
	}
	// An update on the peer's side sets its own field, the fixed ones stay.
	q := peerOf(def)
	if err := q.Update(PipeUpdate{Weight: Set[uint64](7)}); err != nil {
		t.Fatal(err)
	}
	if o := q.Options(); o.Weight != Set[uint64](7) || o.KeyGroup != 1 {
		t.Fatalf("after the peer's update: %+v", o)
	}
	// The bootstrap pipe was not joined: no key group.
	if p0, ok := e.client.Pipe(0); !ok || p0.Options().KeyGroup != 0 {
		t.Fatalf("the bootstrap pipe: %v", ok)
	}
}
