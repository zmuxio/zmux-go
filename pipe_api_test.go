package zmux

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"sync"

	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// Pipe.Wait and Done return once a pipe is gone: nil for a graceful removal,
// ErrPipeAborted for an abort; a pipe that is already gone answers at once.
// HS-28.
func TestPipeWaitDone(t *testing.T) {
	leakcheck.Check(t)
	if !errors.Is(ErrPipeAborted, ErrPipeGone) || errors.Is(ErrPipeGone, ErrPipeAborted) {
		t.Fatal("local abort must be a specific kind of gone pipe")
	}
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
		"abort": {aborted, func(err error) bool { return errors.Is(err, ErrPipeAborted) && errors.Is(err, ErrPipeGone) }},
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
		default:
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
	if j := (Backoff{Jitter: Set(0.0)}).filled().Jitter; j != 0 {
		t.Fatalf("explicit zero jitter %v", j)
	}
	for _, b := range []Backoff{{Min: -1}, {Max: -1}, {Jitter: Set(-1.0)}, {Jitter: Set(1.01)}, {Jitter: Set(math.NaN())}, {JitterUp: Set(1.01)},
		{Max: math.MaxInt64, JitterUp: Set(1.0)},
		{Min: 2 * time.Second, Max: time.Second}, {Min: 2 * DefaultMaxBackoff}} {
		if err := b.validate("Backoff"); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: %v", b, err)
		}
	}
	if err := (Backoff{Min: time.Second, Max: time.Second, Jitter: Set(0.5)}).validate("Backoff"); err != nil {
		t.Fatal(err)
	}
	endpoints, err := (Backoff{Jitter: Set(1.0), JitterUp: Set(1.0)}).WithDefaults(retryDefaults)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		sample float64
		want   time.Duration
	}{{0, 0}, {0.5, time.Second}, {1, 2 * time.Second}} {
		if got := endpoints.waitSample(time.Second, tc.sample); got != tc.want {
			t.Fatalf("sample %v: got %v, want %v", tc.sample, got, tc.want)
		}
	}
	b := Backoff{Jitter: Set(0.0)}.filled()
	if w := b.Wait(time.Second); w != time.Second {
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
	before, ok := p.Stats()
	if !ok || before.CarrierSample != nil {
		t.Fatalf("receive-only pipe stats: ok = %t, sample = %+v", ok, before.CarrierSample)
	}
	for _, stats := range e.client.PipeStats() {
		if stats.PipeID == p.ID() && stats.CarrierSample != nil {
			t.Fatalf("receive-only pipe sample in bulk stats = %+v", stats.CarrierSample)
		}
	}
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
	// ReturnOnReply returns before the peer has the pipe (see
	// TestReturnOnReplyPeerHasNoPipeYet): its view waits for the attachment.
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWait()
	if err := two.WaitAttached(waitCtx); err != nil {
		t.Fatal(err)
	}
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

// laterWritesHeld forwards a connection's first write at once and holds the
// writes after it until release is closed, in order: the Hello of a join
// goes out, the Confirm and what follows it wait.
type laterWritesHeld struct {
	net.Conn
	q       chan []byte
	closing chan struct{}
	once    sync.Once
	mu      sync.Mutex
	first   bool // under mu
}

func holdLaterWrites(c net.Conn, release <-chan struct{}) *laterWritesHeld {
	h := &laterWritesHeld{Conn: c, q: make(chan []byte, 64), closing: make(chan struct{})}
	go func() {
		select {
		case <-release:
		case <-h.closing:
			return
		}
		for {
			select {
			case b := <-h.q:
				_, _ = h.Conn.Write(b)
			case <-h.closing:
				return
			}
		}
	}()
	return h
}

func (h *laterWritesHeld) Write(b []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.first {
		h.first = true
		return h.Conn.Write(b)
	}
	h.q <- append([]byte(nil), b...)
	return len(b), nil
}

func (h *laterWritesHeld) Close() error {
	h.once.Do(func() { close(h.closing) })
	return h.Conn.Close()
}

// With ReturnOnReply, AddPipe returns once the Reply has verified, and the
// peer attaches the pipe only when it has the Confirm: the peer has no pipe
// until then, and Pipe.WaitAttached is what waits for it. The Confirm is
// held back here, so the order does not depend on the scheduler.
func TestReturnOnReplyPeerHasNoPipeYet(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	release := make(chan struct{})
	dial := e.dial
	e.dial = func(ctx context.Context) (net.Conn, error) {
		nc, err := dial(ctx)
		if err != nil {
			return nil, err
		}
		return holdLaterWrites(nc, release), nil
	}
	p := e.addPipe(t, PipeOptions{ReturnOnReply: true})
	e.dial = dial
	if _, ok := e.server.Pipe(p.ID()); ok {
		t.Fatal("the peer has the pipe before it has the Confirm")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	short, cancelShort := context.WithCancel(ctx)
	cancelShort()
	if err := p.WaitAttached(short); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitAttached before the Confirm arrived: %v", err)
	}
	close(release)
	if err := p.WaitAttached(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.server.Pipe(p.ID()); !ok {
		t.Fatal("the peer has no pipe once it is attached")
	}
}

// HS-20, HS-27.
func TestJoinOutcomeEventDelivery(t *testing.T) {
	for _, handler := range []bool{false, true} {
		cc, sc := mpConfig(), mpConfig()
		type notice struct {
			c   *Conn
			err error
			id  PipeID
		}
		traced := make(chan Event, 128)
		cc.Tracer = &Tracer{Event: func(_ SessionID, ev Event) {
			select {
			case traced <- ev:
			default:
			}
		}}
		called := make(chan notice, 4)
		release := make(chan struct{})
		cb := func(c *Conn, id PipeID, err error) {
			_ = c.ConnectionStats()
			called <- notice{c: c, id: id, err: err}
			<-release
		}
		cc.JoinOutcome, sc.JoinOutcome = cb, cb
		e := newMPEnv(t, cc, sc)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		confirm := make(chan struct{})
		nc, err := e.dial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		p, err := e.client.AddPipe(ctx, NetPipe(holdLaterWrites(nc, confirm)), PipeOptions{ReturnOnReply: true})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case n := <-called:
			t.Fatalf("before confirm: %+v", n)
		default:
		}
		close(confirm)
		seen := map[*Conn]bool{}
		for range 2 {
			select {
			case n := <-called:
				if n.err != nil || n.id != p.ID() || seen[n.c] {
					t.Fatal(n)
				}
				seen[n.c] = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		events := make(chan Event, 16)
		for _, c := range []*Conn{e.client, e.server} {
			if handler {
				err := c.HandleEvents(func(ev Event) {
					if pipeEvent(ev.Kind) && ev.PipeID == p.ID() {
						events <- ev
					}
				})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				for {
					ended, stop := context.WithCancel(ctx)
					stop()
					ev, err := c.NextEvent(ended)
					if err != nil {
						break
					}
					if pipeEvent(ev.Kind) && ev.PipeID == p.ID() {
						t.Fatal("delivery during outcome")
					}
				}
			}
		}
		select {
		case ev := <-events:
			t.Fatalf("early handler event %+v", ev)
		default:
		}
		close(release)
		if handler {
			select {
			case <-events:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		} else {
			for _, c := range []*Conn{e.client, e.server} {
				for {
					ev, err := c.NextEvent(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if pipeEvent(ev.Kind) && ev.PipeID == p.ID() {
						break
					}
				}
			}
		}
		// The trace may arrive on either side of hook completion.
		for {
			select {
			case ev := <-traced:
				if pipeEvent(ev.Kind) && ev.PipeID == p.ID() {
					goto tracedPipe
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	tracedPipe:
		cancel()
	}
}

// HS-04, HS-20, HS-24.
func TestInboundJoinOutcomeReasons(t *testing.T) {
	for _, why := range []string{"timeout", "evicted", "canceled", "deadline", "listener", "session"} {
		t.Run(why, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := mpConfig()
				cfg.JoinTimeout = 10 * time.Second
				if why == "timeout" {
					cfg.JoinTimeout = time.Millisecond
				}
				outcomes := make(chan error, 8)
				cfg.JoinOutcome = func(_ *Conn, _ PipeID, err error) { outcomes <- err }
				e := newMemMPEnv(t, mpConfig(), cfg)
				setup, stop := context.WithCancel(t.Context())
				if why == "deadline" {
					stop()
					setup, stop = context.WithTimeout(t.Context(), time.Millisecond)
				}
				defer stop()
				gate := make(chan struct{})
				defer close(gate)
				pending := func() *Pipe {
					a, b := net.Pipe()
					go func() { _ = e.lst.HandlePipe(setup, NetPipe(b), PipeHint{}) }()
					p, err := e.client.AddPipe(t.Context(), NetPipe(holdLaterWrites(a, gate)), PipeOptions{ReturnOnReply: true})
					if err != nil {
						t.Fatal(err)
					}
					return p
				}
				pending()
				var want error
				switch why {
				case "deadline":
					want = context.DeadlineExceeded
				case "timeout":
					want = ErrJoinTimeout
				case "evicted":
					policy := cfg.SessionPolicy
					policy.Revision++
					policy.Limits.PendingPeerJoins = 1
					if err := e.server.SetSessionPolicy(policy); err != nil {
						t.Fatal(err)
					}
					pending()
					want = ErrJoinEvicted
				case "canceled":
					stop()
					want = context.Canceled
				case "listener":
					_ = e.lst.Close()
					want = net.ErrClosed
				case "session":
					e.server.Abort(9, "session ending")
					want = net.ErrClosed
				}
				err := <-outcomes
				if !errors.Is(err, want) {
					t.Fatalf("outcome %v, want %v", err, want)
				}
				if why == "session" {
					var ae *ApplicationError
					if !errors.As(err, &ae) || ae.ErrorCode != 9 {
						t.Fatalf("lost session error %v", err)
					}
				}
				if why != "evicted" {
					select {
					case err := <-outcomes:
						t.Fatalf("second outcome %v", err)
					default:
					}
				}
			})
		})
	}
}

type handshakeGate struct {
	*countingCarrier
	handshake func(context.Context) error
}

func (h *handshakeGate) HandshakeContext(ctx context.Context) error { return h.handshake(ctx) }

// HS-24, HS-25.
func TestOutboundJoinOutcomeBeforePipeID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sentinel := errors.New("handshake failed")
		type notice struct {
			err error
			id  PipeID
		}
		notes := make(chan notice, 1)
		cfg := mpConfig()
		cfg.JoinTimeout = time.Second
		cfg.JoinOutcome = func(c *Conn, id PipeID, err error) { _ = c.ConnectionStats(); notes <- notice{id: id, err: err} }
		e := newMemMPEnv(t, cfg, mpConfig())
		for _, want := range []error{sentinel, context.Canceled, ErrJoinTimeout} {
			ctx, stop := context.WithCancel(t.Context())
			started := make(chan struct{})
			h := &handshakeGate{countingCarrier: newCountingCarrier(), handshake: func(ctx context.Context) error {
				close(started)
				if errors.Is(want, sentinel) {
					return sentinel
				}
				<-ctx.Done()
				return ctx.Err()
			}}
			result := make(chan error, 1)
			go func() {
				_, err := e.client.AddPipe(ctx, PipeIO{R: h, W: h}, PipeOptions{RecordMode: Set(RecordBound)})
				result <- err
			}()
			<-started
			if errors.Is(want, context.Canceled) {
				stop()
			}
			n := <-notes
			if n.id != UnassignedPipeID || !errors.Is(n.err, want) {
				t.Fatalf("handshake notice %+v, want %v", n, want)
			}
			if err := <-result; !errors.Is(err, want) {
				t.Fatalf("explicit join: %v", err)
			}
			stop()
			h.checkOnce(t)
		}
		a, b := net.Pipe()
		_ = b.Close()
		if _, err := e.client.AddPipe(t.Context(), NetPipe(a), PipeOptions{}); !errors.Is(err, ErrPipeGone) {
			t.Fatal(err)
		}
		n := <-notes
		if n.id == UnassignedPipeID || !errors.Is(n.err, ErrPipeGone) {
			t.Fatal(n)
		}
	})
}

// HS-24.
func TestJoinOutcomeRefusal(t *testing.T) {
	for _, authorized := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			cc, sc := mpConfig(), mpConfig()
			in, out := make(chan error, 1), make(chan error, 1)
			sc.SessionPolicy.AuthorizeJoin = func(context.Context, JoinAuthorization) (JoinVerdict, error) {
				if authorized {
					return JoinPermit, nil
				}
				return JoinDeny, nil
			}
			sc.JoinOutcome = func(_ *Conn, _ PipeID, err error) { in <- err }
			cc.JoinOutcome = func(_ *Conn, _ PipeID, err error) { out <- err }
			e := newMemMPEnv(t, cc, sc)
			a, b := net.Pipe()
			handled := make(chan error, 1)
			// The receiver detects that the new pipe would nest the session inside itself.
			go func() { handled <- e.lst.HandlePipe(t.Context(), NetPipe(laneConn{Conn: b, id: e.server}), PipeHint{}) }()
			_, err := e.client.AddPipe(t.Context(), NetPipe(a), PipeOptions{})
			if <-handled == nil || err == nil {
				t.Fatal("unexpected join success")
			}
			if !authorized {
				if !errors.Is(<-out, ErrPipeGone) {
					t.Fatal("silent close did not reach the joining side")
				}
				select {
				case err := <-in:
					t.Fatalf("outcome after denied authorization: %v", err)
				default:
				}
				return
			}
			if !errors.Is(err, ErrJoinRefused) || !errors.Is(<-in, ErrJoinRefused) || !errors.Is(<-out, ErrJoinRefused) {
				t.Fatalf("expected a protocol refusal at both endpoints: %v", err)
			}
		})
	}
}

func TestJoinOutcomeHoldsRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc := mpConfig()
		started, gate := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		cc.JoinOutcome = func(_ *Conn, _ PipeID, err error) {
			if err != nil {
				t.Errorf("join outcome %v", err)
			}
			close(started)
			<-gate
		}
		e := newMemMPEnv(t, cc, mpConfig())
		p := e.addPipe(t, PipeOptions{ReturnOnReply: true})
		<-started
		if err := p.Abort(); err != nil {
			t.Fatal(err)
		}
		<-p.Done()
		e.client.peek(func() {
			h := e.client.events.held[uint64(p.ID())]
			if h == nil || !h.retired || e.client.events.idx[eventKey{kind: EventPipesRetired}] != nil {
				t.Error("retirement must remain private while the hook runs")
			}
		})
		release()
		for {
			ev, err := e.client.NextEvent(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if ev.Kind == EventPipesRetired {
				if ev.Retired != 1 || len(ev.Last) != 1 || ev.Last[0] != p.ID() {
					t.Fatal(ev)
				}
				break
			}
		}
	})
}

// HS-24a. The core can process the shared deadline before the driver's
// deadline context wakes. Both observers still get the join timeout.
func TestPipeConfirmationCoreDeadline(t *testing.T) {
	for _, replyOnly := range []bool{false, true} {
		name := "synchronous"
		if replyOnly {
			name = "replyOnly"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cc, sc := mpConfig(), mpConfig()
				cc.JoinTimeout = 20 * time.Millisecond
				notice := make(chan error, 1)
				cc.JoinOutcome = func(_ *Conn, _ PipeID, err error) { notice <- err }
				e := newMemMPEnv(t, cc, sc)
				release := make(chan struct{})
				defer close(release)
				n, err := e.dial(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				carrier := NetPipe(holdLaterWrites(n, release))
				result := make(chan error, 1)
				go func() {
					_, err := e.client.AddPipe(t.Context(), carrier, PipeOptions{ReturnOnReply: replyOnly})
					result <- err
				}()
				synctest.Wait()
				var p *Pipe
				for _, member := range e.client.Pipes() {
					if member.ID() != 0 {
						p = member
						break
					}
				}
				if p == nil {
					t.Fatal("missing unconfirmed pipe")
				}
				if replyOnly {
					if err := <-result; err != nil {
						t.Fatal(err)
					}
				}
				c := e.client
				deadline := p.end.join.deadline
				if !c.clk.source().Now().Before(deadline) {
					t.Fatal("driver deadline already expired")
				}
				c.locked(func() {
					c.s.HandleTimeout(core.Time(deadline.Sub(c.clk.base)))
				})
				synctest.Wait()
				select {
				case err := <-notice:
					if !errors.Is(err, ErrJoinTimeout) || errors.Is(err, ErrPipeGone) {
						t.Fatalf("join outcome: %v", err)
					}
				default:
					t.Fatal("core deadline did not settle the join")
				}
				if err := p.WaitAttached(t.Context()); !errors.Is(err, ErrJoinTimeout) {
					t.Fatalf("WaitAttached: %v", err)
				}
				if !replyOnly {
					if err := <-result; !errors.Is(err, ErrJoinTimeout) {
						t.Fatalf("AddPipe: %v", err)
					}
				}
			})
		})
	}
}

// HS-23, HS-24.
func TestAddPipeConfirmationContext(t *testing.T) {
	for _, finish := range []string{"cancel", "deadline", "drain", "confirm"} {
		t.Run(finish, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cc := mpConfig()
				cc.JoinTimeout = time.Minute
				notice := make(chan error, 2)
				cc.JoinOutcome = func(_ *Conn, _ PipeID, err error) { notice <- err }
				e := newMemMPEnv(t, cc, mpConfig())
				gate := make(chan struct{})
				open := sync.OnceFunc(func() { close(gate) })
				defer open()
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				n, err := e.dial(ctx)
				if err != nil {
					t.Fatal(err)
				}
				ch := make(chan error, 1)
				carrier := holdLaterWrites(n, gate)
				go func() { _, err := e.client.AddPipe(ctx, NetPipe(carrier), PipeOptions{}); ch <- err }()
				synctest.Wait()
				var pending *Pipe
				for _, p := range e.client.Pipes() {
					if p.ID() != 0 {
						pending = p
						break
					}
				}
				if pending == nil {
					t.Fatal("join missing before confirmation")
				}
				if st, ok := pending.Stats(); !ok || st.Outbound != OutboundActive || st.PeerAttached {
					t.Fatalf("expected a locally active pipe still waiting for its peer: %+v", st)
				}
				var want error
				switch finish {
				case "confirm":
					open()
				case "cancel":
					cancel()
					want = context.Canceled
				case "drain":
					if err := pending.Close(); err != nil {
						t.Fatal(err)
					}
					want = ErrPipeGone
				case "deadline":
					want = context.DeadlineExceeded
				}
				if got := <-ch; !errors.Is(got, want) {
					t.Fatalf("AddPipe = %v, want %v", got, want)
				}
				if want != nil {
					for _, p := range e.client.Pipes() {
						if p.ID() == pending.ID() {
							t.Fatal("failed join still listed after AddPipe returned")
						}
					}
					synctest.Wait()
					select {
					case <-carrier.closing:
					default:
						t.Fatal("failed join retained its locally attached carrier")
					}
				}
				cancel()
				open()
				for _, got := range []error{<-notice, pending.WaitAttached(t.Context())} {
					if !errors.Is(got, want) || errors.Is(got, ErrJoinTimeout) {
						t.Fatalf("terminal result %v, want %v", got, want)
					}
				}
				synctest.Wait()
				if want != nil {
					if st, ok := pending.Stats(); ok && (st.PeerAttached || st.Outbound == OutboundActive) {
						t.Fatalf("late Confirm revived failed pipe: %+v", st)
					}
				}
				select {
				case err := <-notice:
					t.Fatalf("duplicate outcome %v", err)
				default:
				}
			})
		})
	}
}

// HS-04.
func TestJoinRefusalWhileReplyBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sc := mpConfig()
		reported := make(chan error, 2)
		sc.JoinOutcome = func(_ *Conn, _ PipeID, err error) { reported <- err }
		e := newMemMPEnv(t, mpConfig(), sc)
		setup, cancel := context.WithCancel(t.Context())
		defer cancel()
		a, b := net.Pipe()
		w := &gatedWrite{Conn: b, nth: 1, gate: make(chan struct{}), blocked: make(chan struct{})}
		release := sync.OnceFunc(func() { close(w.gate) })
		defer release()
		server, client := make(chan error, 1), make(chan error, 1)
		go func() { server <- e.lst.HandlePipe(setup, NetPipe(laneConn{Conn: w, id: e.server}), PipeHint{}) }()
		go func() { _, err := e.client.AddPipe(t.Context(), NetPipe(a), PipeOptions{}); client <- err }()
		<-w.blocked
		cancel()
		if err := <-reported; !errors.Is(err, ErrJoinRefused) {
			t.Fatalf("refusal changed while flushing: %v", err)
		}
		release()
		if <-server == nil || <-client == nil {
			t.Fatal("refused join succeeded")
		}
		synctest.Wait()
		select {
		case err := <-reported:
			t.Fatalf("repeated outcome %v", err)
		default:
		}
	})
}
