package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// Role is the role argument of Start; the zero value is invalid, so a
// forgotten role is an error rather than a silent client.
type Role uint8

const (
	// RoleClient is the initiator (the endpoint that dialled the carrier).
	RoleClient Role = iota + 1
	// RoleServer is the responder.
	RoleServer
	// RoleAuto negotiates the role with zmux_role (symmetric carriers).
	RoleAuto
)

// StreamID is a stream identifier (RFC 9000 Section 2.1).
type StreamID int64

// Conn is a zmux session. Its methods are safe for concurrent use.
//
// Concurrency model: one mutex guards the sans-IO core; no
// I/O, blocking or application code runs under it. Each pipe direction has
// a reader and a writer goroutine; wake-ups are queued under the lock and
// delivered after it is released.
type Conn struct {
	mu  sync.Mutex
	s   *core.Session
	clk clock
	cfg *Config

	timer     timeshim.Timer
	timerAt   core.Time
	timerArm  core.Time     // when the timer was armed to fire
	timerLate time.Duration // smoothed lateness of the host timer

	pipes  map[uint64]*pipeDrv
	wg     sync.WaitGroup
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelCauseFunc
	err    error // terminal error once done

	// exp is the late exporter of PipeAuth.Exporter, nil without it.
	exp *lateExporter
	// boot holds the bootstrap carrier's exporters (ExportKeyingMaterial),
	// nil when it has none; set before the session is shared.
	boot *lateExporter
	// bootIO is the bootstrap carrier, for ConnectionState's TLS state and
	// application protocol; set before the session is shared.
	bootIO PipeIO
	// firstTimer releases a held first flight at the handshake timeout;
	// firstTimeouts counts that (session lock).
	firstTimer    timeshim.Timer
	firstTimeouts uint64

	// verdictDone closes once the carrier's early-data verdict has been
	// applied; nil without one (set before the session is shared).
	verdictDone chan struct{}

	// admitted is the admission value of the bootstrap carrier (set
	// before the session is shared, never changed).
	admitted any

	// orphans are the handles collected unclosed (cleanupHandle); handles
	// counts those not released, and while there are any the timer also
	// fires at sweepAt to release orphans (session lock).
	orphans orphanQueue
	handles int
	pools   atomic.Int32 // MaintainPipes calls running
	// shapeFaults are the shaper panics already logged (session lock).
	shapeFaults uint64
	sweepAt     core.Time
	lctx        context.Context // the goroutine labels of the session (labels.go)

	events      eventQueue
	pipeWaiters []*core.Signal // AddPipe waiting for a join (woken on pipe state changes)

	unconfirmed []PipeIO          // carriers Unconfirmed at the acceptor
	cc          core.Config       // the session's resolved configuration
	labels      map[uint64]string // pipe labels
	lst         *Listener         // the listener that accepted the session
	nestedLanes int               // lanes that nested pipes hold on this session
	retiredIDs  []uint64          // pipes retired in this critical section
	retiredDone []uint64          // pipes retired in the previous one
	hk          *hooks            // application hooks (nil: none)
	redial      redialer
	// carrierVariant is the chosen candidate's Variant.
	carrierVariant string
	needRedial     bool        // a direction may have been lost (under the lock)
	dialed         Endpoint    // who dialed the bootstrap carrier; 0: unknown
	carrier        CarrierKind // the kind DialMode chose

	fired    bool // the timeout ran: rearm no earlier than G
	closing  bool // CLOSED: no new goroutines
	poisoned bool
	// A shaping deadline is met in place: the timer is armed lead before
	// it, by the host timer's observed lateness, and the callback waits
	// for the deadline itself.
	timerPrecise bool

	chIn    chIn // received channel datagrams for the session
	chInMax int  // the hand-off's length: the datagram receive queue's

	carrierTries []CandidateResult // DialMode's tries, in order
}

var sigPool = sync.Pool{New: func() any { s := make([]*core.Signal, 0, 16); return &s }}
var actPool = sync.Pool{New: func() any { s := make([]core.Action, 0, 4); return &s }}
var pendPool = sync.Pool{New: func() any { s := make([]pendingAct, 0, 4); return &s }}

// lock takes the session lock; the caller defers c.unlock().
func (c *Conn) lock() { c.mu.Lock() }

// unlock is the epilogue of every critical section: it converts a panic of
// the core into a closed session, collects the wake-ups, carrier commands
// and timer change, releases the lock and only then signals.
func (c *Conn) unlock() {
	r := recover()
	if r != nil {
		c.poison(r)
	} else {
		if !c.closing {
			c.releaseOrphans()
		}
		if c.s.SettlePending() {
			c.settle()
		}
	}
	wp := sigPool.Get().(*[]*core.Signal)
	ws := c.s.TakeWakes((*wp)[:0])
	ap := actPool.Get().(*[]core.Action)
	acts := c.s.TakeActions((*ap)[:0])
	c.drainEventsLocked()
	if f := c.s.ShapeFaults(); f > c.shapeFaults {
		c.shapeFaults = f
		c.hk.warn("zmux: a Shaper, its factory or the HandshakeShaper panicked; the panic was contained (ShapingStats.Faults)")
	}
	closed := c.s.State() == core.StateClosed && !c.closing
	if closed {
		c.closing = true
		c.err = sessionErr(c.s.Err())
		c.hk.trace(traceItem{closed: true, err: c.err})
	}
	hk := c.hk
	hk.note(c.s.HasWindowProposal(), c.s.WantsPick())
	redial := c.needRedial && !c.closing
	c.needRedial = false
	if r == nil {
		c.s.ChargeBudget()
	}
	if !c.closing {
		c.rearmLocked()
	} else {
		if c.timer != nil {
			c.timer.Stop()
		}
		if c.firstTimer != nil {
			// A held first flight ends with the session.
			c.firstTimer.Stop()
			c.firstTimer = nil
		}
	}
	pp := pendPool.Get().(*[]pendingAct)
	pend := (*pp)[:0]
	for _, a := range acts {
		pend = append(pend, c.resolveActionLocked(a))
	}
	clear(acts)
	*ap = acts[:0]
	actPool.Put(ap)
	c.mu.Unlock()
	if closed && c.lst != nil {
		// Unregistered before any waiter learns of the end, so that a
		// failed establishment returns with its registration gone.
		c.lst.sessionClosed(c)
	}
	core.FireAll(ws)
	clear(ws)
	*wp = ws[:0]
	sigPool.Put(wp)
	for _, pa := range pend {
		c.doAction(pa)
	}
	clear(pend)
	*pp = pend[:0]
	pendPool.Put(pp)
	hk.start()
	if redial {
		c.maybeRedial()
	}
	if closed {
		c.cancel(c.err)
		close(c.done)
		unregisterLaneKey(c)
		forgetNesting(c)
	}
	if r != nil {
		// The session is closed; the panic is not propagated to the caller,
		// which gets the session error.
		_ = r
	}
}

// settle runs the core's pending completion, converting a panic like the
// other critical sections do.
func (c *Conn) settle() {
	defer func() {
		if r := recover(); r != nil {
			c.poison(r)
		}
	}()
	c.s.Settle()
}

// poison closes the session after a panic under the lock without running
// more core logic than the close itself.
func (c *Conn) poison(r any) {
	if c.poisoned {
		return
	}
	c.poisoned = true
	defer func() { _ = recover() }()
	c.s.FailInternal(fmt.Sprint("internal error: ", r))
}

// releaseOrphans runs releaseOrphansLocked, converting a panic like the
// other critical sections do.
func (c *Conn) releaseOrphans() {
	if c.orphans.head.Load() == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.poison(r)
		}
	}()
	c.releaseOrphansLocked()
}

func (c *Conn) rearmLocked() {
	t := c.s.NextTimeout()
	switch {
	case c.handles == 0:
		c.sweepAt = 0
	case c.sweepAt == 0:
		c.sweepAt = c.clk.now().Add(core.Duration(orphanSweep))
		fallthrough
	default:
		t = min(t, c.sweepAt)
	}
	if c.fired {
		// A deadline the timeout just handled did not move: never fire
		// again at once (a busy loop), but G later.
		c.fired = false
		if now := c.clk.now(); t <= now {
			t = now.Add(core.Millisecond)
		}
	}
	if t == c.timerAt {
		return
	}
	c.timerAt = t
	if t == core.Never {
		if c.timer != nil {
			c.timer.Stop()
		}
		return
	}
	arm := t
	// Only the host clock is met in place: a configured clock moves on
	// its own terms.
	c.timerPrecise = c.cfg.ShapingPreciseTimer && c.clk.ext == nil && t == c.s.ShapingDeadline()
	if c.timerPrecise {
		arm = t.Add(-core.Duration(min(c.timerLate+c.timerLate/4+50*time.Microsecond, maxTimerLead)))
	}
	c.timerArm = arm
	d := c.clk.until(arm)
	if c.timer == nil {
		c.timer = c.clk.afterFunc(d, c.onTimer)
	} else {
		c.timer.Reset(d)
	}
}

// timerFires counts session timer callbacks (tests: quiet sessions fire
// none).
var timerFires atomic.Int64

// maxTimerLead bounds how long a timer callback waits in place for a
// shaping deadline: it yields in a loop (busy) for up to this long per
// deadline, the CPU cost of meeting shaping deadlines precisely
// (SetShaper).
const maxTimerLead = 2 * time.Millisecond

func (c *Conn) onTimer() {
	timerFires.Add(1)
	c.mu.Lock()
	now := c.clk.now()
	if late := time.Duration(now - c.timerArm); late >= 0 && late < time.Second {
		c.timerLate += (late - c.timerLate) / 8
	}
	at, precise := c.timerAt, c.timerPrecise
	c.mu.Unlock()
	if precise && at != core.Never && time.Duration(at-now) <= maxTimerLead {
		// Wait in place for the deadline the timer fired ahead of. A
		// callback that started before a rearm reads the new deadline,
		// which may be further away: it does not wait (the rearmed timer
		// fires for it).
		for c.clk.now() < at {
			runtime.Gosched()
		}
	}
	c.lock()
	defer c.unlock()
	if c.closing {
		return
	}
	c.timerAt = -1
	c.fired = true
	now = c.clk.now()
	if c.sweepAt != 0 && now >= c.sweepAt {
		c.sweepAt = 0 // the orphans are released in unlock; rearm moves it
	}
	c.s.HandleTimeout(now)

}

// Start establishes a session over any carrier with an explicit role. It
// returns once the peer's transport parameters were processed, or at once
// when the session may send early data (EarlyData.ZmuxPeer, or
// EarlyData.Replayable with Remembered parameters); ConnectionState's
// Established is false until the parameters arrive.
//
// The carrier belongs to zmux from the call on, whether Start succeeds or
// fails: on any error, ErrInvalidConfig included, its halves are closed
// before Start returns, and the caller does not close them. ctx bounds
// establishment only; the session's lifetime is Conn.Context, which does
// not inherit ctx.
//
// A session in the server or auto role started here has no Listener, so
// it cannot take joins: MaxPipes above 1 or ResumeTimeout is
// ErrInvalidConfig (serve such sessions with a Listener).
func Start(ctx context.Context, p PipeIO, role Role, cfg *Config) (*Conn, error) {
	c, err := start(ctx, p, role, cfg)
	if err != nil {
		if c == nil {
			closePipeIO(p)
		}
		return nil, err
	}
	return c, nil
}

func start(ctx context.Context, p PipeIO, role Role, cfg *Config) (*Conn, error) {
	if p.R == nil || p.W == nil {
		return nil, fmt.Errorf("%w: the bootstrap carrier needs both halves", ErrInvalidConfig)
	}
	if err := p.checkOverhead(); err != nil {
		return nil, err
	}
	cfg = cfg.Clone()
	transport := core.RoleInitiator
	crole := core.RoleInitiator
	switch role {
	case RoleClient:
	case RoleServer:
		transport, crole = core.RoleResponder, core.RoleResponder
	case RoleAuto:
		transport, crole = core.RoleAuto, core.RoleAuto
	default:
		return nil, fmt.Errorf("%w: role %d", ErrInvalidConfig, role)
	}
	cc, err := cfg.coreConfig(crole, transport)
	if err != nil {
		return nil, err
	}
	if _, ok := earlyCarrier(p, &cfg.EarlyData); ok && role == RoleClient {
		if !cfg.EarlyData.Replayable {
			// The carrier's early data can be replayed: the client
			// consents with Replayable.
			return nil, fmt.Errorf("%w: EarlyData.ZmuxPeer on a carrier with early data needs Replayable", ErrInvalidConfig)
		}
		cc.EarlyCarrier = true
	}
	if role != RoleClient && (cc.MaxPipes > 1 || cc.Resume > 0) {
		return nil, fmt.Errorf("%w: a session without a Listener cannot take joins: MaxPipes above 1 or ResumeTimeout in the server or auto role", ErrInvalidConfig)
	}
	return setupCore(ctx, p, cfg, cc, nil, nil)
}

// startCore starts a session from a resolved core configuration; lst is
// the listener that owns it, if any, and admitted the admission value of
// its bootstrap carrier. It closes p on every error.
func startCore(ctx context.Context, p PipeIO, cfg *Config, cc core.Config, lst *Listener, admitted any) (*Conn, error) {
	c, err := setupCore(ctx, p, cfg, cc, lst, admitted)
	if err != nil {
		if c == nil {
			closePipeIO(p)
		}
		return nil, err
	}
	return c, nil
}

// setupCore is startCore without the close: a Conn returned with an error
// is a session that closed its carrier itself.
func setupCore(ctx context.Context, p PipeIO, cfg *Config, cc core.Config, lst *Listener, admitted any) (*Conn, error) {
	boot := bootExporters(p, lst == nil && cc.TransportRole == core.RoleInitiator)
	var exp *lateExporter
	mode, secure := secureFirstMode(p)
	a := &cfg.PipeAuth
	if secure {
		// The source comes from the Establish alone; EndToEnd is accepted
		// and has no effect.
		if a.Exporter || len(a.PSK) > 0 || a.DeferredPSK {
			return nil, fmt.Errorf("%w: PipeAuth.PSK, DeferredPSK or Exporter on a secure first pipe, which is the secret source itself", ErrInvalidConfig)
		}
		if boot == nil {
			return nil, fmt.Errorf("%w: a secure first pipe without an exporter", ErrInvalidConfig)
		}
		cc.SecureFirstPipe, cc.FirstPipeMode, cc.Declared = true, join.Mode(mode), true
		if cc.Local.Has&wire.HasMultipath != 0 {
			cc.Local.SecretFlags = wire.SecretExporter
		}
		exp = boot
		cc.Exporter = exp.value
	} else if cc.Local.Has&wire.HasMultipath != 0 && !a.Exporter && len(a.PSK) == 0 && !a.DeferredPSK {
		// EndToEnd is no source: it vouches for the exporter or a deferred
		// PSK, it does not replace them.
		return nil, fmt.Errorf("%w: multipath and resume need a secret source (PipeAuth.PSK, DeferredPSK or Exporter, or a secure first pipe)", ErrInvalidConfig)
	}
	if !secure && a.Exporter {
		if boot == nil {
			return nil, fmt.Errorf("%w: PipeAuth.Exporter on a carrier without an exporter", ErrInvalidConfig)
		}
		exp = boot
		cc.Exporter = exp.value
	}
	c := newConn(cfg, cc)
	c.cc = cc
	c.exp, c.boot = exp, boot
	c.bootIO = PipeIO{R: p.R, W: p.W, Carrier: p.Carrier, Protocol: p.Protocol}
	c.lst = lst
	c.admitted = admitted
	switch {
	case lst != nil || cc.TransportRole == core.RoleResponder:
		c.dialed = EndpointPeer
	case cc.TransportRole == core.RoleInitiator:
		c.dialed = EndpointLocal
	}
	registerLaneKey(c)
	c.lock()
	// Publish the locator before bootstrap I/O can expose it to the peer.
	// AcceptJoin already holds joins until negotiation completes.
	if lst != nil {
		lst.mu.Lock()
		closed := lst.closed
		if !closed {
			lst.live[c] = struct{}{}
			if len(cc.Local.Locator) > 0 {
				lst.sessions[string(cc.Local.Locator)] = c
			}
		}
		lst.mu.Unlock()
		if closed {
			c.unlock()
			closePipeIO(p)
			c.closeWith(0, "listener closed")
			return c, errRejected
		}
	}
	if _, ok := earlyIn(p, &c.cfg.EarlyData); ok {
		c.s.CarrierEarlyIn()
	}
	if _, ok := earlyCarrier(p, &c.cfg.EarlyData); ok {
		c.verdictDone = make(chan struct{})
	}
	c.addBootstrap(p)
	if cc.HoldFirstFlight {
		c.firstTimer = c.clk.source().AfterFunc(cfg.handshakeTimeout(), func() { c.releaseFirst(true) })
	}
	early := cc.EarlyZmuxPeer || cc.EarlyRemembered != nil && cc.EarlyReplayable
	c.unlock()
	c.watchEarly(p)
	if early {
		return c, nil
	}
	if err := c.waitEstablished(ctx); err != nil {
		c.closeWith(0, "")
		return c, err
	}
	return c, nil
}

// lateExporter is the exporter value of PipeAuth.Exporter,
// read late: by the bootstrap pipe's reader, outside the session
// lock, before it delivers the first bytes of the carrier, which carry the
// peer's parameters, so that the carrier's own handshake has completed
// (the core asks for the value while it processes them). For a split
// bootstrap whose halves are two connections the value is the uplink's
// exporter followed by the downlink's.
type lateExporter struct {
	up, down KeyingMaterialExporter // either may be nil
	early    []EarlyServerCarrier   // the halves' carriers with early data
	once     sync.Once
	loaded   atomic.Bool
	b        []byte
	err      error
}

// bootExporters finds the exporters of p's halves (Exporter's rule per
// half); nil when neither has one.
func bootExporters(p PipeIO, initiator bool) *lateExporter {
	up, down := any(p.W), any(p.R) // the initiator writes on the uplink
	if !initiator {
		up, down = p.R, p.W
	}
	e := &lateExporter{}
	for _, h := range []any{p.W, p.R} {
		if es, ok := h.(EarlyServerCarrier); ok && (len(e.early) == 0 || !sameHalf(h, e.early[0])) {
			e.early = append(e.early, es)
		}
	}
	e.up, _ = exporterOf(up)
	if !sameHalf(unprefixed(down), unprefixed(up)) { // the halves of one carrier: one exporter
		e.down, _ = exporterOf(down)
	}
	if e.up == nil && e.down == nil {
		return nil
	}
	return e
}

// export reads the exporters: the one value of a carrier, or for a split
// bootstrap whose halves are two connections the uplink's value followed
// by the downlink's.
func (e *lateExporter) export(label string, ctxData []byte, length int) ([]byte, error) {
	u, err := readExporter(e.up, label, ctxData, length)
	if err != nil {
		return nil, err
	}
	d, err := readExporter(e.down, label, ctxData, length)
	if err != nil {
		return nil, err
	}
	switch {
	case u != nil && d != nil && !bytes.Equal(u, d):
		return append(u, d...), nil
	case u != nil:
		return u, nil
	}
	return d, nil
}

func readExporter(x KeyingMaterialExporter, label string, ctxData []byte, length int) ([]byte, error) {
	if x == nil {
		return nil, nil
	}
	return x.ExportKeyingMaterial(label, ctxData, length)
}

// load reads the exporter once; it calls the carrier, so never under the
// session lock. The first bytes of a carrier that accepted early data can
// come before the handshake gives the exporter (the client's Finished): it
// then waits for that handshake, which blocks only the caller, the
// bootstrap reader, whose bytes wait with it. It returns a failure of that
// handshake or the end of the session, which end the read; the wait is
// bounded by the session's handshake timeout (the peer's parameters are not
// in yet).
func (e *lateExporter) load(done <-chan struct{}) (err error) {
	e.once.Do(func() {
		e.b, e.err = e.export(wire.ExporterSession, nil, 32)
		if e.err != nil {
			if err = e.awaitHandshakes(done); err == nil {
				e.b, e.err = e.export(wire.ExporterSession, nil, 32)
			}
		}
		e.loaded.Store(true)
	})
	return err
}

// awaitHandshakes waits for the handshakes still running under early data.
func (e *lateExporter) awaitHandshakes(done <-chan struct{}) error {
	for _, es := range e.early {
		if !es.UsedEarlyData() {
			continue
		}
		var failed <-chan struct{}
		if f, ok := es.(interface{ HandshakeFailed() <-chan struct{} }); ok {
			failed = f.HandshakeFailed()
		}
		select {
		case <-es.HandshakeComplete():
		case <-failed:
			return errCarrierHandshakeFailed
		case <-done:
			return net.ErrClosed
		}
	}
	return nil
}

var errCarrierHandshakeFailed = errors.New("zmux: carrier handshake failed")

// value is the core's Exporter: the value load read.
func (e *lateExporter) value() ([]byte, error) {
	if !e.loaded.Load() {
		return nil, errors.New("not read before the peer's parameters")
	}
	return e.b, e.err
}

// Dial connects to addr on the named network (as net.Dialer.DialContext)
// and starts a client session on the connection, which zmux owns from
// then on, whether the session starts or not. It returns as Start does;
// ctx bounds the dial and establishment, not the session.
func Dial(ctx context.Context, network, addr string, cfg *Config) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return Client(ctx, nc, cfg)
}

// DialTLS connects to addr on the named network, runs the TLS handshake
// with tlsConf (as tls.Dialer.DialContext) and starts a client session on
// the TLS connection, which zmux owns from then on. ctx bounds the dial
// and both handshakes, not the session.
func DialTLS(ctx context.Context, network, addr string, tlsConf *tls.Config, cfg *Config) (*Conn, error) {
	d := tls.Dialer{Config: tlsConf}
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return Client(ctx, nc, cfg)
}

// Client starts a client session on a connection the caller established
// (TCP, TLS, Unix or any net.Conn), like tls.Client; it returns as Start
// does. With cfg.Credential, the connection starts with its prefix
// (PresentPipe; a *tls.Conn is handshaken first). zmux owns c from then
// on, whether Client succeeds or fails (as Start).
func Client(ctx context.Context, c net.Conn, cfg *Config) (*Conn, error) {
	p := NetPipe(c)
	if cfg != nil {
		if err := PresentPipe(ctx, p, cfg.Credential); err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	return Start(ctx, p, RoleClient, cfg)
}

// Server starts a single-pipe server session on an accepted connection,
// like tls.Server. For multipath, resume or pipe joins use a Listener
// (Listener.Serve accepts sessions and joins on a net.Listener): Server
// rejects Config.MaxPipes above 1 and ResumeTimeout with ErrInvalidConfig.
// zmux owns c from then on, whether Server succeeds or fails (as Start).
func Server(ctx context.Context, c net.Conn, cfg *Config) (*Conn, error) {
	return Start(ctx, NetPipe(c), RoleServer, cfg)
}

func newConn(cfg *Config, cc core.Config) *Conn {
	c := &Conn{cfg: cfg, clk: newClock(cfg.clock()), pipes: make(map[uint64]*pipeDrv, 1), done: make(chan struct{}),
		timerAt: core.Never, labels: map[uint64]string{}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.lctx = sessionLabels(cc.TransportRole)
	c.s = core.New(cc, c.clk.now())
	if cfg != nil && cfg.Shaper != nil {
		c.s.SetShaper(c.coreFactory(cfg.Shaper))
	}
	c.chInMax = c.s.Config().DgramRecvQ
	c.hk = newHooks(c)
	return c
}

func (c *Conn) waitEstablished(ctx context.Context) error {
	sig := core.NewSignal()
	for {
		c.lock()
		ready := c.s.WaitEstablished(sig)
		est, err := c.s.Established(), c.s.Err()
		c.unlock()
		if ready {
			if err != nil {
				return sessionErr(err)
			}
			if est {
				return nil
			}
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
		}
	}
}

// Context is canceled when the session ends; context.Cause returns its
// error. It does not inherit the context passed to Start, Client or Dial,
// which bounds establishment only.
func (c *Conn) Context() context.Context { return c.ctx }

// errLocked is the session's error, or net.ErrClosed (under the lock).
func (c *Conn) errLocked() error {
	if c.err != nil {
		return c.err
	}
	if e := c.s.Err(); e != nil {
		return sessionErr(e)
	}
	return net.ErrClosed
}

// closedErr returns the terminal error (call after done is closed).
func (c *Conn) closedErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if e := c.s.Err(); e != nil {
		return sessionErr(e)
	}
	return net.ErrClosed
}

// CloseWithError closes the session immediately with an application error,
// discarding data not yet sent. It writes CONNECTION_CLOSE to every usable
// outbound pipe, then ends each carrier's write direction gracefully (TCP
// FIN, TLS close_notify, the carrier's own close where it has no
// half-close) and keeps reading and discarding what the peer still sends,
// so that unread data does not turn the final close into a reset that
// could destroy CONNECTION_CLOSE at the peer. It returns once every
// carrier's read side has ended (the peer closed), or after the linger
// timeout, or at the latest one second after the call, whichever comes
// first; it does not otherwise wait for the peer.
//
// A process that exits as soon as CloseWithError returns has therefore
// handed CONNECTION_CLOSE to the carriers and half-closed them, and has
// drained them until the peer closed or for the time left of that second.
// Only if a carrier did not take CONNECTION_CLOSE or complete its
// half-close within the second (a peer that does not read) are the
// carriers closed at once. A session that keeps running drains in the
// background until the linger timeout (at most two seconds) and then
// closes the carriers.
//
// A repeated call returns at once once the close has completed. Wait waits
// for the session's end and its goroutines.
func (c *Conn) CloseWithError(code ApplicationErrorCode, msg string) error {
	c.closeWith(code, msg)
	c.awaitClose()
	return nil
}

// closeWith starts the immediate close; it cannot fail.
func (c *Conn) closeWith(code ApplicationErrorCode, msg string) {
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	c.s.Close(uint64(code), []byte(msg))
}

// closeLimit bounds how long a close waits for the carriers to take
// CONNECTION_CLOSE and to drain.
const closeLimit = time.Second

// awaitClose waits until the session has ended (the carriers drained, or
// the linger timeout passed) or closeLimit has passed. At the limit it
// ends the session unless every carrier took CONNECTION_CLOSE and
// completed its half-close; then the drain continues in the background.
// It never waits on a carrier itself: the half-close runs on the lane's
// writer (a TLS CloseWrite can block on a peer that does not read), and
// ending the session aborts the carriers, which releases it.
func (c *Conn) awaitClose() {
	t := time.NewTimer(closeLimit) // real time: a bound on carrier IO
	defer t.Stop()
	select {
	case <-c.done:
		return
	case <-t.C:
	}
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	done := c.s.CloseHandedOff()
	for _, pd := range c.pipes {
		if done && pd.shutdownRequested() && !pd.hc.wDone.Load() {
			done = false
		}
	}
	if !done {
		c.s.EndClose()
	}
}

// Close is CloseWithError(0, ""): it closes the session at once,
// discarding data not yet sent, and returns once CONNECTION_CLOSE has been
// handed to the carriers and they have been half-closed and drained (at
// most about one second, see CloseWithError). Unlike net.Conn.Close
// it does not flush: to let the peer read what was written, call
// Shutdown(ctx, 0, "") instead.
func (c *Conn) Close() error { return c.CloseWithError(0, "") }

// Shutdown closes gracefully: it sends GOAWAY when the session negotiated
// it, so that the peer opens no new stream, waits until the open streams
// have ended, then closes with code and msg. When ctx ends first
// it falls back to CloseWithError(code, msg) and returns ctx's error. The
// usual call is
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	err := conn.Shutdown(ctx, 0, "")
func (c *Conn) Shutdown(ctx context.Context, code ApplicationErrorCode, msg string) error {
	c.lock()
	c.s.SetNow(c.clk.now())
	c.s.Shutdown(uint64(code), []byte(msg))
	c.unlock()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		_ = c.CloseWithError(code, msg)
		return ctx.Err()
	}
}

// Wait blocks until the session is closed and every goroutine of it has
// ended, then returns the session's error.
func (c *Conn) Wait() error {
	<-c.done
	c.wg.Wait()
	return c.closedErr()
}

// Ping sends a QX_PING request (or joins the outstanding one) and returns
// the measured round-trip time.
func (c *Conn) Ping(ctx context.Context) (time.Duration, error) {
	sig := core.NewSignal()
	c.lock()
	c.s.SetNow(c.clk.now())
	ticket, err := c.s.Ping(sig)
	c.unlock()
	if err != nil {
		return 0, apiErr(err)
	}
	defer func() {
		c.lock()
		c.s.CancelPingWait(sig)
		c.unlock()
	}()
	for {
		c.lock()
		done, rtt, err := c.s.PingDone(ticket)
		if !done && err == nil {
			c.s.CancelPingWait(sig)
			sig.Drain()
			_, err = c.s.Ping(sig)
		}
		c.unlock()
		if done {
			return time.Duration(rtt), nil
		}
		if err != nil {
			return 0, apiErr(err)
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-c.done:
		}
	}
}

// QMuxVersion is the QMux version zmux implements, the one ConnectionState
// reports.
const QMuxVersion = "draft-02"

// ConnectionState reports the negotiation results.
type ConnectionState struct {
	Role        Role
	Established bool
	// Suspended: the session has no pipe and waits for one (resume).
	Suspended          bool
	SupportsDatagrams  struct{ Local, Remote bool }
	SupportsResetAt    struct{ Local, Remote bool }
	Multipath          bool
	PipeAck            bool
	Resume             bool
	JoinSecurityAbsent bool
	// JoinPending: there are no join keys yet, because Conn.SetJoinSecret has not
	// supplied the deferred PSK or an Unmanaged first pipe was not declared end to end.
	JoinPending bool
	// WeakestRecordMode is the mode of the session's weakest pipe.
	WeakestRecordMode RecordMode
	PeerIsZmux        bool
	// QMuxVersion is the QMux version in use (QMuxVersion, the only one).
	QMuxVersion string
	// ApplicationProtocol is the application protocol the bootstrap carrier
	// negotiated: PipeIO.Protocol (a WebSocket subprotocol, for example),
	// else, for a raw TLS or QUIC carrier, the ALPN of the handshake this
	// process ran for it; empty when neither gives one.
	ApplicationProtocol string
	// TLS is the state of the TLS (or QUIC) handshake this process ran for
	// the bootstrap carrier (TerminatedTLS); nil where TLS ended outside
	// the process or the carrier has none.
	TLS *tls.ConnectionState
	// PeerGoAway holds the watermarks of the GOAWAY frames received (the
	// minimum per stream type) and the latest error code; nil while none
	// was received. EventGoAwayReceived reports each one.
	PeerGoAway *GoAway
	// MaxDatagramPayload is the largest session datagram payload the peer
	// accepts now, 0 when it accepts none; Stream.MaxDatagramPayload gives
	// the one of a stream.
	MaxDatagramPayload int
}

// GoAway are the watermarks of a received GOAWAY:
// streams the receiver initiated with an index at or above MaxBidi
// (bidirectional) or MaxUni (unidirectional) were not and will not be
// processed by the peer, and fail with a retryable error.
type GoAway struct {
	MaxBidi, MaxUni uint64
	// ErrorCode is the GOAWAY's application error code.
	ErrorCode ApplicationErrorCode
}

// SetJoinSecret supplies the PSK of PipeAuth.DeferredPSK, at least 16 bytes, once: the
// join keys are derived from it (together with the exporter when both are configured)
// and the held joins of the peer go on. It fails with ErrInvalidConfig without
// DeferredPSK or with a shorter PSK, ErrNotNegotiated after an establishment without
// multipath, ErrJoinSecretSet when already supplied, and with the session's error
// once it is closed.
func (c *Conn) SetJoinSecret(psk []byte) error {
	if c == nil {
		return net.ErrClosed
	}
	if !c.cfg.PipeAuth.DeferredPSK {
		return fmt.Errorf("%w: SetJoinSecret without PipeAuth.DeferredPSK", ErrInvalidConfig)
	}
	if len(psk) < 16 {
		return fmt.Errorf("%w: join secret shorter than 16 bytes", ErrInvalidConfig)
	}
	c.lock()
	defer c.unlock()
	if c.closing {
		return c.errLocked()
	}
	return apiErr(c.s.SetJoinSecret(psk))
}

// ExportKeyingMaterial exports keying material (RFC 5705, RFC 8446
// Section 7.5) from the innermost end-to-end handshake of the bootstrap
// carrier, the source Exporter finds (the carrier's own exporter, a
// transform's, or the TLS handshake this process runs). For a split
// bootstrap whose halves are two connections the result is the uplink's
// value followed by the downlink's, 2*length bytes, as for
// PipeAuth.Exporter. It fails with ErrNoExporter when the carrier has
// none, ErrInvalidConfig for a label of zmux's own ("EXPORTER-zmux-"),
// which keys the joins, and with the carrier's error while its handshake
// is incomplete. Both ends obtain the same value. A value that binds a channel to
// the session takes at least 32 bytes and a label of its own: shorter ones can
// collide and identify nothing.
func (c *Conn) ExportKeyingMaterial(label string, ctxData []byte, length int) ([]byte, error) {
	if c == nil {
		return nil, net.ErrClosed
	}
	if strings.HasPrefix(label, "EXPORTER-zmux-") {
		return nil, fmt.Errorf("%w: exporter label %q is reserved", ErrInvalidConfig, label)
	}
	if length < 0 {
		return nil, fmt.Errorf("%w: exporter length %d is negative", ErrInvalidConfig, length)
	}
	if c.boot == nil {
		return nil, ErrNoExporter
	}
	return c.boot.export(label, ctxData, length)
}

// SessionBinding returns a 32-byte value that identifies this session,
// the same at both ends: HKDF-Extract (SHA-256) of the two Session
// Nonces under a fixed label. An application
// includes it in an authentication transcript to bind that
// authentication to this session. It is public, not a secret. It is nil
// before establishment and when multipath is not in use, whose parameter
// carries the nonces (ConnectionState().Multipath).
func (c *Conn) SessionBinding() []byte {
	c.lock()
	defer c.unlock()
	return c.s.SessionBinding()
}

// ConnectionState returns the negotiation results.
func (c *Conn) ConnectionState() ConnectionState {
	var st ConnectionState
	st.QMuxVersion = QMuxVersion
	// The carrier's state is read outside the session lock.
	if ts, ok := TerminatedTLS(c.bootIO); ok {
		st.TLS = &ts
		if k := c.bootIO.Carrier; k == 0 || k == CarrierQUIC {
			// Over HTTP the ALPN names the HTTP version.
			st.ApplicationProtocol = ts.NegotiatedProtocol
		}
	}
	if c.bootIO.Protocol != "" {
		st.ApplicationProtocol = c.bootIO.Protocol
	}
	c.lock()
	defer c.unlock()
	role, _ := c.s.Role()
	st.Role = RoleClient
	if role == core.RoleResponder {
		st.Role = RoleServer
	}
	st.Established = c.s.Established()
	st.Suspended = c.s.State() == core.StateSuspended
	f := c.s.Features()
	st.Multipath, st.PipeAck, st.Resume, st.JoinSecurityAbsent = f.Multipath, f.PipeAck, f.Resume, f.JoinSecurityUnavailable
	st.JoinPending = c.s.JoinPendingNow()
	if m, unmanaged := c.s.WeakestMode(); unmanaged {
		st.WeakestRecordMode = Unmanaged
	} else {
		st.WeakestRecordMode = RecordMode(m)
	}
	l := c.s.LocalParams()
	st.SupportsDatagrams.Local = l.Has&wire.HasDatagram != 0
	st.SupportsResetAt.Local = l.Has&wire.HasResetAt != 0
	if p, ok := c.s.PeerParams(); ok {
		st.SupportsDatagrams.Remote = p.Has&wire.HasDatagram != 0 && p.MaxDatagramFrameSize > 0
		st.SupportsResetAt.Remote = p.Has&wire.HasResetAt != 0
		st.PeerIsZmux = p.IsZmux()
	}
	st.MaxDatagramPayload = c.s.MaxDatagramPayload(1)
	if g, ok := c.s.PeerGoAway(); ok {
		st.PeerGoAway = &GoAway{MaxBidi: g.MaxBidi, MaxUni: g.MaxUni, ErrorCode: ApplicationErrorCode(g.Code)}
	}
	return st
}

// ConnectionStats is a snapshot of a session's statistics.
// Each field holds a value it had during the call; relations between
// counts (a subset, a sum) hold for their final values, not in every
// snapshot. Counts only increase, from 0 when the session was made;
// the fields marked gauge are current values. A duration or RTT that is
// absent (before the first sample) is 0.
type ConnectionStats struct {
	// Carrier is the carrier kind a carrier mode chose (DialMode), the
	// latest after a switch; 0 for a session not dialed in a mode. The
	// chosen candidate's Variant is its pipe's PipeStats.Variant.
	Carrier CarrierKind
	// CarrierCandidates are the tries of the mode's candidates in order,
	// joins of preferred kinds included: the latest 64.
	CarrierCandidates []CandidateResult

	// BytesSent and BytesReceived count every byte of the records passed
	// to and received from the carriers (framing, frames of every kind,
	// copies and acknowledgements included), not what a carrier adds.
	BytesSent, BytesReceived     uint64
	RecordsSent, RecordsReceived uint64

	// The application bytes: stream payload the application
	// wrote and read, and datagram payload handed to a carrier and
	// returned to the application.
	BytesWritten, BytesRead                  uint64
	DatagramBytesSent, DatagramBytesReceived uint64

	// StreamsOpenedLocal counts a local stream when its first frame is
	// emitted or it is pinned, StreamsOpenedRemote a peer stream when it is
	// first seen.
	StreamsOpenedLocal, StreamsOpenedRemote uint64
	StreamsActive                           uint64 // gauge: streams the session holds
	StreamCreditWaits                       uint64 // opens that waited for stream-count credit

	// DatagramsSent counts session and stream datagrams handed to a
	// carrier, DatagramsReceived those received and queued for the
	// application; the Channel counts are those of them on a carrier
	// datagram channel (DatagramChannel).
	DatagramsSent, DatagramsReceived               uint64
	DatagramsSentChannel, DatagramsReceivedChannel uint64
	// DatagramsDroppedSend counts datagrams dropped from the send queue,
	// DatagramsDroppedRecv received ones dropped (a full queue, an unknown
	// or closed stream, the byte limit), DatagramsDroppedLimit those a
	// datagram limiter dropped, either way.
	DatagramsDroppedSend, DatagramsDroppedRecv, DatagramsDroppedLimit uint64

	NoEffectBudget uint64 // gauge: tokens of the no-effect budget

	// The RTT of pipe 0 outside multipath, of the primary pipe in a
	// multipath session; MeanDeviation is 0 where the source measures no
	// variation.
	SmoothedRTT, MinRTT, LatestRTT, MeanDeviation time.Duration

	// BytesReinjected counts stream bytes sent again after a pipe failed
	// or became suspect, or opportunistically; BytesOpportunistic those
	// of them sent opportunistically.
	BytesReinjected, BytesOpportunistic uint64
	Penalties                           uint64 // pipes penalized for slowing down
	Reinjections                        uint64 // reinjection episodes
	BytesRedundant                      uint64 // stream bytes sent as redundant copies
	BytesDuplicateReceived              uint64 // stream bytes received again (multipath)

	SendBuffered uint64 // gauge: written stream bytes not yet sent
	RecvBuffered uint64 // gauge: received stream bytes not yet read
	Retained     uint64 // gauge: sent stream bytes not yet acknowledged (multipath)

	PipesActive      uint64 // gauge: ACTIVE outbound pipes
	PipesAdded       uint64 // attachments, the bootstrap pair included
	PipeFailures     uint64
	JoinAuthFailures uint64 // Hellos that did not verify
	JoinReplays      uint64 // joins closed as replays or too late
	// JoinPending counts peer joins held for the join keys (not join auth failures).
	JoinPending uint64
	// JoinFailures counts own consecutive failed joins, reset by a Proven join.
	JoinFailures uint64
	// JoinBatchDropped counts first batch frames dropped without closing the session.
	JoinBatchDropped uint64
	JoinRefusals     uint64 // own joins refused with ZMUX_PIPE_RESOURCE_LIMIT, or answered with ZMUX_RESUME_FAILED
	Suspensions      uint64
	SuspendedTime    time.Duration // the current suspension included

	SchedulerAnswers   uint64 // custom scheduler answers that named a pipe and were applied; Wait is not counted
	SchedulerFallbacks uint64 // answers replaced by the affinity choice
	// FirstFlightTimeouts is 1 when EarlyData.HoldFirstFlight was released
	// by Config.HandshakeTimeout rather than ReleaseFirstFlight.
	FirstFlightTimeouts uint64
}

// ConnectionStats returns a snapshot of the session's statistics.
func (c *Conn) ConnectionStats() ConnectionStats {
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now()) // SuspendedTime is current
	s, g := c.s.SessionStats(), c.s.Gauges()
	return ConnectionStats{Carrier: c.carrier, CarrierCandidates: slices.Clone(c.carrierTries),
		BytesSent: s.BytesSent, BytesReceived: s.BytesReceived, RecordsSent: s.RecordsSent,
		RecordsReceived: s.RecordsReceived, BytesWritten: s.AppBytesWritten, BytesRead: s.AppBytesRead,
		DatagramBytesSent: s.DatagramBytesSent, DatagramBytesReceived: s.DatagramBytesReceived,
		StreamsOpenedLocal: s.StreamsOpenedLocal, StreamsOpenedRemote: s.StreamsOpenedRemote,
		StreamsActive: g.StreamsActive, StreamCreditWaits: s.StreamCreditWaits,
		DatagramsSent: s.DatagramsSent, DatagramsReceived: s.DatagramsReceived,
		DatagramsSentChannel: s.DatagramsSentChannel, DatagramsReceivedChannel: s.DatagramsReceivedChannel,
		DatagramsDroppedSend: s.DatagramsDroppedSend, DatagramsDroppedRecv: s.DatagramsDroppedRecv,
		DatagramsDroppedLimit: s.DatagramsDroppedLimit, NoEffectBudget: c.s.BudgetTokens(),
		SmoothedRTT: time.Duration(g.SmoothedRTT), MinRTT: time.Duration(g.MinRTT),
		LatestRTT: time.Duration(g.LatestRTT), MeanDeviation: time.Duration(g.MeanDeviation),
		BytesReinjected: s.BytesReinjected, BytesOpportunistic: s.BytesOpportunistic, Penalties: s.Penalties,
		Reinjections: s.Reinjections, BytesRedundant: s.BytesRedundant, BytesDuplicateReceived: s.BytesDuplicateReceived,
		SendBuffered: g.SendBuffered, RecvBuffered: g.RecvBuffered, Retained: g.Retained,
		PipesActive: g.PipesActive, PipesAdded: s.PipesAdded, PipeFailures: s.PipeFailures,
		JoinAuthFailures: s.JoinAuthFailures, JoinReplays: s.JoinReplays, JoinPending: s.JoinPending, JoinFailures: c.s.JoinFailures(), JoinBatchDropped: s.JoinBatchDropped, JoinRefusals: s.JoinRefusals,
		Suspensions: s.Suspensions, SuspendedTime: time.Duration(s.SuspendedTime),
		SchedulerAnswers: s.SchedulerAnswers, SchedulerFallbacks: s.SchedulerFallbacks,
		FirstFlightTimeouts: c.firstTimeouts}
}

// ReleaseFirstFlight ends EarlyData.HoldFirstFlight: everything queued so
// far is built into records and written to the bootstrap carrier in one
// write (one Write or WriteBuffers on a byte stream, one WriteMessages
// where the half is a MessageBatchWriter), as far as one batch holds it.
// Calls after the first, and on a session without HoldFirstFlight, do
// nothing. It never blocks.
func (c *Conn) ReleaseFirstFlight() { c.releaseFirst(false) }

// releaseFirst releases the first flight; timeout: by the handshake
// timeout.
func (c *Conn) releaseFirst(timeout bool) {
	c.lock()
	defer c.unlock()
	if c.firstTimer != nil {
		c.firstTimer.Stop()
		c.firstTimer = nil
	}
	if c.s.ReleaseFirstFlight(c.clk.now()) && timeout {
		c.firstTimeouts++
	}
}

// Admitted returns the Decision.Value of the admission that accepted the
// session's bootstrap carrier (for a split bootstrap, its uplink half's),
// nil without one and on the dialing side. It does not change for the
// session's lifetime, resumes included; Pipe.Admitted is each pipe's.
func (c *Conn) Admitted() any { return c.admitted }

// LocalAddr returns the local address of pipe 0 when its carrier has one.
func (c *Conn) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pd := c.pipes[0]; pd != nil {
		if nc, ok := pd.hc.io.W.(net.Conn); ok {
			return nc.LocalAddr()
		}
	}
	return nil
}

// RemoteAddr returns the remote address of pipe 0 when its carrier has one.
func (c *Conn) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pd := c.pipes[0]; pd != nil {
		if nc, ok := pd.hc.io.W.(net.Conn); ok {
			return nc.RemoteAddr()
		}
	}
	return nil
}

// ---- datagrams ----

// SendDatagram queues a session datagram; it never blocks: a full queue
// drops its oldest datagram. p is copied.
func (c *Conn) SendDatagram(p []byte) error { return c.sendDatagram(nil, 0, p) }

// sendDatagram queues a session (st nil) or stream datagram through the
// send limiter, which runs outside the lock.
func (c *Conn) sendDatagram(st *core.Stream, cx uint64, p []byte) error {
	lim := c.cfg.DatagramLimiter.Send
	if lim != nil && lim.TryReady(len(p)) < len(p) {
		c.lock()
		c.s.DeliverDatagram(core.PendingDatagram{}, true)
		c.unlock()
		return nil
	}
	c.lock()
	err := c.s.SendDatagram(st, cx, p)
	c.unlock()
	if err == nil && lim != nil {
		lim.Charge(len(p))
	}
	return apiErr(err)
}

// SendDatagramWait queues a session datagram, waiting for room in the send
// queue instead of dropping its oldest entry.
func (c *Conn) SendDatagramWait(ctx context.Context, p []byte) error {
	return c.sendDatagramWait(ctx, nil, 0, p)
}

// sendDatagramWait waits for room in the send queue, then queues a session
// (st nil) or stream datagram.
func (c *Conn) sendDatagramWait(ctx context.Context, st *core.Stream, cx uint64, p []byte) error {
	sig := core.NewSignal()
	defer func() {
		c.lock()
		c.s.CancelDatagramSendWait(sig)
		c.unlock()
	}()
	for {
		c.lock()
		full := c.s.DatagramQueueFull() && c.s.Err() == nil
		if full {
			sig.Drain()
			c.s.CancelDatagramSendWait(sig)
			c.s.WaitDatagramSend(sig)
		}
		c.unlock()
		if !full {
			return c.sendDatagram(st, cx, p)
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.closedErr()
		}
	}
}

// passDatagrams runs received datagrams through the receive limiter,
// outside the lock.
func (c *Conn) passDatagrams() {
	lim := c.cfg.DatagramLimiter.Receive
	if lim == nil {
		return
	}
	c.lock()
	ds := c.s.TakeLimitedDatagrams(nil)
	c.unlock()
	if len(ds) == 0 {
		return
	}
	pass := make([]bool, len(ds))
	for i := range ds {
		n := ds[i].Len()
		if lim.TryReady(n) >= n {
			lim.Charge(n)
			pass[i] = true
		}
	}
	c.lock()
	for i := range ds {
		c.s.DeliverDatagram(ds[i], !pass[i])
	}
	c.unlock()
}

// ReceiveDatagram returns the next session datagram.
func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	sig := core.NewSignal()
	defer func() {
		c.lock()
		c.s.CancelDatagramWait(sig)
		c.unlock()
	}()
	for {
		c.lock()
		d, ok := c.s.RecvDatagram()
		var err error
		if !ok {
			if e := c.s.Err(); e != nil {
				err = sessionErr(e)
			} else {
				sig.Drain()
				c.s.CancelDatagramWait(sig)
				c.s.WaitDatagram(sig)
			}
		}
		c.unlock()
		if ok {
			return d, nil
		}
		if err != nil {
			return nil, err
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// ---- streams ----

// OpenStream opens a bidirectional stream without waiting; without stream
// credit it returns StreamLimitReachedError. Nothing is sent before the
// first write (or StreamID).
func (c *Conn) OpenStream() (*Stream, error) {
	h, err := c.open(context.Background(), true, false)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}

// OpenStreamSync opens a bidirectional stream, waiting for credit in FIFO
// order.
func (c *Conn) OpenStreamSync(ctx context.Context) (*Stream, error) {
	h, err := c.open(ctx, true, true)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}

// OpenUniStream opens a unidirectional stream without waiting.
func (c *Conn) OpenUniStream() (*SendStream, error) {
	h, err := c.open(context.Background(), false, false)
	if err != nil {
		return nil, err
	}
	return newSendStream(h), nil
}

// OpenUniStreamSync opens a unidirectional stream, waiting for credit.
func (c *Conn) OpenUniStreamSync(ctx context.Context) (*SendStream, error) {
	h, err := c.open(ctx, false, true)
	if err != nil {
		return nil, err
	}
	return newSendStream(h), nil
}

// open opens a stream; with wait it waits for credit until ctx ends.
func (c *Conn) open(ctx context.Context, bidi, wait bool) (*handle, error) {
	var sig *core.Signal
	defer func() {
		if sig != nil {
			c.lock()
			c.s.CancelOpenWait(bidi, sig)
			c.unlock()
		}
	}()
	for {
		c.lock()
		c.s.SetNow(c.clk.now())
		st, err := c.s.OpenStream(bidi)
		if errors.Is(err, core.ErrStreamLimit) && wait && ctx != nil {
			if sig == nil {
				sig = core.NewSignal()
				c.s.CountCreditWait()
			} else {
				c.s.CancelOpenWait(bidi, sig)
			}
			sig.Drain()
			c.s.WaitOpen(bidi, sig)
			err = nil
		}
		var h *handle
		if st != nil {
			h = c.newHandle(st)
		}
		c.unlock()
		if st != nil {
			return h, nil
		}
		if err != nil {
			return nil, apiErr(err)
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
		}
	}
}

// AcceptStream returns the next bidirectional stream the peer opened.
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	h, err := c.accept(ctx, true)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}

// AcceptUniStream returns the next unidirectional stream the peer opened.
// The first call grants the staged unidirectional credit.
func (c *Conn) AcceptUniStream(ctx context.Context) (*ReceiveStream, error) {
	h, err := c.accept(ctx, false)
	if err != nil {
		return nil, err
	}
	return newReceiveStream(h), nil
}

func (c *Conn) accept(ctx context.Context, bidi bool) (*handle, error) {
	sig := core.NewSignal()
	registered := false
	defer func() {
		if registered {
			c.lock()
			c.s.CancelAcceptWait(bidi, sig)
			c.unlock()
		}
	}()
	for {
		c.lock()
		st, ok := c.s.AcceptStream(bidi)
		var h *handle
		var err error
		if ok {
			h = c.newHandle(st)
		} else if e := c.s.Err(); e != nil {
			err = sessionErr(e)
		} else {
			if registered {
				c.s.CancelAcceptWait(bidi, sig)
			}
			sig.Drain()
			c.s.WaitAccept(bidi, sig)
			registered = true
		}
		c.unlock()
		if h != nil {
			return h, nil
		}
		if err != nil {
			return nil, err
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
