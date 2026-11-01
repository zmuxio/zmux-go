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
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
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
	// firstTimer releases a held first flight at the handshake timeout;
	// firstTimeouts counts that (session lock).
	firstTimer    timeshim.Timer
	firstTimeouts uint64

	// admitted is the admission value of the bootstrap carrier (set
	// before the session is shared, never changed).
	admitted any

	orphMu    sync.Mutex
	orphans   []*core.Stream
	orphanSig atomic.Pointer[core.Signal]

	events      eventQueue
	pipeWaiters []*core.Signal // AddPipe waiting for a join (woken on pipe state changes)

	oobHeaders  map[uint64]string // Zmux-Pipe-Join values of reservations
	labels      map[uint64]string // pipe labels
	lst         *Listener         // the listener that accepted the session
	nestedLanes int               // lanes that nested pipes hold on this session
	retiredIDs  []uint64          // pipes retired in this critical section
	retiredDone []uint64          // pipes retired in the previous one
	hk          *hooks            // application hooks (nil: none)
	redial      redialer
	needRedial  bool        // a direction may have been lost (under the lock)
	dialed      Endpoint    // who dialed the bootstrap carrier; 0: unknown
	carrier     CarrierKind // the kind DialMode chose
	// carrierVariant is the chosen candidate's Variant.
	carrierVariant string

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
	} else if c.s.SettlePending() {
		c.settle()
	}
	wp := sigPool.Get().(*[]*core.Signal)
	ws := c.s.TakeWakes((*wp)[:0])
	ap := actPool.Get().(*[]core.Action)
	acts := c.s.TakeActions((*ap)[:0])
	c.drainEventsLocked()
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
	c.s.Close(uint64(TransportInternalError), []byte(fmt.Sprint("internal error: ", r)))
}

func (c *Conn) rearmLocked() {
	t := c.s.NextTimeout()
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
	c.timerPrecise = c.clk.ext == nil && t == c.s.ShapingDeadline()
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
	c.s.HandleTimeout(c.clk.now())

}

// Start establishes a session over any carrier with an explicit role. It
// returns once the peer's transport parameters were processed, or at once
// when the session may send early data (EarlyData.ZmuxPeer, or
// EarlyData.Replayable with Remembered parameters); ConnectionState's
// Established is false until the parameters arrive.
func Start(ctx context.Context, p PipeIO, role Role, cfg *Config) (*Conn, error) {
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
	return startCore(ctx, p, cfg, cc, nil, nil)
}

// startCore starts a session from a resolved core configuration; lst is
// the listener that owns it, if any, and admitted the admission value of
// its bootstrap carrier.
func startCore(ctx context.Context, p PipeIO, cfg *Config, cc core.Config, lst *Listener, admitted any) (*Conn, error) {
	var exp *lateExporter
	if cfg.PipeAuth.Exporter {
		var err error
		exp, err = sessionExporter(p, lst == nil && cc.TransportRole == core.RoleInitiator)
		if err != nil {
			return nil, err
		}
		cc.Exporter = exp.value
	}
	c := newConn(cfg, cc)
	c.exp = exp
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
			return nil, errRejected
		}
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
		return nil, err
	}
	return c, nil
}

// lateExporter is the exporter value of PipeAuth.Exporter (MULTIPATH.md
// 10.2), read late: by the bootstrap pipe's reader, outside the session
// lock, before it delivers the first bytes of the carrier, which carry the
// peer's parameters, so that the carrier's own handshake has completed
// (the core asks for the value while it processes them). For a split
// bootstrap whose halves are two connections the value is the uplink's
// exporter followed by the downlink's.
type lateExporter struct {
	up, down KeyingMaterialExporter // either may be nil
	once     sync.Once
	b        []byte
	err      error
	loaded   atomic.Bool
}

// sessionExporter finds the exporters of p's halves; ErrInvalidConfig
// when neither has one.
func sessionExporter(p PipeIO, initiator bool) (*lateExporter, error) {
	up, down := any(p.W), any(p.R) // the initiator writes on the uplink
	if !initiator {
		up, down = p.R, p.W
	}
	e := &lateExporter{}
	e.up, _ = exporterOf(up)
	if down != up { // the halves of one carrier: one exporter
		e.down, _ = exporterOf(down)
	}
	if e.up == nil && e.down == nil {
		return nil, fmt.Errorf("%w: PipeAuth.Exporter on a carrier without an exporter", ErrInvalidConfig)
	}
	return e, nil
}

// load reads the exporter once; it calls the carrier, so never under the
// session lock.
func (e *lateExporter) load() {
	e.once.Do(func() {
		read := func(x KeyingMaterialExporter) []byte {
			if x == nil || e.err != nil {
				return nil
			}
			b, err := x.ExportKeyingMaterial(wire.ExporterSession, nil, 32)
			if err != nil {
				e.err = err
			}
			return b
		}
		u, d := read(e.up), read(e.down)
		switch {
		case e.err != nil:
		case u != nil && d != nil && !bytes.Equal(u, d):
			e.b = append(u, d...) // two connections: a split bootstrap
		case u != nil:
			e.b = u
		default:
			e.b = d
		}
		e.loaded.Store(true)
	})
}

// value is the core's Exporter: the value load read.
func (e *lateExporter) value() ([]byte, error) {
	if !e.loaded.Load() {
		return nil, errors.New("not read before the peer's parameters")
	}
	return e.b, e.err
}

// Dial connects to addr on the named network (as net.Dialer.DialContext)
// and starts a client session on the connection, which the session owns
// from then on. It returns as Start does; ctx bounds the dial and the
// handshake.
func Dial(ctx context.Context, network, addr string, cfg *Config) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return clientOwning(ctx, nc, cfg)
}

// DialTLS connects to addr on the named network, runs the TLS handshake
// with tlsConf (as tls.Dialer.DialContext) and starts a client session on
// the TLS connection, which the session owns from then on. ctx bounds the
// dial and both handshakes.
func DialTLS(ctx context.Context, network, addr string, tlsConf *tls.Config, cfg *Config) (*Conn, error) {
	d := tls.Dialer{Config: tlsConf}
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return clientOwning(ctx, nc, cfg)
}

// clientOwning starts a client session on a connection it dialled, closing
// it when the session cannot start.
func clientOwning(ctx context.Context, nc net.Conn, cfg *Config) (*Conn, error) {
	c, err := Client(ctx, nc, cfg)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	return c, nil
}

// Client starts a client session on a connection the caller established
// (TCP, TLS, Unix or any net.Conn), like tls.Client; it returns as Start
// does. With cfg.Credential, the connection starts with its prefix
// (PresentPipe; a *tls.Conn is handshaken first). The session owns c from
// then on, unless Client fails with ErrInvalidConfig.
func Client(ctx context.Context, c net.Conn, cfg *Config) (*Conn, error) {
	p := NetPipe(c)
	if cfg != nil {
		if err := PresentPipe(ctx, p, cfg.Credential); err != nil {
			if !errors.Is(err, ErrInvalidConfig) {
				_ = c.Close()
			}
			return nil, err
		}
	}
	return Start(ctx, p, RoleClient, cfg)
}

// Server starts a single-pipe server session on an accepted connection,
// like tls.Server. For multipath, resume or pipe joins use a Listener
// (Listener.Serve accepts sessions and joins on a net.Listener): Server
// rejects Config.MaxPipes above 1 with ErrInvalidConfig. The session owns
// c from then on, unless Server fails with ErrInvalidConfig.
func Server(ctx context.Context, c net.Conn, cfg *Config) (*Conn, error) {
	if cfg != nil && cfg.MaxPipes > 1 {
		return nil, fmt.Errorf("%w: Server cannot accept joins; use a Listener", ErrInvalidConfig)
	}
	return Start(ctx, NetPipe(c), RoleServer, cfg)
}

func newConn(cfg *Config, cc core.Config) *Conn {
	c := &Conn{cfg: cfg, clk: newClock(cfg.clock()), pipes: make(map[uint64]*pipeDrv, 1), done: make(chan struct{}),
		timerAt: core.Never, oobHeaders: map[uint64]string{}, labels: map[uint64]string{}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
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
// error.
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

// CloseWithError closes the session immediately with an application error;
// it never blocks. Unsent data is discarded.
func (c *Conn) CloseWithError(code ApplicationErrorCode, msg string) error {
	c.closeWith(code, msg)
	return nil
}

// closeWith starts the immediate close; it cannot fail.
func (c *Conn) closeWith(code ApplicationErrorCode, msg string) {
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	c.s.Close(uint64(code), []byte(msg))
}

// Close closes the session at once with application error code 0,
// discarding data not yet sent; it never blocks. Unlike net.Conn.Close it
// does not flush: to let the peer read what was written, call
// Shutdown(ctx, 0, "") instead. Wait waits for the end.
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
		c.closeWith(code, msg)
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

// ConnectionState reports the negotiation results.
type ConnectionState struct {
	Role               Role
	Established        bool
	SupportsDatagrams  struct{ Local, Remote bool }
	SupportsResetAt    struct{ Local, Remote bool }
	Multipath          bool
	PipeAck            bool
	Resume             bool
	JoinSecurityAbsent bool
	PeerIsZmux         bool
	// PeerGoAway holds the watermarks of the GOAWAY frames received (the
	// minimum per stream type) and the latest error code; nil while none
	// was received. EventGoAwayReceived reports each one.
	PeerGoAway         *GoAway
	MaxDatagramPayload int
}

// GoAway are the watermarks of a received GOAWAY (EXTENSIONS.md Section 3):
// streams the receiver initiated with an index at or above MaxBidi
// (bidirectional) or MaxUni (unidirectional) were not and will not be
// processed by the peer, and fail with a retryable error.
type GoAway struct {
	MaxBidi, MaxUni uint64
	// ErrorCode is the GOAWAY's application error code.
	ErrorCode ApplicationErrorCode
}

// ConnectionState returns the negotiation results.
func (c *Conn) ConnectionState() ConnectionState {
	c.lock()
	defer c.unlock()
	var st ConnectionState
	role, _ := c.s.Role()
	st.Role = RoleClient
	if role == core.RoleResponder {
		st.Role = RoleServer
	}
	st.Established = c.s.Established()
	f := c.s.Features()
	st.Multipath, st.PipeAck, st.Resume, st.JoinSecurityAbsent = f.Multipath, f.PipeAck, f.Resume, f.JoinSecurityUnavailable
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

// ConnectionStats is a snapshot of a session's statistics (API.md 7.2).
// Each field holds a value it had during the call; relations between
// counts (a subset, a sum) hold for their final values, not in every
// snapshot. Counts only increase, from 0 when the session was made;
// the fields marked gauge are current values. A duration or RTT that is
// absent (before the first sample) is 0.
type ConnectionStats struct {
	// Carrier is the carrier kind a carrier mode chose (DialMode), the
	// latest after a switch; 0 for a session not dialed in a mode.
	// CarrierVariant is that candidate's Variant.
	Carrier        CarrierKind
	CarrierVariant string
	// CarrierCandidates are the tries of the mode's candidates in order,
	// joins of preferred kinds included: the latest 64.
	CarrierCandidates []CandidateResult

	// BytesSent and BytesReceived count every byte of the records passed
	// to and received from the carriers (framing, frames of every kind,
	// copies and acknowledgements included), not what a carrier adds.
	BytesSent, BytesReceived     uint64
	RecordsSent, RecordsReceived uint64

	// The application bytes (API.md 7.1): stream payload the application
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
	JoinAuthFailures uint64 // joins whose token did not verify
	JoinReplays      uint64 // joins closed as replays or too late
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
	return ConnectionStats{Carrier: c.carrier, CarrierVariant: c.carrierVariant, CarrierCandidates: slices.Clone(c.carrierTries),
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
		JoinAuthFailures: s.JoinAuthFailures, JoinReplays: s.JoinReplays, JoinRefusals: s.JoinRefusals,
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
