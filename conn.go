package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

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
// Concurrency model (GO-DESIGN.md): one mutex guards the sans-IO core; no
// I/O, blocking or application code runs under it. Each pipe direction has
// a reader and a writer goroutine; wake-ups are queued under the lock and
// delivered after it is released.
type Conn struct {
	mu  sync.Mutex
	s   *core.Session
	clk clock
	cfg *Config

	timer   *time.Timer
	timerAt core.Time
	fired   bool // the timeout ran: rearm no earlier than G

	pipes   map[uint64]*pipeDrv
	wg      sync.WaitGroup
	closing bool // CLOSED: no new goroutines
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelCauseFunc
	err     error // terminal error once done

	poisoned bool

	estSig *core.Signal

	orphMu    sync.Mutex
	orphans   []*core.Stream
	orphanSig atomic.Pointer[core.Signal]

	events      eventQueue
	pipeWaiters []*core.Signal    // AddPipe waiting for a join (woken on pipe state changes)
	oobHeaders  map[uint64]string // Zmux-Pipe-Join values of reservations
	labels      map[uint64]string // pipe labels
	lst         *Listener         // the listener that accepted the session
	nestedLanes int               // lanes that nested pipes hold on this session (API.md 4.6)
	retiredIDs  []uint64          // pipes retired in this critical section
	retiredDone []uint64          // pipes retired in the previous one
	hk          *hooks            // application hooks (nil: none)
	redial      redialer
	needRedial  bool // a direction may have been lost (under the lock)
	dialed      dialer
}

var sigPool = sync.Pool{New: func() any { s := make([]*core.Signal, 0, 16); return &s }}
var actPool = sync.Pool{New: func() any { s := make([]core.Action, 0, 4); return &s }}

// lock takes the session lock; the caller defers c.unlock().
func (c *Conn) lock() { c.mu.Lock() }

// unlock is the epilogue of every critical section: it converts a panic of
// the core into a closed session, collects the wake-ups, carrier commands
// and timer change, releases the lock and only then signals.
func (c *Conn) unlock() {
	r := recover()
	if r != nil {
		c.poison(r)
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
	} else if c.timer != nil {
		c.timer.Stop()
	}
	c.mu.Unlock()
	core.FireAll(ws)
	clear(ws)
	*wp = ws[:0]
	sigPool.Put(wp)
	for _, a := range acts {
		c.doAction(a)
	}
	clear(acts)
	*ap = acts[:0]
	actPool.Put(ap)
	hk.start()
	if redial {
		c.maybeRedial()
	}
	if closed {
		c.cancel(c.err)
		close(c.done)
		unregisterLaneKey(c)
		forgetNesting(c)
		if c.lst != nil {
			c.lst.sessionClosed(c)
		}
	}
	if r != nil {
		// The session is closed; the panic is not propagated to the caller,
		// which gets the session error (IMPLEMENTATION.md 2.5.6).
		_ = r
	}
}

// poison closes the session after a panic under the lock without running
// more core logic than the close itself.
func (c *Conn) poison(r any) {
	if c.poisoned {
		return
	}
	c.poisoned = true
	defer func() { _ = recover() }()
	c.s.Close(uint64(InternalError), []byte(fmt.Sprint("internal error: ", r)))
}

func (c *Conn) rearmLocked() {
	t := c.s.NextTimeout()
	if c.fired {
		// A deadline the timeout just handled did not move: never fire
		// again at once (a busy loop), but G later (SCHEDULER.md 9.1).
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
	d := c.clk.until(t)
	if c.timer == nil {
		c.timer = time.AfterFunc(d, c.onTimer)
	} else {
		c.timer.Reset(d)
	}
}

// timerFires counts session timer callbacks (tests: quiet sessions fire
// none).
var timerFires atomic.Int64

func (c *Conn) onTimer() {
	timerFires.Add(1)
	c.lock()
	defer c.unlock()
	if c.closing {
		return
	}
	c.timerAt = -1
	c.fired = true
	c.s.HandleTimeout(c.clk.now())

}

// Start establishes a session over any carrier with an explicit role
// (API.md Section 3). It returns once the peer's transport parameters were
// processed, or at once when early data toward a zmux peer is enabled.
func Start(ctx context.Context, p PipeIO, role Role, cfg *Config) (*Conn, error) {
	if p.R == nil || p.W == nil {
		return nil, fmt.Errorf("%w: the bootstrap carrier needs both halves", ErrInvalidConfig)
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
	return startCore(ctx, p, cfg, cc, nil)
}

// startCore starts a session from a resolved core configuration; lst is
// the listener that owns it, if any.
func startCore(ctx context.Context, p PipeIO, cfg *Config, cc core.Config, lst *Listener) (*Conn, error) {
	if cfg.PipeAuth.TLS {
		exp, err := sessionExporter(p, lst == nil && cc.TransportRole == core.RoleInitiator)
		if err != nil {
			return nil, err
		}
		cc.Exporter = exp
	}
	c := newConn(cfg, cc)
	c.lst = lst
	switch {
	case lst != nil || cc.TransportRole == core.RoleResponder:
		c.dialed = dialPeer
	case cc.TransportRole == core.RoleInitiator:
		c.dialed = dialLocal
	}
	registerLaneKey(c)
	c.lock()
	c.addBootstrap(p)
	early := cc.EarlyZmuxPeer || cc.EarlyRemembered != nil && cc.EarlyReplayable
	c.unlock()
	c.watchEarly(p)
	if early {
		return c, nil
	}
	if err := c.waitEstablished(ctx); err != nil {
		c.CloseWithError(0, "")
		return nil, err
	}
	return c, nil
}

// sessionExporter is the exporter value of PipeAuth.TLS (MULTIPATH.md
// 10.2): from pipe 0's carrier, or for a split bootstrap whose halves are
// two TLS connections the uplink's exporter followed by the downlink's.
func sessionExporter(p PipeIO, initiator bool) ([]byte, error) {
	export := func(x any) ([]byte, bool, error) {
		s, ok := x.(tlsStater)
		if !ok {
			return nil, false, nil
		}
		st := s.ConnectionState()
		b, err := st.ExportKeyingMaterial(wire.ExporterSession, nil, 32)
		if err != nil {
			return nil, true, fmt.Errorf("zmux: TLS exporter: %w", err)
		}
		return b, true, nil
	}
	up, down := any(p.W), any(p.R) // the initiator writes on the uplink
	if !initiator {
		up, down = p.R, p.W
	}
	u, uok, err := export(up)
	if err != nil {
		return nil, err
	}
	d, dok, err := export(down)
	if err != nil {
		return nil, err
	}
	switch {
	case uok && dok && !bytes.Equal(u, d):
		return append(u, d...), nil // two connections: a split bootstrap
	case uok:
		return u, nil
	case dok:
		return d, nil
	}
	return nil, fmt.Errorf("%w: PipeAuth.TLS on a carrier without TLS", ErrInvalidConfig)
}

// Dial establishes a client session over c (the qmux-go form).
func Dial(ctx context.Context, c net.Conn, cfg *Config) (*Conn, error) {
	return Start(ctx, NetPipe(c), RoleClient, cfg)
}

// Server establishes a server session over c. It does not accept pipe
// joins; use a Listener for multipath.
func Server(ctx context.Context, c net.Conn, cfg *Config) (*Conn, error) {
	if cfg != nil && cfg.MaxPipes > 1 {
		return nil, fmt.Errorf("%w: Server cannot accept joins; use a Listener", ErrInvalidConfig)
	}
	return Start(ctx, NetPipe(c), RoleServer, cfg)
}

func newConn(cfg *Config, cc core.Config) *Conn {
	c := &Conn{cfg: cfg, clk: newClock(), pipes: make(map[uint64]*pipeDrv, 1), done: make(chan struct{}),
		timerAt: core.Never, oobHeaders: map[uint64]string{}, labels: map[uint64]string{}}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.s = core.New(cc, c.clk.now())
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
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	c.s.Close(uint64(code), []byte(msg))
	return nil
}

// Close closes the session with code 0; like CloseWithError it never
// blocks (use Wait for the end, or Shutdown for a graceful close).
func (c *Conn) Close() error { return c.CloseWithError(0, "") }

// Shutdown closes gracefully: GOAWAY when it is in use, wait for open
// streams, then CONNECTION_CLOSE (API.md 5.7). When ctx ends first it falls
// back to CloseWithError and returns ctx's error.
func (c *Conn) Shutdown(ctx context.Context, code ApplicationErrorCode, msg string) error {
	c.lock()
	c.s.SetNow(c.clk.now())
	c.s.Shutdown(uint64(code))
	c.unlock()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.CloseWithError(code, msg)
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

// ConnectionState reports the negotiation results (API.md 5.4).
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
	PeerGoAway         *GoAway
	MaxDatagramPayload int
}

// GoAway are the watermarks of a received GOAWAY.
type GoAway struct {
	MaxBidi, MaxUni uint64
	ErrorCode       ApplicationErrorCode
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

// ConnectionStats are transport counters (API.md 7.2).
type ConnectionStats struct {
	BytesSent, BytesReceived     uint64
	RecordsSent, RecordsReceived uint64
	StreamsOpenedLocal           uint64
	StreamsOpenedRemote          uint64
	DatagramsSent                uint64
	DatagramsReceived            uint64
	DatagramsDroppedSend         uint64
	DatagramsDroppedRecv         uint64
	DatagramsDroppedLimit        uint64
	BytesWritten, BytesRead      uint64 // application bytes
	BytesRedundant               uint64 // stream bytes sent again as redundant copies
	NoEffectBudget               int64
}

// ConnectionStats returns a snapshot of the counters.
func (c *Conn) ConnectionStats() ConnectionStats {
	c.lock()
	defer c.unlock()
	s := c.s.SessionStats()
	return ConnectionStats{BytesSent: s.BytesSent, BytesReceived: s.BytesReceived, RecordsSent: s.RecordsSent,
		RecordsReceived: s.RecordsReceived, StreamsOpenedLocal: s.StreamsOpenedLocal,
		StreamsOpenedRemote: s.StreamsOpenedRemote, DatagramsSent: s.DatagramsSent,
		DatagramsReceived: s.DatagramsReceived, DatagramsDroppedSend: s.DatagramsDroppedSend,
		DatagramsDroppedRecv: s.DatagramsDroppedRecv, DatagramsDroppedLimit: s.DatagramsDroppedLimit,
		BytesWritten: s.AppBytesWritten, BytesRead: s.AppBytesRead, BytesRedundant: s.BytesRedundant,
		NoEffectBudget: c.s.BudgetTokens()}
}

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
			return c.SendDatagram(p)
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
// outside the lock (API.md 5.6).
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
	h, err := c.open(nil, true)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}

// OpenStreamSync opens a bidirectional stream, waiting for credit in FIFO
// order.
func (c *Conn) OpenStreamSync(ctx context.Context) (*Stream, error) {
	h, err := c.open(ctx, true)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}

// OpenUniStream opens a unidirectional stream without waiting.
func (c *Conn) OpenUniStream() (*SendStream, error) {
	h, err := c.open(nil, false)
	if err != nil {
		return nil, err
	}
	return newSendStream(h), nil
}

// OpenUniStreamSync opens a unidirectional stream, waiting for credit.
func (c *Conn) OpenUniStreamSync(ctx context.Context) (*SendStream, error) {
	h, err := c.open(ctx, false)
	if err != nil {
		return nil, err
	}
	return newSendStream(h), nil
}

func (c *Conn) open(ctx context.Context, bidi bool) (*handle, error) {
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
		if errors.Is(err, core.ErrStreamLimit) && ctx != nil {
			if sig == nil {
				sig = core.NewSignal()
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
