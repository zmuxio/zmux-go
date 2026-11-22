package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// ListenerConfig configures a Listener under the Settings rules of the
// package documentation. These are safety bounds: the zero value of every
// field selects its stated default. A negative FirstRecordTimeout is no
// timeout and a negative MemoryBudget unlimited; any other negative
// value makes NewListener fail with ErrInvalidConfig.
type ListenerConfig struct {
	// Config is the configuration of the sessions it accepts. NewListener
	// takes a shallow copy and validates it for server sessions. Its
	// HandshakeTimeout also bounds the wait for a split-bootstrap partner
	// or for the handshake of a join that arrived in early data. Its
	// JoinTimeout bounds a stalled join.
	Config *Config
	// Logger receives the listener's warnings and the panics of its host
	// callbacks that it contains (OnReject, SessionConfig, Admission,
	// WrapPipe, Fallback) and
	// the errors SessionConfig returns;
	// nil logs nothing. The sessions log to their Config.Logger.
	Logger *slog.Logger
	// ListenerLimits are the bounds SetLimits changes while the listener
	// runs; these are their initial values.
	ListenerLimits
	// SourcePrefixV4 and SourcePrefixV6 aggregate a source to its prefix:
	// an IPv4 source (an IPv4-mapped IPv6 one included) counts by its first
	// SourcePrefixV4 bits (1 to 32; zero: 32, the address on its own), an
	// IPv6 source by its first SourcePrefixV6 bits (1 to 128; zero: 64),
	// ports ignored. This one aggregation is used by everything the
	// listener keeps per source: the MaxPendingPerSource slots, the count of
	// a source's stalled carriers and the stall duration
	// (Listener.StallDuration). A shorter prefix groups more clients into
	// one share, which tightens the bound against one network and lets its
	// clients crowd each other out; the defaults suit public listeners. A
	// negative value or one beyond the address length makes NewListener
	// fail with ErrInvalidConfig; there is no off setting.
	SourcePrefixV4, SourcePrefixV6 int
	// Backlog is the number of sessions waiting for Accept (zero: 128); a new
	// session beyond it is refused with CONNECTION_REFUSED.
	Backlog int
	// InstanceID is encoded at the start of every session locator, for
	// load-balancer stickiness; random bytes fill the rest of the 16-byte
	// locator. At most MaxInstanceIDLen bytes.
	InstanceID []byte
	// OnReject decides the response to an unauthenticated carrier, the one
	// response to every refusal before authentication: a carrier beyond the
	// pending limits ("pending_limit", Profile 0), an unknown Locator, a
	// Suite outside the implementation or the session's set ("unknown_suite"),
	// a failed Hello, a join that memory pressure refuses ("memory_pressure"),
	// or an Establish a transform refused (RejectError). Every such refusal
	// gets the same response content and way of ending (no byte, a close or a
	// stall of the same bound); processing time is not covered. Without it,
	// a transform's rejection before authentication (an unknown Handle, a
	// wrong pre-shared key, a Profile or mode not accepted) and a carrier
	// beyond the pending limits are stalled, and a join's is closed. A stalled
	// carrier gets no byte and holds a stall slot of its source; when the
	// source's slots are full it is closed at once, whatever the reason. A
	// stalled join ends at Config's join timeout counted from the
	// rejection, whether or not it named a session; a stalled Establish at
	// Config's handshake timeout, at most the listener's stall bound
	// (Admission.StallMax; without one, drawn as Admission draws it). No verdict sends anything. An acceptor
	// that closes at once keeps at least 8 random bytes after InstanceID in
	// every locator, which NewListener guarantees. It is called
	// synchronously on the carrier's goroutine, under no deadline of its
	// own: it must return promptly, well within the peer's handshake or
	// join timeout.
	OnReject func(RejectInfo) RejectAction
	// OnMemoryPressure is called with the bytes in use and the budget when
	// pressure starts (used >= budget) and when it ends, outside every
	// lock, one call at a time; it may close sessions.
	OnMemoryPressure func(used, budget int64)
	// Admission, when set, decides on every carrier of the listener,
	// whatever entry point took it (Serve, HandlePipe, the HTTP, exchange
	// and upgrade handlers, the QUIC and WebTransport adapters), before
	// anything is written on it. The order is the same on every entry:
	// the carrier takes a slot of its source (MaxPendingPerSource), its
	// outer handshake completes (TLS, an HTTP upgrade, QUIC; bounded by
	// Config.HandshakeTimeout), the admission reads its prefix and
	// decides (Admission.PeekTimeout), WrapPipe runs, and the carrier is
	// classified (FirstRecordTimeout) as a join or a new session. NewListener
	// fails if it is invalid (Admission.Validate).
	Admission *Admission
	// Fallback is the listener's byte-stream and message-carrier target.
	// VerdictTakeover selects it on every transport. VerdictFallback selects
	// it for TCP/TLS, QUIC first streams and already upgraded WebSocket or
	// WebTransport carriers. While the carrier is an HTTP request,
	// VerdictFallback serves Options.Next or responds with HTTP 404.
	// A carrier directed here stalls if Fallback is nil.
	//
	// The host receives the inspected prefix replayed and can route by
	// Takeover.Value. For message framing the replay is a whole non-empty
	// first message. The callback runs on the carrier's setup goroutine
	// after its slots are returned. Its context ends when it returns.
	// Returning closes the carrier unless the host called Takeover.Detach,
	// in which case the host owns it and closes it once done.
	Fallback func(ctx context.Context, t *Takeover)
	// WrapPipe, when set, is called once for every carrier the listener
	// takes, whatever fed it (Serve, HandlePipe, the HTTP,
	// exchange and adapter handlers), before the carrier is classified. It
	// returns the PipeIO the listener uses in place of p, such as a layer
	// over PipeConn(p), or p itself; a function is a WrapPipeFunc. A
	// PipeWrapper that also has a Validate() error method, such as a
	// secure-carrier server configuration, is validated by NewListener. Split carriers are wrapped once, after pairing,
	// as one p.
	//
	// While it runs the carrier holds its slots (MaxPending,
	// MaxPendingPerSource), and Config.HandshakeTimeout bounds it: ctx
	// ends at that bound, and p is closed then. FirstRecordTimeout runs from its return. The listener sets p's
	// Carrier, Label and FateGroup on the result, whose halves, Framing,
	// WriteOverhead and Variant are WrapPipe's (p.Variant, the admission's
	// Decision.Variant, stays where the result has none). An error, or the bound, closes p and counts a setup
	// failure (ListenerStats.SetupFailures).
	//
	// A layer must map every write on the PipeIO it returns to exactly
	// one write on p, never merging writes; a WriteMessages call
	// (MessageBatchWriter) counts as one write, which the layer may turn
	// into one write of p.
	WrapPipe PipeWrapper
	// SessionConfig, when set, is called once for every carrier that
	// starts a new session (not for joins), outside every lock, with the
	// carrier's info (Source, Peer, and TLS where this process terminated
	// it, as TerminatedTLS reports) and its admission's Decision.Value.
	// It returns the Config of that session, such as PipeAuth.Exporter
	// where TLS ends here and PipeAuth.DeferredPSK where it ends at an
	// intermediary, or per-session ShapingLimits; nil selects Config.
	// Joins to the session verify with its own Config. The listener-level
	// settings stay the listener's: ListenerConfig.MemoryBudget replaces
	// the returned MemoryBudget as it replaces Config's, the carrier-level
	// bounds (split pairing, the handshake of a join in early data,
	// WrapPipe) use Config.HandshakeTimeout, and the returned Clock must
	// be nil or Config's. A returned Config that violates that or is
	// invalid for a server session (ErrInvalidConfig), an error and a
	// panic close the carrier without a byte, counted as rejected; an
	// error and a panic are logged (Logger). It is called synchronously
	// before the session and its handshake timer exist, under no deadline
	// of its own (ctx is HandlePipe's): it must return well before the
	// peer's handshake deadline, which keeps running meanwhile.
	SessionConfig func(ctx context.Context, info *CarrierInfo, admitted any) (*Config, error)
}

// Listener defaults that zmux itself does not recommend (the protocol
// recommends the others).
const (
	defaultMaxSuspended = 1024
	defaultBacklog      = 128
	defaultMemoryBudget = 1 << 30
	// The default source prefixes: an IPv4 address on its own, an IPv6
	// address to its /64.
	defaultSourcePrefixV4 = 32
	defaultSourcePrefixV6 = 64
)

// PipeWrapper is a ListenerConfig.WrapPipe.
type PipeWrapper interface {
	WrapPipe(ctx context.Context, p PipeIO, hint PipeHint) (PipeIO, error)
}

// WrapPipeFunc is a function used as a PipeWrapper.
type WrapPipeFunc func(ctx context.Context, p PipeIO, hint PipeHint) (PipeIO, error)

// WrapPipe calls f.
func (f WrapPipeFunc) WrapPipe(ctx context.Context, p PipeIO, hint PipeHint) (PipeIO, error) {
	return f(ctx, p, hint)
}

// resolve returns c with every default filled in, or the configuration
// error.
func (c *ListenerConfig) resolve() (ListenerConfig, error) {
	var r ListenerConfig
	if c != nil {
		r = *c
	}
	if len(r.InstanceID) > MaxInstanceIDLen {
		return r, fmt.Errorf("%w: ListenerConfig.InstanceID longer than MaxInstanceIDLen", ErrInvalidConfig)
	}
	if r.Backlog < 0 {
		return r, fmt.Errorf("%w: negative ListenerConfig count", ErrInvalidConfig)
	}
	var err error
	if r.ListenerLimits, err = r.ListenerLimits.resolve(); err != nil {
		return r, err
	}
	r.Config = r.Config.Clone()
	if _, err := r.Config.coreConfig(core.RoleResponder, core.RoleResponder); err != nil {
		return r, fmt.Errorf("ListenerConfig.Config: %w", err)
	}
	if v, ok := r.WrapPipe.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return r, fmt.Errorf("ListenerConfig.WrapPipe: %w", err)
		}
	}
	if r.SourcePrefixV4 < 0 || r.SourcePrefixV4 > 32 || r.SourcePrefixV6 < 0 || r.SourcePrefixV6 > 128 {
		return r, fmt.Errorf("%w: ListenerConfig.SourcePrefixV4 beyond 0..32 or SourcePrefixV6 beyond 0..128", ErrInvalidConfig)
	}
	if r.SourcePrefixV4 == 0 {
		r.SourcePrefixV4 = defaultSourcePrefixV4
	}
	if r.SourcePrefixV6 == 0 {
		r.SourcePrefixV6 = defaultSourcePrefixV6
	}
	if err := r.Admission.Validate(); err != nil {
		return r, err
	}
	if r.Backlog == 0 {
		r.Backlog = defaultBacklog
	}
	return r, nil
}

// RejectInfo describes an unauthenticated carrier to ListenerConfig.OnReject.
type RejectInfo struct {
	// Err is the underlying error of a transform's rejection
	// (RejectError.Err), nil for a join.
	Err        error
	Src        netip.AddrPort
	Reason     string     // "unknown_locator", "unknown_suite", "auth", "pending_limit" or the like
	Profile    uint64     // 2 for a Join
	RecordMode RecordMode // the mode its Hello names
}

// RejectAction is the answer of ListenerConfig.OnReject.
type RejectAction uint8

const (
	// RejectClose closes the carrier.
	RejectClose RejectAction = iota
	// RejectStall holds the carrier, silent (ListenerConfig.OnReject says until when).
	RejectStall
)

// RejectError is what a WrapPipe returns to refuse an unauthenticated carrier: the
// carrier then goes to OnReject, and WrapPipe does not close it. Err is the
// underlying error, if any.
type RejectError struct {
	Err        error
	Reason     string
	Profile    uint64
	RecordMode RecordMode
}

func (e *RejectError) Error() string {
	if e.Err != nil {
		return "zmux: carrier rejected: " + e.Reason + ": " + e.Err.Error()
	}
	return "zmux: carrier rejected: " + e.Reason
}

// Unwrap returns Err.
func (e *RejectError) Unwrap() error { return e.Err }

// PipeHint is what a carrier's request said out of band (HTTP headers).
type PipeHint struct {
	// Admitted is Decision.Value of the admission that accepted the
	// carrier (nil: none). The listener sets it for WrapPipe; a value
	// passed to HandlePipe is replaced.
	Admitted any
	// Source is the carrier's client address (CarrierInfo.Source), for a
	// host that resolves it itself (zero: the address the carrier arrived
	// from). The adapters set it from TrustedProxies.
	Source netip.AddrPort
	// Pair is the Zmux-Pipe-Pair header: the pairing token of split
	// carriers; Uplink says which half this carrier is.
	// A half waits for its partner within the handshake timeout, holding
	// its slots (ListenerConfig.MaxPending, MaxPendingPerSource) until the
	// pair is complete.
	Pair   string
	Uplink bool
}

// Listener is the server side of zmux: it accepts sessions and the pipes
// that join them, so it is the way to serve
// clients that use multipath, resume or split carriers. Serve feeds it the
// connections of a net.Listener (TCP, or TLS with tls.NewListener);
// HandlePipe feeds it any other carrier, and zmuxhttp.NewHandler HTTP carriers.
// Accept returns the sessions it started:
//
//	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
//	if err != nil {
//		return err
//	}
//	defer l.Close()
//	go l.Serve(ctx, ln)
//	for {
//		conn, err := l.Accept(ctx)
//		if err != nil {
//			return err
//		}
//		go handle(conn)
//	}
//
// A listener that took a carrier runs a goroutine until it is closed:
// call Close (or Shutdown) when done with it, as for a net.Listener.
// Close stops accepting; Shutdown also closes the accepted sessions
// gracefully. Its locks are leaves: none takes a session lock while held.
type Listener struct {
	cfg ListenerConfig
	// lim holds the limits in effect (SetLimits); cfg keeps none.
	lim atomic.Pointer[ListenerLimits]

	// pmu guards the pending carriers' slots, apart from mu: every carrier
	// takes and gives back slots, so they do not wait for the session
	// registry. A leaf: nothing is locked under it.
	pmu      sync.Mutex
	pending  core.PendingCarriers // under pmu
	mu       sync.RWMutex         // read: a new session's handoff to Accept, the checks of closed and draining
	registry sessionRegistry
	suspMu   sync.Mutex // protects the global oldest-first suspension limit
	// The suspended sessions in suspension order (Conn.suspPrev and
	// suspNext): the oldest first.
	suspHead, suspTail *Conn
	suspN              int
	pairing            core.SplitPairing
	drainReason        string
	shutdownCtx        context.Context
	shutdownCancel     context.CancelFunc
	// served is set by the first carrier: SetLimits starts the memory
	// watcher only on a listener that serves.
	served       atomic.Bool
	closed       bool        // under mu
	draining     bool        // under mu: policy for new bootstraps
	shuttingDown bool        // under mu
	watching     atomic.Bool // the memory-pressure watcher was started (startWatch)

	accept     chan *Conn
	done       chan struct{} // closed by Close
	drain      chan struct{} // closed by Shutdown or Close
	stopCtx    context.Context
	stopCancel context.CancelFunc
	clk        timeshim.Clock
	budget     *core.Budget
	stallKey   [32]byte      // drawn by NewListener
	stallMax   time.Duration // the bound of an OnReject stall of a transform's rejection
	st         listenerCounters
}

// ListenerStats are the counts of a listener since it was made. The
// unit is the pipe input handed to the listener: a carrier, a lane, or the two carriers of a split pipe once paired, which are one
// input (a half never paired is one of its own). Every input whose
// outcome is known is counted once, in one of the first five fields, so
// that
//
//	Sessions + Joins + Rejected + Refused + TimedOut
//
// is the number of pipe inputs whose outcome is known; SetupFailures
// counts connections that never became a pipe input. Each count is read
// on its own: relations between them hold for their final values, not in
// every snapshot.
type ListenerStats struct {
	// Sessions counts sessions started: bootstraps offered to Accept, a
	// split bootstrap once.
	Sessions uint64
	// Joins counts joins accepted into existing sessions, and lanes
	// accepted into their pipes.
	Joins uint64
	// Rejected counts inputs closed without a reason on the wire:
	// unknown, unauthenticated or replayed joins, lanes that name no pipe,
	// or not zmux at all. Admission verdicts count in Admission.
	Rejected uint64
	// Refused counts inputs refused at a limit: of the listener
	// (MaxPendingPerSource, the memory budget), of the session (a join
	// refused with ZMUX_PIPE_RESOURCE_LIMIT, and an Unconfirmed carrier
	// closed to make room for a newer join, which counts here and in
	// Joins only if it was Confirmed), new sessions during
	// Shutdown, or after the listener was closed.
	Refused uint64
	// TimedOut counts inputs without a first record, or join, within
	// FirstRecordTimeout, and split halves whose other half did not arrive
	// in time.
	TimedOut uint64
	// SetupFailures counts connections never made a pipe input: a failed
	// TLS handshake or its timeout (without an Admission; with one it
	// counts in Admission.HandshakeFailed); the handshake that a join sent in
	// early data waits for, when it fails, times out or its wait is
	// cancelled (that join is then counted nowhere else); an HTTP request
	// answered 425 (Too Early); a failed HTTP or WebSocket upgrade; or the
	// setup limit. Only failures an adapter observes count: a QUIC
	// handshake that fails inside the stack before the connection is
	// accepted is counted nowhere. Adapters with
	// their own accept loops report theirs with CountSetupFailure.
	SetupFailures uint64
	// Admission are the outcomes of ListenerConfig.Admission.
	Admission AdmissionStats
}

// listenerCounters are the ListenerStats counts, each updated with one
// atomic add when a carrier's outcome is decided and read without a lock.
type listenerCounters struct {
	adm                                                         admissionCounters
	sessions, joins, setupFailures, rejected, refused, timedOut atomic.Uint64
}

// Stats returns the listener's counts. A nil l returns zero counts.
func (l *Listener) Stats() ListenerStats {
	if l == nil {
		return ListenerStats{}
	}
	c := &l.st
	return ListenerStats{Sessions: c.sessions.Load(), Joins: c.joins.Load(), SetupFailures: c.setupFailures.Load(),
		Rejected: c.rejected.Load(), Refused: c.refused.Load(), TimedOut: c.timedOut.Load(), Admission: c.adm.snapshot()}
}

// VariantStats returns the admission outcomes by Decision.Variant, a copy
// sorted by Name, with the bucket of the variants past
// MaxAdmissionVariants last; their sums are AdmissionStats.Accepted,
// Fallback, Takeover, Stalled + StallOverflow and Closed + TooLarge, respectively.
// Takeover is recorded as the carrier enters the host callback. A nil l returns nil.
func (l *Listener) VariantStats() []VariantStat {
	if l == nil {
		return nil
	}
	return l.st.adm.variantStats()
}

// CountSetupFailure counts a connection that never became a pipe input
// (ListenerStats.SetupFailures). Adapters that run their own accept
// loops call it for a failed TLS handshake or its timeout, a failed HTTP
// or WebSocket upgrade, or their setup limit; the listener's own serve
// loops count theirs. A nil l does nothing.
func (a *ListenerAdapter) CountSetupFailure() { (*Listener)(a).countSetupFailure() }

func (l *Listener) countSetupFailure() {
	if l != nil {
		l.st.setupFailures.Add(1)
	}
}

// rejectedBy counts a carrier that ended with err before it became a
// session, join or lane.
func (l *Listener) rejectedBy(err error) {
	if l == nil {
		return
	}
	switch {
	case errors.Is(err, errTimedOut):
		l.st.timedOut.Add(1)
	case errors.Is(err, errRefused), errors.Is(err, net.ErrClosed):
		l.st.refused.Add(1)
	default:
		l.st.rejected.Add(1)
	}
}

type pairWait struct {
	ctx      context.Context
	admitted any         // the half's admission value
	wait     *attachment // setup and cleanup of this half
	done     chan error  // the session's result for the waiting half
	p        PipeIO
}

// MaxInstanceIDLen bounds ListenerConfig.InstanceID so that at least 8
// random bytes remain in the 16-byte session locator.
const MaxInstanceIDLen = 8

// NewListener returns a listener; feed it carriers with Serve, HandlePipe
// or zmuxhttp.NewHandler, and Close it when done. cfg may be nil. It validates
// the whole configuration: an InstanceID longer than MaxInstanceIDLen, a
// negative count, a Config
// invalid for a server session, an invalid Admission or a WrapPipe whose
// Validate fails is ErrInvalidConfig.
func NewListener(cfg *ListenerConfig) (*Listener, error) {
	r, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	l := &Listener{cfg: r, registry: sessionRegistry{seed: maphash.MakeSeed()},
		done: make(chan struct{}), drain: make(chan struct{})}
	l.clk = timeshim.Or(l.cfg.Config.clock())
	l.stopCtx, l.stopCancel = context.WithCancel(context.Background())
	l.shutdownCtx, l.shutdownCancel = context.WithCancel(l.stopCtx)
	l.accept = make(chan *Conn, l.cfg.Backlog)
	_, _ = rand.Read(l.stallKey[:])
	lim := l.cfg.ListenerLimits
	l.cfg.ListenerLimits = ListenerLimits{} // lim is the only live copy
	l.lim.Store(&lim)
	l.pending.MaxTotal = lim.MaxPending
	l.pending.MaxPerSource = lim.MaxPendingPerSource
	l.pending.Timeout = core.Duration(lim.firstRecord())
	var limit uint64 // unlimited
	if lim.MemoryBudget > 0 {
		limit = uint64(lim.MemoryBudget)
	}
	l.budget = core.NewBudget(limit)
	if a := l.cfg.Admission; a != nil {
		l.stallMax = a.r.stallMax
	} else {
		l.stallMax = drawDuration(30*time.Second, 120*time.Second)
	}
	return l, nil
}

// startWatch starts the memory-pressure watcher with the first carrier
// the listener takes, so that a listener that never serves owns no
// goroutine; Close stops it (one started after Close returns at once).
func (l *Listener) startWatch() {
	l.served.Store(true)
	if l.budget.Limit() == 0 || l.watching.Load() || !l.watching.CompareAndSwap(false, true) {
		return
	}
	labels.Go(context.Background(), "listener-budget", func() { l.watchBudget() })
}

// MemoryUsed reports the bytes charged to the listener's memory budget,
// in the unit of ListenerConfig.MemoryBudget.
func (a *ListenerAdapter) MemoryUsed() int64 { return int64(a.budget.Used()) }

// MemoryPressure reports whether the memory charged to the listener's
// budget is above ListenerConfig.MemoryBudget: an adapter that buffers a
// carrier's data outside a session (the exchange carrier) then takes no
// more.
func (a *ListenerAdapter) MemoryPressure() bool { return a.budget.Pressure() }

// ChargeMemory charges delta bytes (negative: gives them back) to the
// listener's memory budget, for data an adapter buffers outside a
// session; it is safe for concurrent use and does not block.
func (a *ListenerAdapter) ChargeMemory(delta int64) { a.budget.Add(delta) }

// HasAdmission reports whether the listener runs an admission
// (ListenerConfig.Admission): an adapter that answered a carrier's request
// only once the listener took the carrier answers an admitted one at once.
func (a *ListenerAdapter) HasAdmission() bool { return a.cfg.Admission != nil }

// Clock is the listener's clock (Config.Clock, or the host's).
func (a *ListenerAdapter) Clock() timeshim.Clock { return a.clk }

// FirstRecordTimeout is the effective ListenerConfig.FirstRecordTimeout,
// the bound an adapter applies to the start of a carrier it reads itself.
func (a *ListenerAdapter) FirstRecordTimeout() time.Duration { return a.lim.Load().firstRecord() }

// HandshakeTimeout is the effective Config.HandshakeTimeout of the
// listener's sessions.
func (a *ListenerAdapter) HandshakeTimeout() time.Duration { return a.cfg.Config.handshakeTimeout() }

// watchBudget is the serial context of the memory budget: it calls the
// pressure hook and, under pressure, closes suspended sessions holding the
// most memory first.
func (l *Listener) watchBudget() {
	for {
		select {
		case <-l.budget.Changed:
		case <-l.done:
			return
		}
		over := l.budget.Pressure()
		if h := l.cfg.OnMemoryPressure; h != nil {
			h(int64(l.budget.Used()), int64(l.budget.Limit()))
		}
		for over && l.budget.Pressure() {
			candidate := l.largestSuspended()
			if candidate.c == nil {
				break
			}
			candidate.close()
		}
	}
}

// A suspension ticket identifies a particular suspension of a session.
// The session lock serializes its last validation with resume and close.
type suspension struct {
	c          *Conn
	generation uint64
}

func (t suspension) close() {
	c := t.c
	c.locked(func() {
		if c.suspGeneration == t.generation && c.s.State() == core.StateSuspended {
			c.s.CloseResumeTimeout()
		}
	})
}

func (l *Listener) largestSuspended() suspension {
	l.suspMu.Lock()
	cs := make([]suspension, 0, l.suspN)
	for c := l.suspHead; c != nil; c = c.suspNext {
		cs = append(cs, suspension{c, c.suspGeneration})
	}
	l.suspMu.Unlock()
	var best suspension
	var most uint64
	for _, ticket := range cs {
		c := ticket.c
		c.peek(func() {
			if c.suspGeneration == ticket.generation && c.s.State() == core.StateSuspended {
				if m := c.s.Charged(); best.c == nil || m > most {
					best, most = ticket, m
				}
			}
		})
	}
	return best
}

// Accept returns the next session a carrier started.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	select {
	case <-l.drain: // first: after Close or Shutdown, never a session
		return nil, net.ErrClosed
	default:
	}
	select {
	case c := <-l.accept:
		l.mu.Lock()
		stopped := l.closed || l.shuttingDown
		if !stopped {
			c.listenerAccepted = true
		}
		l.mu.Unlock()
		if stopped {
			c.Abort(0, "")
			return nil, net.ErrClosed
		}
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.drain:
		return nil, net.ErrClosed
	}
}

// Close stops the listener at once: Accept returns net.ErrClosed, Serve
// returns net.ErrClosed and closes its net.Listener, every new carrier
// is refused, joins and resumptions included, the admissions,
// handshakes and pairing waits in progress are cancelled, and the
// memory-pressure watcher stops. It does not block; sessions already
// accepted continue, without new pipes from the listener (Shutdown ends
// them), and those still waiting for Accept are aborted.
func (l *Listener) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		l.stopCancel()
		close(l.done)
		if !l.shuttingDown {
			close(l.drain)
		}
	}
	l.mu.Unlock()

	// Sessions in the backlog have no one to accept them now (no more
	// arrive after the stop cutoff under l.mu): they end here. Shutdown
	// ends them with every other session instead.
	for {
		select {
		case c := <-l.accept:
			c.Abort(0, "")
		default:
			return nil
		}
	}
}

// Drain refuses subsequent bootstrap handshakes with CONNECTION_REFUSED
// and msg. Existing sessions can still join, add lanes, and resume.
func (l *Listener) Drain(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.shuttingDown && !l.closed {
		l.draining, l.drainReason = true, msg
	}
}

// Undrain resumes bootstrap admission unless shutdown has started.
func (l *Listener) Undrain() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.shuttingDown && !l.closed {
		l.draining, l.drainReason = false, ""
	}
}

// Shutdown irreversibly drains bootstrap admission and gracefully closes
// published sessions. Their joins and resumptions can run until Close.
func (l *Listener) Shutdown(ctx context.Context, code ApplicationErrorCode, msg string) error {
	l.mu.Lock()
	if !l.shuttingDown && !l.closed {
		l.shuttingDown, l.draining, l.drainReason = true, true, msg
		l.shutdownCancel()
		close(l.drain)
	}
	l.mu.Unlock()
	// A session registered after this snapshot is refused at its handoff
	// (draining).
	cs := l.registry.snapshot(true)
	errs := make(chan error, len(cs))
	for _, c := range cs {
		go func() {
			if !c.listenerAccepted {
				errs <- c.Close()
				return
			}
			errs <- c.Shutdown(ctx, code, msg)
		}()
	}
	var err error
	for range cs {
		if e := <-errs; e != nil && err == nil {
			err = e
		}
	}
	_ = l.Close()
	return err
}

// Serve accepts connections from ln and hands each one to HandlePipe, in a
// goroutine of its own, as a carrier that starts or joins a session; the
// sessions it starts are returned by Accept. With an Admission, a TLS
// connection completes its handshake first, and VerdictFallback hands the
// connection to ListenerConfig.Fallback. During Shutdown new carriers are refused. For TLS, pass a listener made
// by tls.NewListener. Serve closes ln when it returns: when ln.Accept
// fails, when ctx ends (ctx's error), or when the listener is closed
// (net.ErrClosed). Each carrier goroutine returns after HandlePipe decides its
// fate. Cancelling ctx stops pending setup. Established pipes remain with their sessions.
func (l *Listener) Serve(ctx context.Context, ln net.Listener) error {
	stop := make(chan struct{})
	defer close(stop)
	labels.Go(context.Background(), "listener-serve", func() {
		select {
		case <-ctx.Done():
		case <-l.done:
		case <-stop:
		}
		_ = ln.Close()
	})
	for {
		nc, err := ln.Accept()
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case l.isClosed():
				return net.ErrClosed
			}
			return err
		}
		labels.Go(ctx, "listener-pipe", func() { _ = l.HandlePipe(ctx, NetPipe(nc), PipeHint{}) })
	}
}

func (l *Listener) isClosed() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

var errRejected = errors.New("zmux: carrier rejected")

// errRefused and errTimedOut are the rejections that ListenerStats counts
// as refused and timed out.
var (
	errRefused  = fmt.Errorf("zmux: carrier refused: %w", errRejected)
	errTimedOut = fmt.Errorf("zmux: first record timeout: %w", errRejected)
	// errUnknownSuite is a join whose Suite is outside its session's set.
	errUnknownSuite = fmt.Errorf("zmux: join suite not usable in the session: %w", errRejected)
)

// HandlePipe takes ownership of p on entry. The caller must never close p,
// including when HandlePipe returns an error. It returns when the carrier
// has started or joined a session, has been refused, or Fallback has
// returned on the caller's goroutine. ctx bounds admission, classification
// and setup through handoff. Once attached, the session drives the close,
// including CONNECTION_CLOSE after a handshake failure or policy refusal.
// A nil result means bootstrap entered the backlog, a join was confirmed, or
// a lane was attached, or Fallback returned normally. Once committed, the
// setup result survives cancellation even before HandlePipe returns. I/O can outlive an
// error return, and later carrier failures are reported by the session.
//
// Classification uses the first record or the hint. The carrier
// takes a slot of its source first (hint.Source, or the remote address of
// a half: Arrive) and passes the listener's admission (Arrival.HandlePipe).
// A carrier that is not yet classified, or that joins a session and is
// not yet verified, also holds a pending slot (ListenerConfig.MaxPending).
func (l *Listener) HandlePipe(ctx context.Context, p PipeIO, hint PipeHint) error {
	p = owned(p)
	var peer netip.AddrPort
	if !safeLog(l.cfg.Logger, "carrier peer", func() { peer = peerOf(p) }) {
		closePipe(p)
		l.countSetupFailure()
		return errRejected
	}
	a, err := l.arrive(hint.Source, peer)
	if errors.Is(err, errRefused) {
		return l.refusePending(p, unmapped(hint.Source), peer)
	}
	if err != nil {
		closePipe(p)
		return err
	}
	return a.HandlePipe(ctx, p, hint)
}

// refusePending refuses a carrier beyond the pending limits, the first check
// of an accepted carrier: it goes to the reject policy, reason "pending_limit",
// like every refusal before authentication, and is closed.
func (l *Listener) refusePending(p PipeIO, src, peer netip.AddrPort) error {
	if !src.IsValid() {
		src = unmapped(peer)
	}
	l.onReject(p, nil, RejectInfo{Reason: "pending_limit", Src: src})
	closePipe(p)
	return errRefused
}

// take hands an admitted carrier, which holds slot, to the listener, or
// refuses it at once when the listener is closed,
// before it waits for anything. run then does the rest of HandlePipe; the
// slots are given back once the carrier enters a session or is closed.
func (l *Listener) take(hint PipeHint, wait *attachment) (func(context.Context) error, error) {
	p := wait.pipe
	l.mu.RLock()
	ok := l.stopCtx.Err() == nil
	l.mu.RUnlock()
	if !ok {
		closePipe(p)
		l.st.refused.Add(1)
		return nil, errRefused
	}
	return func(ctx context.Context) error {
		if hint.Pair != "" {
			return l.handlePair(ctx, p, hint, wait)
		}
		return l.handleAdmitted(ctx, hint, wait)
	}, nil
}

// wrapPipe applies ListenerConfig.WrapPipe to p, within the handshake
// timeout; p is closed when it fails.
func (l *Listener) wrapPipe(ctx context.Context, p PipeIO, hint PipeHint, wait *attachment) (PipeIO, error) {
	wrap := l.cfg.WrapPipe
	if wrap == nil {
		return p, nil
	}
	ctx, cancel := timeshim.WithTimeout(ctx, l.clk, l.cfg.Config.handshakeTimeout())
	defer cancel()
	// A layer that does not watch ctx is interrupted by closing p, through
	// the closer every failure below shares.
	stop := interruptOnCancel(ctx, func() { closePipe(p) })
	// The layer is a host callback the peer drives (seal's Gate, AppData,
	// Signer included): a panic in it fails this carrier, not the process.
	var q PipeIO
	err := hostCall(l.cfg.Logger, "ListenerConfig.WrapPipe", func() error { var err error; q, err = wrap.WrapPipe(ctx, p, hint); return err })
	fired := !stop()
	if rej := (*RejectError)(nil); !fired && errors.As(err, &rej) {
		markTaken(p)
		l.st.rejected.Add(1)
		l.onReject(p, wait, RejectInfo{Profile: rej.Profile, RecordMode: rej.RecordMode, Reason: rej.Reason, Src: hint.Source, Err: rej.Err})
		return PipeIO{}, errRejected
	}
	if err != nil {
		// Halves a failing layer returned anyway are closed too.
		return l.wrapFailed(p, q, err)
	}
	if fired {
		return l.wrapFailed(p, q, context.Cause(ctx))
	}
	if q.R == nil && q.W == nil {
		return l.wrapFailed(p, q, errors.New("no pipe halves"))
	}
	if err := q.validate(); err != nil {
		return l.wrapFailed(p, q, err)
	}
	if !carrierio.SameHalf(q.R, p.R) || !carrierio.SameHalf(q.W, p.W) {
		// New halves are the layer's, over p: closing them ends p (through
		// p's closer where the layer uses PipeConn), so they get a closer
		// of their own.
		if !safeLog(l.cfg.Logger, "CarrierTaken", func() { markTaken(p) }) {
			return l.wrapFailed(p, q, errRejected)
		}
		q.own = nil
	} else {
		q.own = p.own
	}
	q = owned(q)
	q.Carrier, q.Label, q.FateGroup = p.Carrier, p.Label, p.FateGroup
	if q.Variant == "" {
		q.Variant = p.Variant
	}
	return q, nil
}

// wrapFailed closes what wrapPipe holds after err: q's new halves, and p
// (once: the bound may have closed it), and counts a setup failure.
func (l *Listener) wrapFailed(p, q PipeIO, err error) (PipeIO, error) {
	safeLog(l.cfg.Logger, "CarrierTaken", func() { markTaken(p) })
	if r, w := newHalf(q.R, p.R, p.W), newHalf(q.W, p.R, p.W); r != nil || w != nil {
		carrierio.NewCloser(r, w).Close()
	}
	closePipe(p)
	l.countSetupFailure()
	return PipeIO{}, fmt.Errorf("zmux: WrapPipe: %w", err)
}

// handleAdmitted is HandlePipe for an admitted carrier.
func (l *Listener) handleAdmitted(ctx context.Context, hint PipeHint, wait *attachment) error {
	wait.ctx = ctx
	setup, cancel := l.stopContext(ctx)
	defer cancel()
	err := hostCall(l.cfg.Logger, "carrier setup", func() error {
		return l.classify(setup, hint, wait)
	})
	if err != nil && wait.rollback() {
		closePipe(wait.pipe)
	}
	wait.ctx = nil
	wait.parentsEnded = nil
	wait.pipe = PipeIO{}
	if err == nil {
		wait.leave()
	}
	return err
}

func (l *Listener) classify(ctx context.Context, hint PipeHint, wait *attachment) error {
	p, err := l.wrapPipe(ctx, wait.pipe, hint, wait)
	if err != nil {
		return err
	}
	wait.pipe = p
	first, cls, err := l.readFirst(ctx, p)
	if err != nil {
		closePipe(p)
		if tlsSetupFailed(p) {
			l.countSetupFailure()
		} else {
			l.rejectedBy(err)
		}
		return err
	}
	stop := wait.watch(ctx, p)
	defer stop()
	if (cls == core.ClassSession || cls == core.ClassJoin) && (p.R == nil || p.W == nil) {
		l.rejectedBy(errRejected)
		return errRejected
	}
	switch cls {
	case core.ClassSession:
		admitted := hint.Admitted
		if v := carrierPeer(p); v != nil {
			admitted = v
		}
		return l.startSession(ctx, withPrefix(p, first), admitted, hint.Source, wait)
	case core.ClassJoin:
		return l.join(ctx, p, first, hint, wait)
	case core.ClassLane:
		return handleLane(ctx, l, withPrefix(p, first), first, wait)
	case core.ClassMore, core.ClassReject:
	}
	closePipe(p)
	l.st.rejected.Add(1)
	return errRejected
}

// readFirst reads until the first record is complete (or rejected),
// within the first-record timeout; ctx ending or the listener closing
// abandon the carrier as well. It reads on the caller's goroutine: the
// timeout and the two ends close the carrier from callbacks, with no
// goroutine or shared channel per carrier.
func (l *Listener) readFirst(ctx context.Context, p PipeIO) ([]byte, core.Class, error) {
	return readFirstWithin(ctx, l.stopCtx, l.clk, l.lim.Load().firstRecord(), p)
}

// readFirstWithin reads the first record of p within d on clk; ctx or stop
// ending abandons the carrier as well (stop ends as net.ErrClosed).
func readFirstWithin(ctx, stop context.Context, clk timeshim.Clock, d time.Duration, p PipeIO) ([]byte, core.Class, error) {
	var once sync.Once
	var why error
	abandon := func(err func() error) func() {
		return func() {
			once.Do(func() {
				why = err()
				closePipe(p) // interrupts the read
			})
		}
	}
	t := clk.AfterFunc(d, abandon(func() error { return errTimedOut }))
	stopCtx := context.AfterFunc(ctx, abandon(ctx.Err))
	stopListener := context.AfterFunc(stop, abandon(func() error { return net.ErrClosed }))
	var b []byte
	var cls core.Class
	err := hostCall(nil, "first record", func() error { var e error; b, cls, e = readFirstRecord(p); return e })
	t.Stop()
	stopCtx()
	stopListener()
	abandoned := true
	once.Do(func() { abandoned = false }) // waits for an abandon under way
	if abandoned {
		return nil, core.ClassReject, why
	}
	return b, cls, err
}

// firstReadChunk is the size of readFirstRecord's reads.
const firstReadChunk = 512

// MaxUntakenRead bounds what the listener reads of a byte-stream carrier
// before it is classified: a whole record of MaxRecordSize with its
// length, and one read beyond. A CarrierTaker keeps at most this much.
const MaxUntakenRead = MaxRecordSize + 8 + firstReadChunk

// CarrierTaker is a carrier's receiving half that records what the
// listener reads until the listener takes the carrier, so that a carrier
// it does not take can be handed on with those bytes replayed (at most
// MaxUntakenRead of them). The listener calls CarrierTaken once a
// session, a join or a lane attaches the carrier.
type CarrierTaker interface{ CarrierTaken() }

// markTaken tells p's receiving half, through its views, that the
// listener took the carrier: a session, a join or a lane attaches it.
func markTaken(p PipeIO) {
	if t, ok := p.R.(CarrierTaker); ok {
		t.CarrierTaken()
	}
}

// readFirstRecord reads a carrier's first record.
func readFirstRecord(p PipeIO) ([]byte, core.Class, error) {
	if p.R == nil {
		return nil, core.ClassReject, errRejected
	}
	if p.Framing == FramingRecords {
		mr, ok := p.R.(MessageReader)
		if !ok {
			return nil, core.ClassReject, errors.New("zmux: Records framing needs a MessageReader")
		}
		m, err := mr.ReadMessage(MaxRecordSize)
		if err != nil {
			return nil, core.ClassReject, err
		}
		if join.IsHello(m) {
			return m, core.ClassJoin, nil
		}
		framed := varint.AppendVarint(nil, uint64(len(m)))
		cls, _ := core.Classify(append(framed, m...), MaxRecordSize)
		return m, cls, nil
	}
	var b []byte
	var tmp [firstReadChunk]byte
	for {
		n, err := p.R.Read(tmp[:])
		b = append(b, tmp[:n]...)
		cls, rl := core.Classify(b, MaxRecordSize)
		switch {
		case cls == core.ClassReject:
			return nil, cls, errRejected
		case cls == core.ClassJoin:
			if p.W == nil {
				return b, cls, nil
			} // No further bytes can supply a reply direction.
			// A joiner sends nothing after a Hello until it has the Reply.
			if _, _, e := join.ParseHelloExact(b, join.Stream); !errors.Is(e, join.ErrIncomplete) {
				return b, cls, nil // the parser's verdict is the listener's to act on
			}
		case cls != core.ClassMore && rl > 0:
			return b, cls, nil
		}
		if err != nil {
			return nil, core.ClassReject, err
		}
	}
}

// startSession starts a server session on a carrier whose first record is
// QX_TRANSPORT_PARAMETERS; admitted is the carrier's admission value,
// src its PipeHint.Source.
func (l *Listener) startSession(ctx context.Context, p PipeIO, admitted any, src netip.AddrPort, wait *attachment) error {
	// Snapshot the policy once, when this carrier is classified as a
	// bootstrap (after pairing for split). Undrain cannot reverse its decision.
	l.mu.RLock()
	refuse, reason := l.draining, l.drainReason
	if wait != nil && !refuse {
		wait.stopHandshake = l.drain
	}
	l.mu.RUnlock()
	cfg, err := l.sessionConfig(ctx, p, admitted, src)
	if err != nil {
		closePipe(p)
		l.st.rejected.Add(1)
		return err
	}
	cc, err := cfg.coreConfig(core.RoleResponder, core.RoleResponder)
	if err != nil {
		closePipe(p)
		l.st.rejected.Add(1)
		return err
	}
	loc := make([]byte, 16)
	n := copy(loc, l.cfg.InstanceID)
	_, _ = rand.Read(loc[n:])
	if cc.Local.Has&wire.HasMultipath != 0 {
		cc.Local.Locator = loc
		cc.LocatorPrefix = loc[:n]
	}
	cc.Budget = l.budget
	if err := wait.acceptHead(ctx); err != nil {
		return err
	}
	markTaken(p)
	// Close cancels the bootstrap handshake; the session no longer
	// depends on the context once established.
	//goland:noinspection GoResourceLeak // a Conn returned with an error owns its own close (setupCore)
	c, err := startCore(ctx, p, cfg, cc, l, admitted, wait)

	if err != nil {
		l.st.rejected.Add(1)
		return err
	}
	refuse, err = l.publishBootstrap(ctx, c, wait, refuse, reason)
	if err != nil {
		c.Abort(0, "")
		l.st.rejected.Add(1)
		return err
	}
	if refuse {
		l.st.refused.Add(1)
		return errRefused
	}
	return nil
}

// The provisional driver runs the bootstrap handshake under setup
// ownership. Publication or refusal decides the attachment only after
// establishment. The listener lock serializes publishers with its cutoff.
func (l *Listener) publishBootstrap(ctx context.Context, c *Conn, wait *attachment, refuse bool, reason string) (bool, error) {
	c.lock()
	defer c.unlock()
	if c.closing || c.s.State() >= core.StateClosing {
		return false, c.errLocked()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if l.stopCtx.Err() != nil {
		return false, net.ErrClosed
	}
	state := setupAttached
	switch {
	case refuse:
	case l.shuttingDown:
		refuse, reason = true, l.drainReason
	case l.budget.Pressure():
		refuse, reason = true, "memory budget"
	case len(l.accept) == cap(l.accept):
		refuse, reason = true, "backlog"
	}
	if refuse {
		state = setupRejecting
	}
	if wait != nil {
		if err := wait.commitLocked(ctx, setupBootstrap, state); err != nil {
			return false, err
		}
	}
	if refuse {
		c.s.SetNow(c.clk.now())
		c.s.Refuse(reason)
	} else {
		if !l.registry.publish(c) {
			return false, net.ErrClosed
		}
		l.st.sessions.Add(1)
		l.accept <- c
	}
	c.provisional = false
	if c.publication != nil {
		close(c.publication)
		c.publication = nil
	}
	return refuse, nil
}

// A locator can be visible before the bootstrap has reached the backlog.
func (c *Conn) waitPublication(ctx context.Context) error {
	if c.lst == nil {
		return nil
	}
	var published chan struct{}
	c.peek(func() {
		if c.provisional {
			if c.publication == nil {
				c.publication = make(chan struct{})
			}
			published = c.publication
		}
	})
	if published == nil {
		return nil
	}
	select {
	case <-published:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.lst.done:
		return net.ErrClosed
	case <-c.done:
		return c.closedErr()
	}
}

// sessionConfig returns the listener's immutable Config or a copy of the
// callback's Config checked against it.
func (l *Listener) sessionConfig(ctx context.Context, p PipeIO, admitted any, src netip.AddrPort) (*Config, error) {
	f := l.cfg.SessionConfig
	if f == nil {
		return l.cfg.Config, nil
	}
	info := &CarrierInfo{Source: src, Peer: peerOf(p)}
	if !info.Source.IsValid() {
		info.Source = info.Peer
	}
	if cs, ok := TerminatedTLS(p); ok {
		info.TLS = &cs
	}
	var cfg *Config
	var err error
	if !safeLog(l.cfg.Logger, "ListenerConfig.SessionConfig", func() { cfg, err = f(ctx, info, admitted) }) {
		err = errors.New("panicked")
	} else if err != nil && l.cfg.Logger != nil {
		l.cfg.Logger.Warn("zmux: ListenerConfig.SessionConfig refused a session", "source", info.Source.String(), "error", err.Error())
	}
	if err != nil {
		return nil, fmt.Errorf("zmux: SessionConfig: %w", err)
	}
	if cfg == nil {
		return l.cfg.Config, nil
	}
	if c := cfg.clock(); c != nil && !sameClock(c, l.cfg.Config.clock()) {
		return nil, fmt.Errorf("%w: SessionConfig returned a Clock other than the listener's", ErrInvalidConfig)
	}
	return cfg.Clone(), nil
}

// sameClock reports whether a and b are the same clock (a clock of a
// type that is not comparable is not).
func sameClock(a, b timeshim.Clock) (same bool) {
	defer func() { _ = recover() }()
	return a == b
}

// KnownJoin reports, for use in Admission.Decide, whether prefix begins a
// Join Hello (the first byte of a join) whose Locator names a session of l
// at this moment. A prefix that is not a Join, or is malformed (a suite this
// process does not have, reserved mode bits, a zero Locator length), gives
// false, 0.
// When prefix ends before the Locator and the nonce that follows it (the
// head of the Hello), need is the total prefix length required, to
// return as Decision.Need with VerdictMore; Admission.MaxPeek must allow
// it: at most 259 bytes plus the suite's nonce length (32 for the suites
// defined), 291 in all. On a message-framed carrier the prefix is the whole
// message and need > 0 means it is too short.
//
// The answer is advisory: the join still authenticates, and a session that
// ended meanwhile gets OnReject as for any unknown Locator. l.mu is held
// for the lookup only.
func (a *ListenerAdapter) KnownJoin(prefix []byte) (known bool, need int) {
	l := (*Listener)(a)
	if !join.IsHello(prefix) {
		return false, 0
	}
	h, n, err := join.ParseHelloHead(prefix, join.Stream)
	if errors.Is(err, join.ErrIncomplete) {
		return false, n
	}
	if err != nil {
		return false, 0
	}
	ok := l.registry.lookup(string(h.Locator)) != nil
	return ok, 0
}

// join runs the acceptor's side of a Hello. The carrier keeps its
// pending slot until it is Confirmed.
func (l *Listener) join(ctx context.Context, p PipeIO, b []byte, hint PipeHint, wait *attachment) error {
	jf := join.Stream
	if p.Framing == FramingRecords {
		jf = join.Message
	}
	reject := func(mode RecordMode, reason string) error {
		l.st.rejected.Add(1)
		l.onReject(p, wait, RejectInfo{Profile: join.ProfileJoin, RecordMode: mode, Reason: reason, Src: hint.Source})
		return errRejected
	}
	h, _, err := join.ParseHelloExact(b, jf)
	if errors.Is(err, join.ErrUnknownSuite) {
		return reject(0, "unknown_suite")
	}
	if err != nil {
		return reject(0, "malformed")
	}
	c := l.registry.lookup(string(h.Locator))
	if l.stopCtx.Err() != nil {
		closePipe(p)
		l.st.refused.Add(1)
		return errRefused
	}
	if c == nil {
		return reject(RecordMode(h.Mode), "unknown_locator")
	}
	if l.budget.Pressure() {
		// Under pressure only a join that lets a suspended session resume.
		susp := c.inSusp.Load()
		if !susp {
			return reject(RecordMode(h.Mode), "memory_pressure")
		}
	}
	// A Hello that arrived as carrier early data waits for the carrier's handshake, as
	// its Binding does.
	if es, ok := p.W.(EarlyServerCarrier); ok && es.UsedEarlyData() {
		timer := timeshim.NewTimer(l.clk, l.cfg.Config.handshakeTimeout())
		defer timer.Stop()
		var failed <-chan struct{}
		if f, ok := es.(interface{ HandshakeFailed() <-chan struct{} }); ok {
			failed = f.HandshakeFailed()
		}
		select {
		case <-failed:
			closePipe(p)
			l.countSetupFailure()
			return errRejected
		case <-es.HandshakeComplete():
		case <-timer.C:
			closePipe(p)
			l.countSetupFailure()
			return errRejected
		case <-ctx.Done():
		case <-l.done:
		}
		if ctx.Err() != nil || l.isClosed() {
			closePipe(p)
			l.countSetupFailure()
			return errRejected
		}
	}
	var binding []byte
	if h.Mode == join.Bound {
		// A carrier without the contract never passes, whatever the joiner mixed.
		if binding, err = joinBinding(p); err != nil {
			c.helloWithoutBinding()
			return reject(RecordMode(h.Mode), "auth")
		}
	}
	// Closing the listener ends the handshake, not an attached pipe.
	err = c.acceptHello(ctx, p, h, binding, hint.Admitted, l.lim.Load().MaxPendingPerSession, wait)
	if err != nil {
		if errors.Is(err, errUnknownSuite) {
			return reject(RecordMode(h.Mode), "unknown_suite")
		}
		if errors.Is(err, errRejected) && !errors.Is(err, errRefused) {
			return reject(RecordMode(h.Mode), "auth")
		}
		if errors.Is(err, errRefusalSent) {
			l.lingerClose(ctx, p)
		} else {
			closePipe(p)
		}
		l.rejectedBy(err)
		return err
	}
	l.st.joins.Add(1)
	return nil
}

// joinLinger bounds how long a carrier that carried a refusal is read
// after its write side ended: SPEC Section 6.5's linger bound.
const joinLinger = 2 * time.Second

// lingerClose closes a carrier whose last write the peer must read: it
// ends the write side gracefully (TCP FIN, TLS close_notify) and reads and
// discards what the peer still sends until the peer ends it, joinLinger
// passes or the listener stops, so that unread data does not turn the
// close into a reset that destroys the write at the peer (SPEC Section
// 6.5). A carrier without a half-close is closed after the reads.
func (l *Listener) lingerClose(ctx context.Context, p PipeIO) {
	ctx, cancel := l.stopContext(ctx)
	defer cancel()
	ctx, cancelT := timeshim.WithTimeout(ctx, l.clk, joinLinger)
	defer cancelT()
	// A blocked half-close or read ends with the carrier.
	stop := interruptOnCancel(ctx, func() { closePipe(p) })
	defer stop()
	if cw, ok := p.W.(CloseWriter); ok {
		_ = cw.CloseWrite()
	}
	if p.R != nil {
		discard(p.R)
	}
	closePipe(p)
}

// onReject decides the response to a rejected carrier, OnReject or the
// default (RejectStall for a transform's rejection before
// authentication, RejectClose for a join), and stalls it if so. The
// caller closes p.
func (l *Listener) onReject(p PipeIO, wait *attachment, info RejectInfo) {
	act := RejectClose
	if info.Profile != join.ProfileJoin {
		act = RejectStall
	}
	if f := l.cfg.OnReject; f != nil && !safeLog(l.cfg.Logger, "ListenerConfig.OnReject", func() { act = f(info) }) {
		act = RejectClose
	}
	if act == RejectStall && wait != nil {
		l.stall(wait.ctx, p, wait, info)
	}
}

// stall holds a rejected carrier, silent, in a stall slot of its source:
// a source whose share of stalled carriers is full has it closed at once,
// whatever the reason. A rejected join ends at the join timeout of the
// listener's Config, whether or not it named a session, counted from the
// rejection; a transform's rejection at the handshake timeout, at most the
// listener's stall bound (Admission.StallMax; without one drawn when the
// listener was made).
func (l *Listener) stall(ctx context.Context, p PipeIO, wait *attachment, info RejectInfo) {
	if wait == nil {
		return
	}
	markTaken(p)
	d := l.cfg.Config.joinTimeout()
	if info.Profile != join.ProfileJoin {
		d = min(l.cfg.Config.handshakeTimeout(), l.stallMax)
	}
	wait.stall(ctx, d, nil, true, func(ctx context.Context) { <-ctx.Done(); closePipe(p) })
}

// sessionSuspended records a suspension and returns the sessions to close
// beyond MaxSuspended (closed by the caller outside every lock).
func (l *Listener) sessionSuspended(c *Conn) []suspension {
	l.suspMu.Lock()
	defer l.suspMu.Unlock()
	l.dropSuspended(c)
	c.suspGeneration++
	c.inSusp.Store(true)
	c.suspPrev, c.suspNext = l.suspTail, nil
	if l.suspTail == nil {
		l.suspHead = c
	} else {
		l.suspTail.suspNext = c
	}
	l.suspTail = c
	l.suspN++
	return l.trimSuspended()
}

// trimSuspended drops the longest-suspended sessions beyond MaxSuspended
// and returns them, for the caller to close outside every lock. Under suspMu.
func (l *Listener) trimSuspended() []suspension {
	var over []suspension
	for l.suspN > l.lim.Load().MaxSuspended {
		old := l.suspHead
		l.dropSuspended(old)
		over = append(over, suspension{old, old.suspGeneration})
	}
	return over
}

func (l *Listener) sessionResumed(c *Conn) {
	l.suspMu.Lock()
	defer l.suspMu.Unlock()
	l.dropSuspended(c)
}

func (l *Listener) dropSuspended(c *Conn) {
	if !c.inSusp.Load() {
		return
	}
	if c.suspPrev == nil {
		l.suspHead = c.suspNext
	} else {
		c.suspPrev.suspNext = c.suspNext
	}
	if c.suspNext == nil {
		l.suspTail = c.suspPrev
	} else {
		c.suspNext.suspPrev = c.suspPrev
	}
	c.inSusp.Store(false)
	c.suspPrev, c.suspNext = nil, nil
	l.suspN--
}

// ---- split bootstrap ----

// The half holds its slots until the pair is complete, and the pair goes
// on with those of the half that completed it; release gives them back.
func (l *Listener) handlePair(ctx context.Context, p PipeIO, hint PipeHint, wait *attachment) error {
	reject := func() error {
		closePipe(p)
		l.st.rejected.Add(1)
		return errRejected
	}
	var t [16]byte
	readOnly := p.R != nil && p.W == nil
	writeOnly := p.W != nil && p.R == nil
	if len(hint.Pair) != base64.RawURLEncoding.EncodedLen(len(t)) || hint.Uplink && !readOnly || !hint.Uplink && !writeOnly {
		return reject()
	}
	n, err := base64.RawURLEncoding.Strict().Decode(t[:], []byte(hint.Pair))
	if err != nil || n != len(t) {
		return reject()
	}
	hto := l.cfg.Config.handshakeTimeout()
	me := &pairWait{ctx: ctx, p: p, admitted: hint.Admitted, wait: wait, done: make(chan error, 1)}
	l.mu.Lock()
	res, partner := l.pairing.Offer(t, hint.Uplink, me)
	l.mu.Unlock()
	switch res {
	case core.PairReject:
		return reject()
	case core.PairWait:
		timer := timeshim.NewTimer(l.clk, hto)
		defer timer.Stop()
		select {
		case err := <-me.done:
			return err
		case <-timer.C:
		case <-ctx.Done():
		case <-l.stopCtx.Done():
		}
		l.mu.Lock()
		mine := l.pairing.Withdraw(t, me)
		l.mu.Unlock()
		if mine {
			// A half never paired is an input of its own.
			closePipe(p)
			if ctx.Err() != nil {
				l.st.rejected.Add(1) // abandoned by its caller
				return ctx.Err()
			}
			if l.stopCtx.Err() != nil {
				l.st.refused.Add(1)
				return net.ErrClosed
			}
			l.st.timedOut.Add(1) // no partner within the handshake timeout
			return errTimedOut
		}
		// The partner took both halves and will report their shared outcome.
		return <-me.done
	case core.PairDone:
	}
	other := partner.(*pairWait)
	other.wait.pipe = PipeIO{}
	other.wait.leave() // The pair keeps the completing half's slots.
	up, down, admitted := p, other.p, hint.Admitted
	if !hint.Uplink {
		up, down, admitted = other.p, p, other.admitted
	}
	// up and down are named for the peer: this end writes on down. What describes the
	// write is the writing half's.
	both := owned(PipeIO{R: up.R, W: down.W, Framing: up.Framing, Label: up.Label, Carrier: up.Carrier, FateGroup: up.FateGroup,
		WriteOverhead: down.WriteOverhead, MaxRecord: down.MaxRecord, Variant: up.Variant})
	// Once paired, neither individual request can cancel the combined setup.
	first, second := other.ctx, ctx
	ctx, cancel := l.stopContext(context.WithoutCancel(second))
	abandon := func() {
		if first.Err() != nil && second.Err() != nil {
			cancel()
		}
	}
	stopFirst := context.AfterFunc(first, abandon)
	stopSecond := context.AfterFunc(second, abandon)
	unlink := func() { stopFirst(); stopSecond(); cancel() }
	abandon()
	inner := PipeHint{Admitted: admitted, Source: hint.Source}
	partnerOwner := other.wait
	// The driver is reserved by the completing attachment. Each half
	// contributes one reference, released only after its own request
	// tail and all exclusive work have ended.
	pair := newAttachment(l, other.wait.done != nil || wait.done != nil)
	pair.refs.Store(2) // initialized before either half can release it
	pair.onEnd = func() { l.mu.Lock(); l.pairing.Forget(t); l.mu.Unlock() }
	partnerOwner.pair, wait.pair = pair, pair
	if firstHead := partnerOwner.head; firstHead != nil {
		secondHead := wait.head
		wait.head = func(ctx context.Context) error {
			if err := firstHead(ctx); err != nil {
				return err
			}
			if secondHead != nil {
				return secondHead(ctx)
			}
			return nil
		}
		partnerOwner.head = nil
	}
	partnerOwner.onEnd, wait.onEnd = pair.release, pair.release
	wait.parentsEnded = func() bool { return first.Err() != nil && second.Err() != nil }
	wait.pipe = both
	err = l.handleAdmitted(ctx, inner, wait)
	if wait.state.Load() == setupStalled {
		wait.onEnd = func() { unlink(); pair.release() }
	} else {
		unlink()
	}
	other.done <- err
	return err
}

// ---- helpers ----

func withPrefix(p PipeIO, first []byte) PipeIO {
	if len(first) == 0 {
		return p
	}
	r := p.R
	if carrierio.SameHalf(r, p.W) {
		// The write half owns the close of a shared carrier in either framing.
		r = readOnly{halfCaps: viewOf(r), r: r}
	}
	if p.Framing == FramingRecords {
		p.R = &prefixMessages{halfCaps: viewOf(r), first: first, r: r}
		return p
	}
	p.R = &prefixReader{halfCaps: viewOf(p.R), pre: bytes.Clone(first), r: r}
	return p
}

// halfReportsFailure reports whether h is a FailureReporter that reports.
func halfReportsFailure(h any) bool {
	f, ok := h.(FailureReporter)
	return ok && f.ReportsFailure()
}

// newHalf is h unless it is one of the halves r and w (nil then).
func newHalf(h io.Closer, r, w any) io.Closer {
	if h == nil || carrierio.SameHalf(h, r) || carrierio.SameHalf(h, w) {
		return nil
	}
	return h
}

// tlsSetupFailed reports whether p is a TLS connection whose handshake did
// not complete: the read failed below the carrier.
func tlsSetupFailed(p PipeIO) bool {
	switch c := unprefixed(p.R).(type) {
	case *tls.Conn:
		return !c.ConnectionState().HandshakeComplete
	case *secureTLSConn:
		return !c.ConnectionState().HandshakeComplete
	}
	return false
}
