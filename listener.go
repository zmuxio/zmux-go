package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/setting"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// ListenerConfig configures a Listener under the Settings rules of the
// package documentation. These are safety bounds: the zero value of every
// field selects its stated default. A negative FirstRecordTimeout is no
// timeout and a negative MemoryBudget unlimited; any other negative
// value makes NewListener fail with ErrInvalidConfig.
type ListenerConfig struct {
	// Config is the configuration of the sessions it accepts, validated by
	// NewListener as a server session's. Its HandshakeTimeout also bounds
	// how long a carrier of the listener waits for its split-bootstrap
	// partner or for the handshake of a join that arrived in early data;
	// its JoinTimeout bounds a stalled join.
	Config *Config
	// Logger receives the listener's warnings and the panics of its host
	// callbacks that it contains (OnReject, SessionConfig, Admission) and
	// the errors SessionConfig returns;
	// nil logs nothing. The sessions log to their Config.Logger.
	Logger *slog.Logger
	// MaxSuspended bounds the suspended sessions it holds. Zero is 1024;
	// the longest-suspended are closed with ZMUX_RESUME_TIMEOUT.
	MaxSuspended int
	// MaxPending bounds the carriers of the listener before they belong to
	// a session: a carrier holds one of its slots from its arrival, through
	// its handshake, the admission, the inbound transform, a split half's
	// wait for its partner, the classification, join verification and the
	// exchange of transport parameters, and through a stall, until it
	// enters a session, is closed or is handed to a fallback. Zero is 1024;
	// a negative value is unlimited. When every slot is taken, a new
	// carrier closes the oldest stalled carrier to take its slot, or, with
	// none stalled, is closed at once (ListenerStats.Rejected). Stalled
	// carriers hold at most half of MaxPending; a carrier to be stalled
	// beyond that is closed (AdmissionStats.StallOverflow).
	MaxPending int
	// MaxPendingPerSource bounds those carriers per source (zero: 8),
	// stalled ones included: a new carrier beyond it is refused, and a
	// source holds at most MaxPendingPerSource - 1 stalled carriers, a
	// carrier to be stalled beyond that being closed. A source is a client
	// address aggregated to its source prefix (SourcePrefixV4,
	// SourcePrefixV6); deployments behind a carrier-grade NAT raise it.
	// Each half of a split bootstrap holds a slot until the pair is
	// complete, so split bootstraps need at least 2.
	MaxPendingPerSource int
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
	// MaxPendingPerSession bounds the Unconfirmed carriers of one session:
	// joins that passed authentication and wait for the joiner's
	// confirmation. Zero is 4; a new authenticated Hello beyond it evicts
	// the one that has been Unconfirmed longest and is never refused for
	// lack of room.
	MaxPendingPerSession int
	// FirstRecordTimeout closes a carrier whose first record does not
	// arrive in time. Zero is 2 s; a negative value is no timeout.
	FirstRecordTimeout time.Duration
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
	// every locator, which NewListener guarantees.
	OnReject func(RejectInfo) RejectAction
	// MemoryBudget bounds the sum over the listener's sessions of buffered
	// receive data, retained data and session window growth. Zero is
	// 1 GiB; a negative value is unlimited. It replaces the sessions' own
	// Config.MemoryBudget. Above it sessions grant no new credit, refuse
	// new peer streams, new sessions and pipes are refused, and suspended
	// sessions are closed, the largest first; normal operation resumes
	// below three quarters of it.
	MemoryBudget int64
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
	// Fallback receives the byte-stream carriers that the admission
	// decides on with VerdictFallback, with every byte read replayed (once
	// they are read, WriteTo and ReadFrom use the carrier's own, and Unwrap
	// returns it); nil: they are stalled. It returns when it is done
	// with c, and closes it. HTTP carriers fall back to zmuxhttp.Options.Next.
	Fallback func(ctx context.Context, c net.Conn)
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
	// error and a panic are logged (Logger).
	SessionConfig func(ctx context.Context, info *CarrierInfo, admitted any) (*Config, error)
}

// Listener defaults that zmux itself does not recommend (the protocol
// recommends the others).
const (
	defaultMaxSuspended = 1024
	defaultBacklog      = 128
	// defaultMaxPendingPerSession is the default ListenerConfig.MaxPendingPerSession.
	defaultMaxPendingPerSession = 4
	defaultMemoryBudget         = 1 << 30
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
	if r.MaxSuspended < 0 || r.MaxPendingPerSource < 0 || r.MaxPendingPerSession < 0 || r.Backlog < 0 {
		return r, fmt.Errorf("%w: negative ListenerConfig count", ErrInvalidConfig)
	}
	if _, err := r.Config.coreConfig(core.RoleResponder, core.RoleResponder); err != nil {
		return r, fmt.Errorf("ListenerConfig.Config: %w", err)
	}
	if v, ok := r.WrapPipe.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return r, fmt.Errorf("ListenerConfig.WrapPipe: %w", err)
		}
	}
	if r.MaxSuspended == 0 {
		r.MaxSuspended = defaultMaxSuspended
	}
	if r.MaxPending == 0 {
		r.MaxPending = core.DefaultMaxPending
	}
	if r.MaxPendingPerSource == 0 {
		r.MaxPendingPerSource = core.DefaultMaxPendingPerSource
	}
	if r.MaxPendingPerSession == 0 {
		r.MaxPendingPerSession = defaultMaxPendingPerSession
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
	r.FirstRecordTimeout = setting.Timeout(r.FirstRecordTimeout, time.Duration(core.DefaultFirstRecordTimeout))
	if err := r.Admission.Validate(); err != nil {
		return r, err
	}
	if r.Backlog == 0 {
		r.Backlog = defaultBacklog
	}
	if r.MemoryBudget == 0 {
		r.MemoryBudget = defaultMemoryBudget
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
// gracefully. Its lock is a leaf: it never takes a session lock while held.
type Listener struct {
	cfg ListenerConfig

	mu        sync.Mutex
	live      map[*Conn]struct{} // every session it started, until it ends
	sessions  map[string]*Conn   // by locator
	suspended []*Conn            // in suspension order
	pending   core.PendingCarriers
	pairing   core.SplitPairing
	pairs     map[[16]byte]*pairWait
	closed    bool
	draining  bool // Shutdown: no new sessions
	watching  bool // the memory-pressure watcher was started (startWatchLocked)

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
	// refused with ZMUX_PIPE_RESOURCE_LIMIT), new sessions during
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
// Fallback, Stalled + StallOverflow and Closed. A nil l returns nil.
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
	admitted any        // the half's admission value
	paired   chan error // the partner arrived
	done     chan error // the session's result for the waiting half
	end      chan struct{}
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
	l := &Listener{cfg: r, live: map[*Conn]struct{}{}, sessions: map[string]*Conn{},
		pairs: map[[16]byte]*pairWait{}, done: make(chan struct{}), drain: make(chan struct{})}
	l.clk = timeshim.Or(l.cfg.Config.clock())
	l.stopCtx, l.stopCancel = context.WithCancel(context.Background())
	l.accept = make(chan *Conn, l.cfg.Backlog)
	_, _ = rand.Read(l.stallKey[:])
	l.pending.MaxTotal = l.cfg.MaxPending
	l.pending.MaxPerSource = l.cfg.MaxPendingPerSource
	l.pending.Timeout = core.Duration(l.cfg.FirstRecordTimeout)
	var limit uint64 // unlimited
	if l.cfg.MemoryBudget > 0 {
		limit = uint64(l.cfg.MemoryBudget)
	}
	l.budget = core.NewBudget(limit)
	if a := l.cfg.Admission; a != nil {
		l.stallMax = a.r.stallMax
	} else {
		l.stallMax = drawDuration(30*time.Second, 120*time.Second)
	}
	return l, nil
}

// startWatchLocked starts the memory-pressure watcher with the first
// carrier the listener takes, so that a listener that never serves owns no
// goroutine; Close stops it. The caller holds l.mu, the listener is open,
// and it calls the returned function, if any, after unlocking.
func (l *Listener) startWatchLocked() (start func()) {
	if l.watching || l.budget.Limit() == 0 {
		return nil
	}
	l.watching = true
	return func() { labels.Go(context.Background(), "listener-budget", func() { l.watchBudget() }) }
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
func (a *ListenerAdapter) FirstRecordTimeout() time.Duration { return a.cfg.FirstRecordTimeout }

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
			c := l.largestSuspended()
			if c == nil {
				break
			}
			closeSuspended([]*Conn{c})
		}
	}
}

// largestSuspended removes and returns the suspended session that holds
// the most memory.
func (l *Listener) largestSuspended() *Conn {
	l.mu.Lock()
	cs := append([]*Conn(nil), l.suspended...)
	l.mu.Unlock()
	var best *Conn
	var most uint64
	for _, c := range cs { // session locks outside the listener lock
		c.lock()
		m := c.s.Charged()
		c.unlock()
		if best == nil || m > most {
			best, most = c, m
		}
	}
	if best != nil {
		l.mu.Lock()
		l.dropSuspended(best)
		l.mu.Unlock()
	}
	return best
}

// Accept returns the next session a carrier started.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-l.accept:
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
// started continue, without new pipes from the listener (Shutdown ends
// them).
func (l *Listener) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.done)
		if !l.draining {
			l.draining = true
			close(l.drain)
		}
	}
	l.mu.Unlock()
	l.stopCancel()
	return nil
}

// Shutdown closes the listener gracefully, as http.Server.Shutdown does:
// Accept returns net.ErrClosed and new sessions are refused at once, while
// carriers that join or resume the listener's sessions are still taken
// (through the same admission); every session it started, accepted or
// not, is shut down with Conn.Shutdown(ctx, code, msg) in parallel: each
// sends GOAWAY, lets its open streams finish and closes. Once every
// session has ended, or ctx has (the remaining sessions are then closed
// at once and ctx's error is returned), Shutdown calls Close.
func (l *Listener) Shutdown(ctx context.Context, code ApplicationErrorCode, msg string) error {
	l.mu.Lock()
	if !l.draining {
		l.draining = true
		close(l.drain)
	}
	cs := make([]*Conn, 0, len(l.live))
	for c := range l.live {
		cs = append(cs, c)
	}
	l.mu.Unlock()
	errs := make(chan error, len(cs))
	for _, c := range cs {
		go func() { errs <- c.Shutdown(ctx, code, msg) }()
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
// connection to ListenerConfig.Fallback. During Shutdown it keeps serving
// joins and resumptions. For TLS, pass a listener made
// by tls.NewListener. Serve closes ln when it returns: when ln.Accept
// fails, when ctx ends (ctx's error), or when the listener is closed
// (net.ErrClosed). Cancelling ctx also aborts the carriers Serve handed
// over, ending their pipes; Shutdown or Close leave them to the sessions.
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

// HandlePipe classifies a new carrier by its first record (or its hint)
// and starts a session on it, attaches it to a session as a pipe, or
// closes it. It blocks for the lifetime of the pipe, so that an HTTP
// handler stays alive; when ctx ends the pipe is aborted. The carrier
// takes a slot of its source first (hint.Source, or the remote address of
// a half: Arrive) and passes the listener's admission (Arrival.HandlePipe).
// A carrier that is not yet classified, or that joins a session and is
// not yet verified, also holds a pending slot (ListenerConfig.MaxPending).
func (l *Listener) HandlePipe(ctx context.Context, p PipeIO, hint PipeHint) error {
	a, err := l.arrive(hint.Source, peerOf(p))
	if errors.Is(err, errRefused) {
		return l.refusePending(ctx, p, unmapped(hint.Source), peerOf(p))
	}
	if err != nil {
		closePipeIO(p)
		return err
	}
	return a.HandlePipe(ctx, p, hint)
}

// refusePending refuses a carrier beyond the pending limits, the first check
// of an accepted carrier: it goes to the reject policy, reason "pending_limit",
// like every refusal before authentication, and is closed.
func (l *Listener) refusePending(ctx context.Context, p PipeIO, src, peer netip.AddrPort) error {
	if !src.IsValid() {
		src = unmapped(peer)
	}
	l.onReject(ctx, p, RejectInfo{Reason: "pending_limit", Src: src})
	closePipeIO(p)
	return errRefused
}

// take hands an admitted carrier, which holds slot, to the listener, or
// refuses it at once (a closed listener, a split half during Shutdown),
// before it waits for anything. run then does the rest of HandlePipe; the
// slots are given back once the carrier enters a session or is closed.
func (l *Listener) take(p PipeIO, hint PipeHint, slot *core.PendingSlot) (run func(context.Context) error, err error) {
	release := func() { l.leave(slot) }
	l.mu.Lock()
	ok := !l.closed && (hint.Pair == "" || !l.draining)
	l.mu.Unlock()
	if !ok {
		release()
		closePipeIO(p)
		l.st.refused.Add(1)
		return nil, errRefused
	}
	return func(ctx context.Context) error {
		defer release()
		ctx = context.WithValue(ctx, slotKey{}, slot)
		if hint.Pair != "" {
			return l.handlePair(ctx, p, hint, release)
		}
		p, err := l.wrapPipe(ctx, p, hint)
		if err != nil {
			return err
		}
		return l.handleAdmitted(ctx, p, hint, release)
	}, nil
}

// slotKey is the context key of the slots of the carrier a listener
// handles, which a stall keeps.
type slotKey struct{}

// wrapPipe applies ListenerConfig.WrapPipe to p, within the handshake
// timeout; p is closed when it fails.
func (l *Listener) wrapPipe(ctx context.Context, p PipeIO, hint PipeHint) (PipeIO, error) {
	wrap := l.cfg.WrapPipe
	if wrap == nil {
		return p, nil
	}
	ctx, stopL := l.stopContext(ctx)
	defer stopL()
	ctx, cancel := timeshim.WithTimeout(ctx, l.clk, l.cfg.Config.handshakeTimeout())
	defer cancel()
	// A layer that does not watch ctx is interrupted by closing p.
	stop := context.AfterFunc(ctx, func() { closePipeIO(p) })
	q, err := wrap.WrapPipe(ctx, p, hint)
	fired := !stop()
	if rej := (*RejectError)(nil); !fired && errors.As(err, &rej) {
		markTaken(p)
		l.st.rejected.Add(1)
		l.onReject(ctx, p, RejectInfo{Profile: rej.Profile, RecordMode: rej.RecordMode, Reason: rej.Reason, Src: hint.Source, Err: rej.Err})
		closePipeIO(p)
		return PipeIO{}, errRejected
	}
	if err != nil {
		// Halves a failing layer returned anyway are closed too.
		return l.wrapFailed(p, q, fired, err)
	}
	if fired {
		return l.wrapFailed(p, q, fired, context.Cause(ctx))
	}
	if q.R == nil && q.W == nil {
		return l.wrapFailed(p, q, fired, errors.New("no pipe halves"))
	}
	if err := q.checkOverhead(); err != nil {
		return l.wrapFailed(p, q, fired, err)
	}
	if !sameHalf(q.R, p.R) || !sameHalf(q.W, p.W) {
		markTaken(p)
	}
	q.Carrier, q.Label, q.FateGroup = p.Carrier, p.Label, p.FateGroup
	if q.Variant == "" {
		q.Variant = p.Variant
	}
	return q, nil
}

// wrapFailed closes what wrapPipe holds after err: q's halves, and p
// unless the bound fired (which closed it), and counts a setup failure.
func (l *Listener) wrapFailed(p, q PipeIO, fired bool, err error) (PipeIO, error) {
	markTaken(p) // what the layer read is no longer a request to hand on
	if q.R != nil || q.W != nil {
		closePipeIO(q)
	}
	if !fired {
		closePipeIO(p)
	}
	l.countSetupFailure()
	return PipeIO{}, fmt.Errorf("zmux: WrapPipe: %w", err)
}

// handleAdmitted is HandlePipe for an admitted carrier.
func (l *Listener) handleAdmitted(ctx context.Context, p PipeIO, hint PipeHint, release func()) error {
	first, cls, err := l.readFirst(ctx, p)
	if err != nil {
		closePipeIO(p)
		if tlsSetupFailed(p) {
			l.countSetupFailure()
		} else {
			l.rejectedBy(err)
		}
		return err
	}
	switch cls {
	case core.ClassSession:
		admitted := hint.Admitted
		if v := carrierPeer(p); v != nil {
			admitted = v
		}
		return l.startSession(ctx, withPrefix(p, first), admitted, hint.Source, release)
	case core.ClassJoin:
		return l.join(ctx, p, first, hint, release)
	case core.ClassLane:
		release()
		return handleLane(ctx, l, withPrefix(p, first), first)
	case core.ClassMore, core.ClassReject:
	}
	closePipeIO(p)
	l.st.rejected.Add(1)
	return errRejected
}

// readFirst reads until the first record is complete (or rejected),
// within the first-record timeout; ctx ending or the listener closing
// abandon the carrier as well.
func (l *Listener) readFirst(ctx context.Context, p PipeIO) ([]byte, core.Class, error) {
	type res struct {
		b   []byte
		cls core.Class
		err error
	}
	ch := make(chan res, 1)
	labels.Go(ctx, "first-record", func() {
		b, cls, err := readFirstRecord(p)
		ch <- res{b, cls, err}
	})
	timer := timeshim.NewTimer(l.clk, l.cfg.FirstRecordTimeout)
	defer timer.Stop()
	var err error
	select {
	case r := <-ch:
		return r.b, r.cls, r.err
	case <-timer.C:
		err = errTimedOut
	case <-ctx.Done():
		err = ctx.Err()
	case <-l.done:
		err = net.ErrClosed
	}
	closePipeIO(p) // interrupts the read
	<-ch
	return nil, core.ClassReject, err
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

// markTaken tells p's receiving half, through the wrappers below, that the
// listener took the carrier: a session, a join or a lane attaches it.
func markTaken(p PipeIO) {
	x := any(p.R)
	for x != nil {
		switch r := x.(type) {
		case CarrierTaker:
			r.CarrierTaken()
			return
		case *prefixReader:
			x = r.r
		case *prefixMessages:
			x = r.r
		case readOnly:
			x = r.r
		default:
			return
		}
	}
}

// readFirstRecord reads a carrier's first record.
func readFirstRecord(p PipeIO) ([]byte, core.Class, error) {
	if p.Framing == FramingRecords {
		mr, ok := p.R.(MessageReader)
		if !ok {
			return nil, core.ClassReject, errors.New("zmux: Records framing needs a MessageReader")
		}
		m, err := mr.ReadMessage()
		if err != nil {
			return nil, core.ClassReject, err
		}
		if len(m) > 0 && m[0] == join.ProfileJoin {
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
// The carrier's slots are given back with release once its transport
// parameters have been exchanged, or it failed.
func (l *Listener) startSession(ctx context.Context, p PipeIO, admitted any, src netip.AddrPort, release func()) error {
	cfg, err := l.sessionConfig(ctx, p, admitted, src)
	if err != nil {
		closePipeIO(p)
		l.st.rejected.Add(1)
		return err
	}
	cc, err := cfg.coreConfig(core.RoleResponder, core.RoleResponder)
	if err != nil {
		closePipeIO(p)
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
	markTaken(p)
	c, err := startCore(ctx, p, cfg, cc, l, admitted)
	release()
	if err != nil {
		l.st.rejected.Add(1)
		return err
	}
	reason := ""
	if l.budget.Pressure() {
		reason = "memory budget"
	} else {
		l.mu.Lock()
		if l.draining {
			reason = "draining"
		} else {
			// Counted before Accept can return it.
			l.st.sessions.Add(1)
			select {
			case l.accept <- c:
				// startCore registered the locator before bootstrap I/O.
				// sessionClosed owns its removal, including handshake failures.
			default:
				l.st.sessions.Add(^uint64(0))
				reason = "backlog"
			}
		}
		l.mu.Unlock()
	}
	if reason != "" {
		// Refused by policy after its own parameters.
		c.lock()
		c.s.Refuse(reason)
		c.unlock()
		_ = c.Wait()
		l.st.refused.Add(1)
		return errRefused
	}
	return c.waitPipe(ctx, 0)
}

// sessionConfig is a copy of the Config of a new session on p:
// ListenerConfig.SessionConfig's, checked against the listener's, or
// Config.
func (l *Listener) sessionConfig(ctx context.Context, p PipeIO, admitted any, src netip.AddrPort) (*Config, error) {
	f := l.cfg.SessionConfig
	if f == nil {
		return l.cfg.Config.Clone(), nil
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
		return l.cfg.Config.Clone(), nil
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
	if len(prefix) == 0 || prefix[0] != join.ProfileJoin {
		return false, 0
	}
	h, n, err := join.ParseHelloHead(prefix, join.Stream)
	if errors.Is(err, join.ErrIncomplete) {
		return false, n
	}
	if err != nil {
		return false, 0
	}
	l.mu.Lock()
	_, ok := l.sessions[string(h.Locator)]
	l.mu.Unlock()
	return ok, 0
}

// join runs the acceptor's side of a Hello. The carrier keeps its
// pending slot until it is Confirmed.
func (l *Listener) join(ctx context.Context, p PipeIO, b []byte, hint PipeHint, release func()) error {
	jf := join.Stream
	if p.Framing == FramingRecords {
		jf = join.Message
	}
	reject := func(mode RecordMode, reason string) error {
		l.st.rejected.Add(1)
		l.onReject(ctx, p, RejectInfo{Profile: join.ProfileJoin, RecordMode: mode, Reason: reason, Src: hint.Source})
		closePipeIO(p)
		return errRejected
	}
	h, _, err := join.ParseHelloExact(b, jf)
	if errors.Is(err, join.ErrUnknownSuite) {
		return reject(0, "unknown_suite")
	}
	if err != nil {
		return reject(0, "malformed")
	}
	l.mu.Lock()
	c, closed := l.sessions[string(h.Locator)], l.closed
	l.mu.Unlock()
	if closed {
		closePipeIO(p)
		l.st.refused.Add(1)
		return errRefused
	}
	if c == nil {
		return reject(RecordMode(h.Mode), "unknown_locator")
	}
	if l.budget.Pressure() {
		// Under pressure only a join that lets a suspended session resume.
		l.mu.Lock()
		susp := slices.Contains(l.suspended, c)
		l.mu.Unlock()
		if !susp {
			return reject(RecordMode(h.Mode), "memory_pressure")
		}
	}
	// A Hello that arrived as carrier early data waits for the carrier's handshake, as
	// its Binding does.
	if es, ok := p.W.(EarlyServerCarrier); ok && es.UsedEarlyData() {
		timer := timeshim.NewTimer(l.clk, l.cfg.Config.handshakeTimeout())
		defer timer.Stop()
		select {
		case <-es.HandshakeComplete():
		case <-timer.C:
			closePipeIO(p)
			l.countSetupFailure()
			return errRejected
		case <-ctx.Done():
		case <-l.done:
		}
		if ctx.Err() != nil || l.isClosed() {
			closePipeIO(p)
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
	hctx, unlink := l.stopContext(ctx)
	id, err := c.acceptHello(hctx, p, h, binding, hint.Admitted, l.cfg.MaxPendingPerSession)
	unlink()
	if err == nil {
		release() // Confirmed: the carrier is the session's
	}
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
			closePipeIO(p)
		}
		l.rejectedBy(err)
		return err
	}
	l.st.joins.Add(1)
	return c.waitPipe(ctx, id)
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
	stop := context.AfterFunc(ctx, func() { closePipeIO(p) })
	defer stop()
	if cw, ok := p.W.(CloseWriter); ok {
		_ = cw.CloseWrite()
	}
	if p.R != nil {
		discard(p.R)
	}
	closePipeIO(p)
}

// onReject decides the response to a rejected carrier, OnReject or the
// default (RejectStall for a transform's rejection before
// authentication, RejectClose for a join), and stalls it if so. The
// caller closes p.
func (l *Listener) onReject(ctx context.Context, p PipeIO, info RejectInfo) {
	act := RejectClose
	if info.Profile != join.ProfileJoin {
		act = RejectStall
	}
	if f := l.cfg.OnReject; f != nil && !safeLog(l.cfg.Logger, "ListenerConfig.OnReject", func() { act = f(info) }) {
		act = RejectClose
	}
	if act == RejectStall {
		l.stall(ctx, p, info)
	}
}

// stall holds a rejected carrier, silent, in a stall slot of its source:
// a source whose share of stalled carriers is full has it closed at once,
// whatever the reason. A rejected join ends at the join timeout of the
// listener's Config, whether or not it named a session, counted from the
// rejection; a transform's rejection at the handshake timeout, at most the
// listener's stall bound (Admission.StallMax; without one drawn when the
// listener was made).
func (l *Listener) stall(ctx context.Context, p PipeIO, info RejectInfo) {
	slot, _ := ctx.Value(slotKey{}).(*core.PendingSlot)
	evicted := make(chan struct{})
	l.mu.Lock()
	ok := slot != nil && !l.closed && l.pending.Stall(slot, sync.OnceFunc(func() { close(evicted) }))
	l.mu.Unlock()
	if !ok {
		return
	}
	defer func() {
		l.mu.Lock()
		l.pending.Unstall(slot)
		l.mu.Unlock()
	}()
	markTaken(p)
	var d time.Duration
	switch {
	case info.Profile != join.ProfileJoin:
		d = min(l.cfg.Config.handshakeTimeout(), l.stallMax)
	default:
		d = l.cfg.Config.joinTimeout()
	}
	t := timeshim.NewTimer(l.clk, d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	case <-l.done:
	case <-evicted:
	}
}

// addLocator lets joins name a session by a Locator of a new epoch.
func (l *Listener) addLocator(c *Conn, loc []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, live := l.live[c]; live && !l.closed {
		l.sessions[string(loc)] = c
	}
}

// removeLocator forgets the Locator of a deleted epoch.
func (l *Listener) removeLocator(c *Conn, loc []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sessions[string(loc)] == c {
		delete(l.sessions, string(loc))
	}
}

// sessionClosed forgets a session that ended.
func (l *Listener) sessionClosed(c *Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.live, c)
	for k, v := range l.sessions {
		if v == c {
			delete(l.sessions, k)
		}
	}
	l.dropSuspended(c)
}

// sessionSuspended records a suspension and returns the sessions to close
// beyond MaxSuspended (closed by the caller outside every lock).
func (l *Listener) sessionSuspended(c *Conn) []*Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropSuspended(c)
	l.suspended = append(l.suspended, c)
	var over []*Conn
	for len(l.suspended) > l.cfg.MaxSuspended {
		over = append(over, l.suspended[0])
		l.suspended = l.suspended[1:]
	}
	return over
}

func (l *Listener) sessionResumed(c *Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropSuspended(c)
}

func (l *Listener) dropSuspended(c *Conn) {
	for i, x := range l.suspended {
		if x == c {
			l.suspended = append(l.suspended[:i], l.suspended[i+1:]...)
			return
		}
	}
}

// ---- split bootstrap ----

// The half holds its slots until the pair is complete, and the pair goes
// on with those of the half that completed it; release gives them back.
func (l *Listener) handlePair(ctx context.Context, p PipeIO, hint PipeHint, release func()) error {
	tok, err := base64.RawURLEncoding.DecodeString(hint.Pair)
	if err != nil || len(tok) != 16 {
		closePipeIO(p)
		l.st.rejected.Add(1)
		return errRejected
	}
	var t [16]byte
	copy(t[:], tok)
	hto := l.cfg.Config.handshakeTimeout()
	me := &pairWait{p: p, admitted: hint.Admitted, paired: make(chan error, 1), done: make(chan error, 1), end: make(chan struct{})}
	l.mu.Lock()
	res, partner := l.pairing.Offer(t, hint.Uplink, me)
	l.mu.Unlock()
	switch res {
	case core.PairReject:
		closePipeIO(p)
		l.st.rejected.Add(1)
		return errRejected
	case core.PairWait:
		timer := timeshim.NewTimer(l.clk, hto)
		defer timer.Stop()
		select {
		case err := <-me.paired:
			release() // the pair is complete: its slots are the partner's
			if err == nil {
				err = <-me.done
			}
			if err != nil {
				return err
			}
			<-me.end
			return nil
		case <-timer.C:
		case <-ctx.Done():
		case <-l.done:
		}
		l.mu.Lock()
		mine := l.pairing.Withdraw(t, me)
		l.mu.Unlock()
		if mine {
			// A half never paired is an input of its own.
			closePipeIO(p)
			if ctx.Err() != nil {
				l.st.rejected.Add(1) // abandoned by its caller
				return ctx.Err()
			}
			if l.isClosed() {
				l.st.refused.Add(1)
				return net.ErrClosed
			}
			l.st.timedOut.Add(1) // no partner within the handshake timeout
			return errTimedOut
		}
		// A partner took this carrier meanwhile: it is the session's now.
		release()
		if err := <-me.done; err != nil {
			return err
		}
		<-me.end
		return nil
	case core.PairDone:
	}
	other := partner.(*pairWait)
	other.paired <- nil
	up, down, admitted := p, other.p, hint.Admitted
	if !hint.Uplink {
		up, down, admitted = other.p, p, other.admitted
	}
	// up and down are named for the peer: this end writes on down. What describes the
	// write is the writing half's.
	both := PipeIO{R: up.R, W: down.W, Framing: up.Framing, Label: up.Label, Carrier: up.Carrier, FateGroup: up.FateGroup,
		WriteOverhead: down.WriteOverhead, Variant: up.Variant}
	// Closing one half is no pipe event, so the pipe does not live by the context of
	// whichever request arrived second. WrapPipe runs on the pair, and classification
	// again, so that a pair can start a session or carry a Join.
	ctx = context.WithoutCancel(ctx)
	inner := PipeHint{Admitted: admitted, Source: hint.Source}
	if both, err = l.wrapPipe(ctx, both, inner); err == nil {
		err = l.handleAdmitted(ctx, both, inner, release)
	}
	other.done <- err
	close(other.end)
	// The token stays used while its session lives (no second pairing).
	l.mu.Lock()
	l.pairing.Forget(t)
	l.mu.Unlock()
	return err
}

// ---- helpers ----

// prefixReader returns what was read for classification before the rest
// of the carrier.
type prefixReader struct {
	r   io.ReadCloser
	pre []byte
}

func (p *prefixReader) Read(b []byte) (int, error) {
	if len(p.pre) > 0 {
		n := copy(b, p.pre)
		p.pre = p.pre[n:]
		return n, nil
	}
	return p.r.Read(b)
}

func (p *prefixReader) Close() error { return p.r.Close() }

// prefixMessages returns the first message before the rest.
type prefixMessages struct {
	r     io.ReadCloser
	first []byte
}

func (p *prefixMessages) Read([]byte) (int, error) { return 0, errors.New("zmux: message carrier") }
func (p *prefixMessages) Close() error             { return p.r.Close() }
func (p *prefixMessages) ReadMessage() ([]byte, error) {
	if p.first != nil {
		m := p.first
		p.first = nil
		return m, nil
	}
	return p.r.(MessageReader).ReadMessage()
}

func withPrefix(p PipeIO, first []byte) PipeIO {
	if len(first) == 0 {
		return p
	}
	if p.Framing == FramingRecords {
		p.R = &prefixMessages{first: first, r: p.R}
		return p
	}
	if same := sameHalf(p.R, p.W); same {
		// Keep the write half as it is; the read half is wrapped.
		p.R = &prefixReader{pre: bytes.Clone(first), r: readOnly{p.R}}
		return p
	}
	p.R = &prefixReader{pre: bytes.Clone(first), r: p.R}
	return p
}

// readOnly closes nothing: the carrier is closed through its write half.
type readOnly struct{ r io.ReadCloser }

func (r readOnly) Read(b []byte) (int, error) { return r.r.Read(b) }
func (r readOnly) Close() error               { return nil }

// carrierConnOf is the carrier connection of a half, through the wrappers
// above (lanes are accepted only on their pipe's connection).
func carrierConnOf(x any) any {
	switch r := x.(type) {
	case *prefixReader:
		return carrierConnOf(r.r)
	case *prefixMessages:
		return carrierConnOf(r.r)
	case readOnly:
		return carrierConnOf(r.r)
	case CarrierConner:
		return r.CarrierConn()
	}
	return nil
}

func (p *prefixReader) CarrierConn() any   { return carrierConnOf(p.r) }
func (p *prefixMessages) CarrierConn() any { return carrierConnOf(p.r) }

func closePipeIO(p PipeIO) {
	if p.W != nil {
		_ = p.W.Close()
	}
	if p.R != nil && !sameHalf(p.R, p.W) {
		_ = p.R.Close()
	}
}

// sameHalf reports whether two halves of a carrier are one value. The halves come
// from the host and may be of a type that cannot be compared; those count as two.
func sameHalf(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.ValueOf(a).Comparable() && reflect.ValueOf(b).Comparable() && a == b
}

// tlsSetupFailed reports whether p is a TLS connection whose handshake did
// not complete: the read failed below the carrier.
func tlsSetupFailed(p PipeIO) bool {
	switch c := p.R.(type) {
	case *tls.Conn:
		return !c.ConnectionState().HandshakeComplete
	case *secureTLSConn:
		return !c.ConnectionState().HandshakeComplete
	}
	return false
}
