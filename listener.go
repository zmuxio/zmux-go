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
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// ListenerConfig configures a Listener under the Settings rules of the
// package documentation. These are safety bounds: the zero value of every
// field selects its stated default. A negative FirstRecordTimeout is no
// timeout and MemoryBudget NoMemoryLimit unlimited; NewListener panics on
// any other negative value.
type ListenerConfig struct {
	// Config is the configuration of the sessions it accepts. Its
	// HandshakeTimeout also bounds how long a carrier of the listener waits
	// for its split-bootstrap partner or for the handshake of a join that
	// arrived in early data.
	Config *Config
	// MaxSuspended bounds the suspended sessions it holds. Zero is 1024;
	// the longest-suspended are closed with ZMUX_RESUME_TIMEOUT.
	MaxSuspended int
	// MaxPending bounds the carriers of the listener that have not been
	// classified, or that join a session and have not passed join
	// verification. Zero is 64; when they are full, a new carrier closes the
	// oldest of them rather than being refused.
	MaxPending int
	// MaxPendingPerSource bounds those carriers per source (zero: 8), together
	// with the carriers an Admission stalls: a new carrier beyond it is
	// refused, and one to be stalled is closed. A source is a client address, its /64
	// prefix for IPv6; deployments behind a carrier-grade NAT raise it.
	// Each half of a split bootstrap holds a slot until it waits for its
	// partner, so split bootstraps need at least 2.
	MaxPendingPerSource int
	// FirstRecordTimeout closes a carrier whose first record does not
	// arrive in time. Zero is 2 s; a negative value is no timeout.
	FirstRecordTimeout time.Duration
	// Backlog is the number of sessions waiting for Accept (zero: 128); a new
	// session beyond it is refused.
	Backlog int
	// InstanceID is encoded at the start of every session locator, for
	// load-balancer stickiness; random bytes fill the rest of the 16-byte
	// locator. At most MaxInstanceIDLen bytes.
	InstanceID []byte
	// MemoryBudget bounds the sum over the listener's sessions of buffered
	// receive data, retained data and session window growth. Zero is
	// 1 GiB; NoMemoryLimit is unlimited. It replaces the sessions' own
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
	// panics if it is invalid (Admission.Validate).
	Admission *Admission
	// Fallback receives the byte-stream carriers that the admission
	// decides on with VerdictFallback, with every byte read replayed
	// (PrefixedConn); nil: they are stalled. It returns when it is done
	// with c, and closes it. HTTP carriers fall back to HTTPOptions.Next.
	Fallback func(ctx context.Context, c net.Conn)
	// WrapPipe, when set, is called once for every carrier the listener
	// takes, whatever fed it (Serve, HandlePipe, the HTTP,
	// exchange and adapter handlers), before the carrier is classified. It
	// returns the PipeIO the listener uses in place of p, such as a layer
	// over PipeConn(p), or p itself. hint is the carrier's: hint.Pair marks
	// one half of a split bootstrap, a p with one half only, which WrapPipe
	// may return as it is.
	//
	// While it runs the carrier holds its pending slot (MaxPending,
	// MaxPendingPerSource; a split half holds a pairing slot only once
	// WrapPipe has returned), and Config.HandshakeTimeout bounds it: ctx
	// ends at that bound or when the carrier is evicted, and p is closed
	// then. FirstRecordTimeout runs from its return. The listener sets p's
	// Carrier, Label and FateGroup on the result, whose halves and Framing
	// are WrapPipe's. An error, or the bound, closes p and counts a setup
	// failure (ListenerStats.SetupFailures).
	//
	// A layer must map every write on the PipeIO it returns to exactly
	// one write on p, never merging writes.
	WrapPipe func(ctx context.Context, p PipeIO, hint JoinHint) (PipeIO, error)
}

// Listener defaults that zmux itself does not recommend (MULTIPATH.md 5.6
// covers the others).
const (
	defaultMaxSuspended = 1024
	defaultBacklog      = 128
	defaultMemoryBudget = 1 << 30
)

// resolve returns c with every default filled in.
func (c *ListenerConfig) resolve() ListenerConfig {
	var r ListenerConfig
	if c != nil {
		r = *c
	}
	if len(r.InstanceID) > MaxInstanceIDLen {
		panic("zmux: ListenerConfig.InstanceID longer than MaxInstanceIDLen")
	}
	if r.MaxSuspended < 0 || r.MaxPending < 0 || r.MaxPendingPerSource < 0 || r.Backlog < 0 || r.MemoryBudget < 0 && r.MemoryBudget != NoMemoryLimit {
		panic("zmux: negative ListenerConfig count or MemoryBudget")
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
	r.FirstRecordTimeout = timeout(r.FirstRecordTimeout, time.Duration(core.DefaultFirstRecordTimeout))
	if err := r.Admission.Validate(); err != nil {
		panic(err.Error())
	}
	if r.Backlog == 0 {
		r.Backlog = defaultBacklog
	}
	if r.MemoryBudget == 0 {
		r.MemoryBudget = defaultMemoryBudget
	}
	return r
}

// JoinHint is what a carrier's request said out of band (HTTP headers).
type JoinHint struct {
	// Join is the Zmux-Pipe-Join header: an out-of-band PIPE_JOIN.
	Join string
	// Pair is the Zmux-Pipe-Pair header: the pairing token of a split
	// bootstrap (SPEC.md 6.1.1); Uplink says which half this carrier is.
	// A half waits for its partner in one of 64 pairing slots of the
	// listener, at most 8 per Source; without an Admission in front of
	// it, a few sources can hold them all, so a split bootstrap that the
	// open Internet reaches is served with one.
	Pair   string
	Uplink bool
	// Source is the carrier's client address (CarrierInfo.Source), for a
	// host that resolves it itself (zero: the address the carrier arrived
	// from). The adapters set it from TrustedProxies.
	Source netip.AddrPort
}

// Listener is the server side of zmux: it accepts sessions and the pipes
// that join them (MULTIPATH.md 5.4, 5.5), so it is the way to serve
// clients that use multipath, resume or split carriers. Serve feeds it the
// connections of a net.Listener (TCP, or TLS with tls.NewListener);
// HandlePipe feeds it any other carrier, and NewHTTPHandler HTTP carriers.
// Accept returns the sessions it started:
//
//	l := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
//	go l.Serve(ctx, ln)
//	for {
//		conn, err := l.Accept(ctx)
//		if err != nil {
//			return err
//		}
//		go handle(conn)
//	}
//
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

	accept     chan *Conn
	done       chan struct{} // closed by Close
	drain      chan struct{} // closed by Shutdown or Close
	stopCtx    context.Context
	stopCancel context.CancelFunc
	clk        timeshim.Clock
	budget     *core.Budget
	stallKey   [32]byte // drawn by NewListener
	st         listenerCounters
}

// ListenerStats are the counts of a listener since it was made (API.md
// 7.2). The unit is the pipe input handed to the listener: a carrier, a
// lane, or the two carriers of a split pipe once paired, which are one
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
	// Rejected counts inputs closed without a distinguishable reason:
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
	sessions, joins, setupFailures, rejected, refused, timedOut atomic.Uint64
	adm                                                         admissionCounters
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

// CountSetupFailure counts a connection that never became a pipe input
// (ListenerStats.SetupFailures). Adapters that run their own accept
// loops call it for a failed TLS handshake or its timeout, a failed HTTP
// or WebSocket upgrade, or their setup limit; the listener's own serve
// loops count theirs. A nil l does nothing.
func (l *Listener) CountSetupFailure() {
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
	p    PipeIO
	done chan error // the session's result for the waiting half
	end  chan struct{}
}

// MaxInstanceIDLen bounds ListenerConfig.InstanceID so that at least 8
// random bytes remain in the 16-byte session locator.
const MaxInstanceIDLen = 8

// NewListener returns a listener; feed it carriers with Serve, HandlePipe
// or NewHTTPHandler. cfg may be nil. It panics if cfg.InstanceID is longer than
// MaxInstanceIDLen or a count or MemoryBudget is negative (other than
// NoMemoryLimit).
func NewListener(cfg *ListenerConfig) *Listener {
	l := &Listener{cfg: cfg.resolve(), live: map[*Conn]struct{}{}, sessions: map[string]*Conn{},
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
	if limit > 0 {
		go l.watchBudget()
	}
	return l
}

// MemoryUsed reports the bytes charged to the listener's memory budget,
// in the unit of ListenerConfig.MemoryBudget.
func (l *Listener) MemoryUsed() int64 { return int64(l.budget.Used()) }

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
	go func() {
		select {
		case <-ctx.Done():
		case <-l.done:
		case <-stop:
		}
		_ = ln.Close()
	}()
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
		go func() { _ = l.HandlePipe(ctx, NetPipe(nc), JoinHint{}) }()
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
)

// HandlePipe classifies a new carrier by its first record (or its hint)
// and starts a session on it, attaches it to a session as a pipe, or
// closes it. It blocks for the lifetime of the pipe, so that an HTTP
// handler stays alive; when ctx ends the pipe is aborted. The carrier
// takes a slot of its source first (hint.Source, or the remote address of
// a half: Arrive) and passes the listener's admission (Arrival.HandlePipe).
// A carrier that is not yet classified, or that joins a session and is
// not yet verified, also holds a pending slot (ListenerConfig.MaxPending).
func (l *Listener) HandlePipe(ctx context.Context, p PipeIO, hint JoinHint) error {
	a, err := l.Arrive(hint.Source, peerOf(p))
	if err != nil {
		closePipeIO(p)
		return err
	}
	return a.HandlePipe(ctx, p, hint)
}

// take hands an admitted carrier, which holds the source slot of key, to
// the listener: it takes its pending slot, or refuses it at once (a
// closed listener, a split half during Shutdown), before it waits for
// anything. run then does the rest of HandlePipe; the source slot is given
// back once the carrier is classified or closed.
func (l *Listener) take(p PipeIO, hint JoinHint, key string) (run func(context.Context) error, err error) {
	leave := sync.OnceFunc(func() { l.leave(key) })
	if hint.Pair != "" {
		l.mu.Lock()
		ok := !l.draining
		l.mu.Unlock()
		if !ok {
			leave()
			closePipeIO(p)
			l.st.refused.Add(1)
			return nil, errRefused
		}
		return func(ctx context.Context) error {
			p, err := l.wrapPipe(ctx, p, hint)
			leave() // the half waits in a pairing slot of its own
			if err != nil {
				return err
			}
			return l.handlePair(ctx, p, hint, key)
		}, nil
	}
	// Eviction ends the carrier's wait (first record, join handshake) and
	// closes it.
	evict, cancelEvict := context.WithCancel(context.Background())
	var slot *core.PendingCarrier
	var evicted func()
	l.mu.Lock()
	ok := !l.closed
	if ok {
		slot, evicted = l.pending.Take(key, func() { cancelEvict(); closePipeIO(p) })
	}
	l.mu.Unlock()
	if evicted != nil {
		evicted()
	}
	if !ok {
		cancelEvict()
		leave()
		closePipeIO(p)
		l.st.refused.Add(1)
		return nil, errRefused
	}
	return func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(evict, cancel)
		defer stop()
		defer cancelEvict()
		release := func() {
			l.mu.Lock()
			l.pending.Release(slot)
			l.mu.Unlock()
			leave()
		}
		defer release()
		p, err := l.wrapPipe(ctx, p, hint)
		if err != nil {
			return err
		}
		return l.handleAdmitted(ctx, p, hint, release)
	}, nil
}

// wrapPipe applies ListenerConfig.WrapPipe to p, within the handshake
// timeout; p is closed when it fails.
func (l *Listener) wrapPipe(ctx context.Context, p PipeIO, hint JoinHint) (PipeIO, error) {
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
	q, err := wrap(ctx, p, hint)
	fired := !stop()
	if err == nil && fired {
		err = context.Cause(ctx)
	}
	if err == nil && q.R == nil && q.W == nil {
		err = errors.New("no pipe halves")
	}
	if err != nil {
		markTaken(p) // what the layer read is no longer a request to hand on
		if q.R != nil || q.W != nil {
			closePipeIO(q)
		}
		if !fired {
			closePipeIO(p)
		}
		l.CountSetupFailure()
		return PipeIO{}, fmt.Errorf("zmux: WrapPipe: %w", err)
	}
	if any(q.R) != any(p.R) || any(q.W) != any(p.W) {
		markTaken(p)
	}
	q.Carrier, q.Label, q.FateGroup = p.Carrier, p.Label, p.FateGroup
	return q, nil
}

// handleAdmitted is HandlePipe for an admitted carrier.
func (l *Listener) handleAdmitted(ctx context.Context, p PipeIO, hint JoinHint, release func()) error {
	if hint.Join != "" {
		f, err := base64.RawURLEncoding.DecodeString(hint.Join)
		if err != nil {
			closePipeIO(p)
			l.st.rejected.Add(1)
			return errRejected
		}
		j, mac, _, ok := core.ParseJoin(f)
		if !ok {
			closePipeIO(p)
			l.st.rejected.Add(1)
			return errRejected
		}
		return l.join(ctx, p, j, mac, nil, false, release)
	}
	first, cls, err := l.readFirst(ctx, p)
	if err != nil {
		closePipeIO(p)
		if tlsSetupFailed(p) {
			l.CountSetupFailure()
		} else {
			l.rejectedBy(err)
		}
		return err
	}
	switch cls {
	case core.ClassSession:
		release()
		return l.startSession(ctx, withPrefix(p, first))
	case core.ClassJoin:
		body := first
		if p.Framing == FramingByteStream {
			_, n := varint.ConsumeVarint(first)
			body = first[n:]
		}
		j, mac, _, ok := core.ParseJoin(body)
		if !ok {
			closePipeIO(p)
			l.st.rejected.Add(1)
			return errRejected
		}
		return l.join(ctx, withPrefix(p, first), j, mac, nil, true, release)
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
	go func() {
		b, cls, err := readFirstRecord(p)
		ch <- res{b, cls, err}
	}()
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

// firstReadChunk is the size of readFirstRecord's reads; firstReadBound
// bounds what it reads of a byte stream before the carrier is classified:
// a whole record of MaxRecordSize with its length, and one read beyond.
const (
	firstReadChunk = 512
	firstReadBound = MaxRecordSize + 8 + firstReadChunk
)

// carrierTaker is a carrier half that records what the listener reads
// until the listener takes the carrier, so that a carrier it does not take
// can be handed on with those bytes replayed.
type carrierTaker interface{ carrierTaken() }

// markTaken tells p's receiving half, through the wrappers below, that the
// listener took the carrier: a session, a join or a lane attaches it.
func markTaken(p PipeIO) {
	x := any(p.R)
	for x != nil {
		switch r := x.(type) {
		case carrierTaker:
			r.carrierTaken()
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
		case cls != core.ClassMore && rl > 0:
			return b, cls, nil
		}
		if err != nil {
			return nil, core.ClassReject, err
		}
	}
}

// startSession starts a server session on a carrier whose first record is
// QX_TRANSPORT_PARAMETERS.
func (l *Listener) startSession(ctx context.Context, p PipeIO) error {
	cfg := l.cfg.Config.Clone()
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
	}
	cc.Budget = l.budget
	markTaken(p)
	c, err := startCore(ctx, p, cfg, cc, l)
	if err != nil {
		l.st.rejected.Add(1)
		return err
	}
	if l.budget.Pressure() {
		// Refused after its own parameters.
		c.lock()
		c.s.Refuse()
		c.unlock()
		_ = c.Wait()
		l.st.refused.Add(1)
		return errRefused
	}
	l.mu.Lock()
	full := l.draining
	if !full {
		select {
		case l.accept <- c:
			// startCore registered the locator before bootstrap I/O.
			// sessionClosed owns its removal, including handshake failures.
		default:
			full = true
		}
	}
	l.mu.Unlock()
	if full {
		c.closeWith(0, "refused")
		l.st.refused.Add(1)
		return errRefused
	}
	l.st.sessions.Add(1)
	return c.waitPipe(ctx, 0)
}

// join attaches a carrier that joins a session; inband: its first record
// starts with the PIPE_JOIN (already read into its prefix). release gives
// up the carrier's pending slot once the join was verified or refused.
func (l *Listener) join(ctx context.Context, p PipeIO, j *wire.Join, mac []byte, binding []byte, inband bool, release func()) error {
	l.mu.Lock()
	c, closed := l.sessions[string(j.Locator)], l.closed
	l.mu.Unlock()
	if closed {
		closePipeIO(p)
		l.st.refused.Add(1)
		return errRefused
	}
	if c == nil {
		closePipeIO(p)
		l.st.rejected.Add(1)
		return errRejected
	}
	if l.budget.Pressure() {
		// Under pressure only a join that lets a suspended session resume.
		l.mu.Lock()
		susp := slices.Contains(l.suspended, c)
		l.mu.Unlock()
		if !susp {
			closePipeIO(p)
			l.st.refused.Add(1)
			return errRefused
		}
	}
	if j.CarrierBound {
		// A carrier without an exporter is never carrier-bound
		// (MULTIPATH.md 10.5): refuse rather than verify an empty binding.
		st, ok := carrierIsTLS(p)
		if !ok {
			closePipeIO(p)
			l.st.rejected.Add(1)
			return errRejected
		}
		b, err := st.ExportKeyingMaterial(wire.ExporterPipe, nil, 32)
		if err != nil {
			closePipeIO(p)
			l.st.rejected.Add(1)
			return errRejected
		}
		binding = b
	}
	// A join that arrived as carrier early data could be a replay: it is
	// processed only once the carrier's handshake completed.
	if es, ok := p.W.(EarlyServerCarrier); ok && es.UsedEarlyData() {
		timer := timeshim.NewTimer(l.clk, l.cfg.Config.handshakeTimeout())
		defer timer.Stop()
		select {
		case <-es.HandshakeComplete():
		case <-timer.C:
			closePipeIO(p)
			// The TLS handshake timed out: the join is a setup failure
			// and counted nowhere else.
			l.CountSetupFailure()
			return errRejected
		case <-ctx.Done():
		case <-l.done:
		}
		if ctx.Err() != nil || l.isClosed() {
			// The wait was cancelled (the listener closed, the caller's
			// context ended): the handshake never completed, a setup
			// failure as well.
			closePipeIO(p)
			l.CountSetupFailure()
			return errRejected
		}
	}
	id, err := c.acceptJoin(ctx, p, j, mac, binding, inband)
	release()
	if err != nil {
		l.rejectedBy(err)
		return err
	}
	l.st.joins.Add(1)
	return c.waitPipe(ctx, id)
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

// ---- split bootstrap (SPEC.md 6.1.1) ----

func (l *Listener) handlePair(ctx context.Context, p PipeIO, hint JoinHint, src string) error {
	tok, err := base64.RawURLEncoding.DecodeString(hint.Pair)
	if err != nil || len(tok) != 16 {
		closePipeIO(p)
		l.st.rejected.Add(1)
		return errRejected
	}
	var t [16]byte
	copy(t[:], tok)
	hto := l.cfg.Config.handshakeTimeout()
	me := &pairWait{p: p, done: make(chan error, 1), end: make(chan struct{})}
	l.mu.Lock()
	res, partner := l.pairing.Offer(t, hint.Uplink, src, me, core.Time(l.clk.Now().Add(hto).UnixNano()))
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
		case err := <-me.done:
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
		if err := <-me.done; err != nil {
			return err
		}
		<-me.end
		return nil
	case core.PairDone:
	}
	other := partner.(*pairWait)
	up, down := p, other.p
	if !hint.Uplink {
		up, down = other.p, p
	}
	both := PipeIO{R: up.R, W: down.W, Framing: up.Framing, Label: up.Label, Carrier: up.Carrier}
	err = l.startSession(ctx, both)
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
	pre []byte
	r   io.ReadCloser
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
	first []byte
	r     io.ReadCloser
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
	if same := any(p.R) == any(p.W); same {
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
	case carrierConner:
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
	if p.R != nil && any(p.R) != any(p.W) {
		_ = p.R.Close()
	}
}

// tlsSetupFailed reports whether p is a TLS connection whose handshake did
// not complete: the read failed below the carrier.
func tlsSetupFailed(p PipeIO) bool {
	tc, ok := p.R.(*tls.Conn)
	return ok && !tc.ConnectionState().HandshakeComplete
}
