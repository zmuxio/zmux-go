package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// ListenerConfig configures a Listener. The zero value of
// every field selects its default.
type ListenerConfig struct {
	// Config is the configuration of the sessions it accepts.
	Config *Config
	// MaxSuspended bounds the suspended sessions it holds (1024); the
	// longest-suspended are closed with ZMUX_RESUME_TIMEOUT.
	MaxSuspended int
	// MaxPendingPerSource bounds the carriers of one source that have not
	// delivered their first record (8).
	MaxPendingPerSource int
	// FirstRecordTimeout closes a carrier whose first record does not
	// arrive in time (2 s).
	FirstRecordTimeout time.Duration
	// Backlog is the number of sessions waiting for Accept (128); a new
	// session beyond it is refused.
	Backlog int
	// InstanceID is encoded at the start of every session locator, for
	// load-balancer stickiness.
	InstanceID []byte
	// MemoryBudget bounds the sum over the listener's sessions of buffered
	// receive data, retained data and session window growth (1 GiB;
	// negative: unlimited). It replaces the sessions' own
	// Config.MemoryBudget. Above it sessions grant no new credit, refuse
	// new peer streams, new sessions and pipes are refused, and suspended
	// sessions are closed, the largest first; normal operation resumes
	// below three quarters of it.
	MemoryBudget int64
	// OnMemoryPressure is called when pressure starts (used >= budget) and
	// when it ends, outside every lock, one call at a time; it may close
	// sessions.
	OnMemoryPressure func(used, budget uint64)
}

// JoinHint is what a carrier's request said out of band (HTTP headers).
type JoinHint struct {
	// Join is the Zmux-Pipe-Join header: an out-of-band PIPE_JOIN.
	Join string
	// Pair is the Zmux-Pipe-Pair header: the pairing token of a split
	// bootstrap (SPEC.md 6.1.1); Uplink says which half this carrier is.
	Pair   string
	Uplink bool
	// Source identifies the client for the per-source limit (default: the
	// carrier's remote address).
	Source string
}

// Listener is the server side of zmux: it accepts sessions and the pipes
// that join them (MULTIPATH.md 5.4, 5.5), so it is the way to serve
// clients that use multipath, resume or split carriers. Serve feeds it the
// connections of a net.Listener (TCP, or TLS with tls.NewListener);
// HandlePipe feeds it any other carrier, and ServeHTTP HTTP carriers.
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

	accept chan *Conn
	done   chan struct{}
	budget *core.Budget
}

type pairWait struct {
	p    PipeIO
	done chan error // the session's result for the waiting half
	end  chan struct{}
}

// NewListener returns a listener; feed it carriers with Serve, HandlePipe
// or ServeHTTP. cfg may be nil.
func NewListener(cfg *ListenerConfig) *Listener {
	l := &Listener{live: map[*Conn]struct{}{}, sessions: map[string]*Conn{}, pairs: map[[16]byte]*pairWait{}, done: make(chan struct{})}
	if cfg != nil {
		l.cfg = *cfg
	}
	b := l.cfg.Backlog
	if b <= 0 {
		b = 128
	}
	l.accept = make(chan *Conn, b)
	l.pending.MaxPerSource = l.cfg.MaxPendingPerSource
	var limit uint64 = 1 << 30
	switch {
	case l.cfg.MemoryBudget < 0:
		limit = 0
	case l.cfg.MemoryBudget > 0:
		limit = uint64(l.cfg.MemoryBudget)
	}
	l.budget = core.NewBudget(limit)
	if limit > 0 {
		go l.watchBudget()
	}
	return l
}

// MemoryUsed reports the sum charged to the listener's memory budget.
func (l *Listener) MemoryUsed() uint64 { return l.budget.Used() }

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
			h(l.budget.Used(), l.budget.Limit())
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
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close stops accepting: Accept and Serve return net.ErrClosed, new
// carriers are refused and the memory-pressure watcher stops. It does not
// block, and sessions already started continue; Shutdown ends them too.
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.done)
	}
	return nil
}

// Shutdown closes the listener and then shuts down every session it
// started, accepted or not, with Conn.Shutdown(ctx, code, msg) in
// parallel: each sends GOAWAY, lets its open streams finish and closes.
// It returns when every session has ended, or with ctx's error once ctx
// ends (the remaining sessions are then closed at once).
func (l *Listener) Shutdown(ctx context.Context, code ApplicationErrorCode, msg string) error {
	_ = l.Close()
	l.mu.Lock()
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
	return err
}

// Serve accepts connections from ln and hands each one to HandlePipe, in a
// goroutine of its own, as a carrier that starts or joins a session; the
// sessions it starts are returned by Accept. For TLS, pass a listener made
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

// HandlePipe classifies a new carrier by its first record (or its hint)
// and starts a session on it, attaches it to a session as a pipe, or
// closes it. It blocks for the lifetime of the pipe, so that an HTTP
// handler stays alive; when ctx ends the pipe is aborted.
func (l *Listener) HandlePipe(ctx context.Context, p PipeIO, hint JoinHint) error {
	src := hint.Source
	if src == "" {
		if nc, ok := p.R.(net.Conn); ok {
			if a, _, err := net.SplitHostPort(nc.RemoteAddr().String()); err == nil {
				src = a
			}
		}
	}
	if hint.Pair != "" {
		return l.handlePair(ctx, p, hint, src)
	}
	if hint.Join != "" {
		f, err := base64.RawURLEncoding.DecodeString(hint.Join)
		if err != nil {
			closePipeIO(p)
			return errRejected
		}
		j, mac, _, ok := core.ParseJoin(f)
		if !ok {
			closePipeIO(p)
			return errRejected
		}
		return l.join(ctx, p, j, mac, nil, false)
	}
	l.mu.Lock()
	ok := !l.closed && l.pending.Admit(src)
	l.mu.Unlock()
	if !ok {
		closePipeIO(p)
		return errRejected
	}
	first, cls, err := l.readFirst(p)
	l.mu.Lock()
	l.pending.Release(src)
	l.mu.Unlock()
	if err != nil {
		closePipeIO(p)
		return err
	}
	switch cls {
	case core.ClassSession:
		return l.startSession(ctx, withPrefix(p, first))
	case core.ClassJoin:
		body := first
		if p.Framing == FramingByteStream {
			_, n := wire.ReadVarint(first)
			body = first[n:]
		}
		j, mac, _, ok := core.ParseJoin(body)
		if !ok {
			closePipeIO(p)
			return errRejected
		}
		return l.join(ctx, withPrefix(p, first), j, mac, nil, true)
	case core.ClassLane:
		return handleLane(ctx, withPrefix(p, first), first)
	case core.ClassMore, core.ClassReject:
	}
	closePipeIO(p)
	return errRejected
}

// readFirst reads until the first record is complete (or rejected),
// within the first-record timeout.
func (l *Listener) readFirst(p PipeIO) ([]byte, core.Class, error) {
	t := l.cfg.FirstRecordTimeout
	if t <= 0 {
		t = 2 * time.Second
	}
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
	timer := time.NewTimer(t)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.b, r.cls, r.err
	case <-timer.C:
		closePipeIO(p) // interrupts the read
		<-ch
		return nil, core.ClassReject, fmt.Errorf("zmux: first record timeout: %w", errRejected)
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
		framed := wire.AppendVarint(nil, uint64(len(m)))
		cls, _ := core.Classify(append(framed, m...), 65535)
		return m, cls, nil
	}
	var b []byte
	var tmp [512]byte
	for {
		n, err := p.R.Read(tmp[:])
		b = append(b, tmp[:n]...)
		cls, rl := core.Classify(b, 65535)
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
		return err
	}
	loc := make([]byte, len(l.cfg.InstanceID), len(l.cfg.InstanceID)+16)
	copy(loc, l.cfg.InstanceID)
	var r [16]byte
	_, _ = rand.Read(r[:])
	loc = append(loc, r[:]...)
	if cc.Local.Has&wire.HasMultipath != 0 {
		cc.Local.Locator = loc
	}
	cc.Budget = l.budget
	c, err := startCore(ctx, p, cfg, cc, l)
	if err != nil {
		return err
	}
	if l.budget.Pressure() {
		// Refused after its own parameters.
		c.lock()
		c.s.Refuse()
		c.unlock()
		_ = c.Wait()
		return errRejected
	}
	l.mu.Lock()
	full := l.closed
	if !full {
		select {
		case l.accept <- c:
			// sessionClosed runs after done is closed: a session that
			// already ended is not recorded.
			select {
			case <-c.done:
			default:
				l.live[c] = struct{}{}
				if len(cc.Local.Locator) > 0 {
					l.sessions[string(cc.Local.Locator)] = c
				}
			}
		default:
			full = true
		}
	}
	l.mu.Unlock()
	if full {
		c.closeWith(0, "refused")
		return errRejected
	}
	return c.waitPipe(ctx, 0)
}

// join attaches a carrier that joins a session; inband: its first record
// starts with the PIPE_JOIN (already read into its prefix).
func (l *Listener) join(ctx context.Context, p PipeIO, j *wire.Join, mac []byte, binding []byte, inband bool) error {
	l.mu.Lock()
	c := l.sessions[string(j.Locator)]
	l.mu.Unlock()
	if c == nil {
		closePipeIO(p)
		return errRejected
	}
	if l.budget.Pressure() {
		// Under pressure only a join that lets a suspended session resume.
		l.mu.Lock()
		susp := slices.Contains(l.suspended, c)
		l.mu.Unlock()
		if !susp {
			closePipeIO(p)
			return errRejected
		}
	}
	if j.CarrierBound {
		// A carrier without an exporter is never carrier-bound
		// (MULTIPATH.md 10.5): refuse rather than verify an empty binding.
		st, ok := carrierIsTLS(p)
		if !ok {
			closePipeIO(p)
			return errRejected
		}
		b, err := st.ExportKeyingMaterial(wire.ExporterPipe, nil, 32)
		if err != nil {
			closePipeIO(p)
			return errRejected
		}
		binding = b
	}
	// A join that arrived as carrier early data could be a replay: it is
	// processed only once the carrier's handshake completed.
	if es, ok := p.W.(EarlyServerCarrier); ok && es.UsedEarlyData() {
		hto := 10 * time.Second
		if cfg := l.cfg.Config; cfg != nil && cfg.HandshakeTimeout > 0 {
			hto = cfg.HandshakeTimeout
		}
		timer := time.NewTimer(hto)
		defer timer.Stop()
		select {
		case <-es.HandshakeComplete():
		case <-timer.C:
			closePipeIO(p)
			return errRejected
		case <-ctx.Done():
			closePipeIO(p)
			return errRejected
		}
	}
	id, err := c.acceptJoin(ctx, p, j, mac, binding, inband)
	if err != nil {
		return err
	}
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
	limit := l.cfg.MaxSuspended
	if limit <= 0 {
		limit = 1024
	}
	var over []*Conn
	for len(l.suspended) > limit {
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
		return errRejected
	}
	var t [16]byte
	copy(t[:], tok)
	hto := 10 * time.Second
	if cfg := l.cfg.Config; cfg != nil && cfg.HandshakeTimeout > 0 {
		hto = cfg.HandshakeTimeout
	}
	me := &pairWait{p: p, done: make(chan error, 1), end: make(chan struct{})}
	l.mu.Lock()
	res, partner := l.pairing.Offer(t, hint.Uplink, src, me, core.Time(time.Now().Add(hto).UnixNano()))
	l.mu.Unlock()
	switch res {
	case core.PairReject:
		closePipeIO(p)
		return errRejected
	case core.PairWait:
		timer := time.NewTimer(hto)
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
		}
		l.mu.Lock()
		mine := l.pairing.Withdraw(t, me)
		l.mu.Unlock()
		if mine {
			closePipeIO(p)
			return errRejected
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
	both := PipeIO{R: up.R, W: down.W, Framing: up.Framing, Label: up.Label}
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
