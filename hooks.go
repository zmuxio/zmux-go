package zmux

import (
	"context"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/poolhost"
)

// Tracer receives session events. Its functions
// run serially and in order on a goroutine of the session, never under a
// lock; the session never waits for them: each session has a backlog of
// 256 events, fixed and not configurable, and an event that finds it full
// is dropped and counted (the Tracer observes; an application that must not lose
// events reads NextEvent, whose queue never drops). Established, Dropped
// and Closed take no room of the backlog and are never dropped. Wait and
// Done do not wait for the Tracer (nor for Pick or
// AllowConnectionWindowIncrease), so a hook may call Wait or Done; Closed
// may run after Wait returned.
type Tracer struct {
	// Established: the peer's transport parameters were processed. It is
	// the first call.
	Established func(id SessionID)
	// Event: each occurrence of a pipe or session event, before NextEvent's
	// queue merges it: a JoinAuthFailed has Count 1, and every retired pipe
	// is one EventPipesRetired.
	Event func(id SessionID, ev Event)
	// StreamEvents enables per-frame placement and receive-gap events in Event
	// for multipath sessions. These events share the Tracer's drop accounting.
	StreamEvents bool
	// Dropped: this many of the session's events were lost since the
	// previous call because its backlog was full. It is called where the
	// lost events stood: before the next event the backlog accepted, or
	// before Closed, never after it.
	Dropped func(id SessionID, count uint64)
	// Closed: the session ended. It is the last call: nothing follows it,
	// and what occurs after the session ended is not traced.
	Closed func(id SessionID, err error)
}

// Pick is a custom scheduling policy. It runs outside every
// lock and must not block. One call carries every stream direction that
// waits for a decision; it writes one answer per request into out (preset
// to Wait). An answer names a pipe of pipes by index and how many bytes it
// covers (0: until it is invalidated by a change of the pipe set, a pipe's
// state or eligibility).
type Pick interface {
	Pick(reqs []PickRequest, pipes []PickPipe, out []PickAnswer)
}

// PickRequest is a stream direction waiting for a decision.
type PickRequest struct {
	Stream       StreamID
	Len          uint64 // bytes ready to place
	EligibleTier uint64 // the most-preferred usable tier
	LastPipe     uint64
	HasLastPipe  bool
	Urgency      Urgency
}

// PickPipe is a pipe as a custom policy sees it (values only).
type PickPipe struct {
	PipeID    PipeID
	SRTT      time.Duration
	RTTVar    time.Duration
	Rate      uint64 // delivery rate estimate, bytes per second
	Unsent    uint64 // Q_p: handed to the carrier and not taken yet, plus its own unsent count where reported
	InFlight  uint64
	Priority  uint64
	Weight    uint64
	State     OutboundState
	Writable  bool
	Standby   bool
	Saturated bool
	Probation bool
	Eligible  bool // under tier gating: the built-in policies choose among these
	// ShapingActive: a Shaper is active on the pipe.
	ShapingActive bool
	// ShapingWeight is the weight the pipe's Shaper set; 0: none.
	ShapingWeight uint32
}

// PickAnswer is the answer for one direction: Index is the chosen pipe's
// index into the pipes passed to Pick (not its pipe ID), Bytes the bytes it
// covers (0: until invalidated); Wait holds the direction until the
// session's next input.
type PickAnswer struct {
	Index int
	Bytes uint64
	Wait  bool
}

// SchedulerCustom returns a scheduler that asks p.
func SchedulerCustom(p Pick) Scheduler { return customPolicy{p} }

type customPolicy struct{ p Pick }

func (customPolicy) policy() core.Policy { return core.PolicyCustom }

// traceBacklog is the tracer's backlog per session: the events waiting for
// the Tracer, fixed. The established, dropped and closed calls take none of it.
const traceBacklog = 256

// warnQueueMax bounds the warnings waiting for the Logger per session: a
// warning arriving at a full queue is dropped and counted (LogLinesDropped).
const warnQueueMax = 256

// hooks runs the application's hooks of one session on two goroutines of
// their own, each started when its first hook is needed (an idle session
// without hooks keeps its footprint): one answers the scheduler (Pick,
// AllowConnectionWindowIncrease), the other delivers the Tracer's events
// and the Logger's warnings, so a slow Tracer or Logger never delays a
// decision. Its lock is a leaf.
type hooks struct {
	pick  Pick
	c     *Conn
	tr    *Tracer
	allow func(delta uint64) bool
	log   *slog.Logger
	warns []string // warnings for log, queued under the session lock

	logLost uint64      // warnings dropped at a full queue (ConnectionStats.LogLinesDropped)
	q       []traceItem // from qh: the calls waiting for the Tracer
	qh      int
	nev     int // the events among them: the backlog's fill
	mu      sync.Mutex
	wantWin bool
	wantPk  bool
	decRun  bool // the decision goroutine runs
	trRun   bool // the trace goroutine runs
	estQ    bool // the established call is queued: events may be delivered
	ended   bool // the closed call is queued: nothing follows it
}

// traceItem is one call of the Tracer: established, closed, dropped (lost
// events) or an event.
type traceItem struct {
	err    error
	ev     Event
	lost   uint64
	est    bool
	closed bool
}

// newHooks returns the hooks of c's configuration with the custom policy
// pick (nil: the configured one), or nil when there are none: the one
// constructor, so a hook set late (SetScheduler) keeps the configured
// Tracer and Logger.
func newHooks(c *Conn, pick Pick) *hooks {
	cfg := c.cfg
	h := &hooks{c: c, tr: cfg.Tracer, allow: cfg.AllowConnectionWindowIncrease, log: cfg.Logger, pick: pick}
	if cp, ok := cfg.Scheduler.(customPolicy); ok && pick == nil {
		h.pick = cp.p
	}
	if h.tr == nil && h.pick == nil && h.allow == nil && h.log == nil {
		return nil
	}
	return h
}

// warn queues a warning for Config.Logger (under the session lock; the
// leaf lock only); the hook goroutine logs it.
func (h *hooks) warn(msg string) {
	if h == nil || h.log == nil {
		return
	}
	h.mu.Lock()
	if len(h.warns) < warnQueueMax {
		h.warns = append(h.warns, msg)
	} else {
		h.logLost++
	}
	h.mu.Unlock()
}

// logLinesDropped is the count of warnings dropped at a full queue; h may be nil.
func (h *hooks) logLinesDropped() uint64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.logLost
}

// logger is h's Config.Logger; h may be nil.
func (h *hooks) logger() *slog.Logger {
	if h == nil {
		return nil
	}
	return h.log
}

// trace queues a tracer call (under the session lock; the leaf lock only).
// established goes first and closed last, neither takes room of the
// backlog, and nothing is queued after closed. An event that finds the
// backlog full is lost: the count stands where it would have been, and is
// delivered before the next event the backlog accepts or before closed.
func (h *hooks) trace(it traceItem) {
	if h == nil || h.tr == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case h.ended:
		return
	case it.closed:
		h.ended = true
		h.q = append(h.q, it)
	case it.est:
		h.estQ = true
		h.q = slices.Insert(h.q, h.qh, it)
	case h.nev >= traceBacklog:
		if n := len(h.q); n > h.qh && h.q[n-1].lost > 0 {
			h.q[n-1].lost++
		} else {
			h.q = append(h.q, traceItem{lost: 1})
		}
	default:
		h.nev++
		h.q = append(h.q, it)
	}
}

// next takes the next call for the Tracer, if one may be delivered: events
// wait for established, which is the first call.
func (h *hooks) next() (traceItem, bool) {
	if h.qh >= len(h.q) || !h.estQ && !h.ended {
		return traceItem{}, false
	}
	it := h.q[h.qh]
	h.q[h.qh] = traceItem{}
	h.qh++
	if h.qh == len(h.q) {
		h.q, h.qh = h.q[:0], 0
	} else if h.qh >= traceBacklog {
		h.q = h.q[:copy(h.q, h.q[h.qh:])]
		h.qh = 0
	}
	if !it.est && !it.closed && it.lost == 0 {
		h.nev--
	}
	return it, true
}

// note records the work the critical section left for the hooks.
func (h *hooks) note(win, pick bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.wantWin = h.wantWin || win && h.allow != nil
	h.wantPk = h.wantPk || pick && h.pick != nil
	h.mu.Unlock()
}

// start runs the hook goroutines that have work (outside the lock). They
// are not side goroutines: Wait never waits for host hooks, which may call
// Wait themselves. A running one finds new work in its next check (h.mu).
func (h *hooks) start() {
	if h == nil {
		return
	}
	h.mu.Lock()
	dec := (h.wantWin || h.wantPk) && !h.decRun
	tr := (len(h.warns) > 0 || h.qh < len(h.q) && (h.estQ || h.ended)) && !h.trRun
	h.decRun = h.decRun || dec
	h.trRun = h.trRun || tr
	h.mu.Unlock()
	if dec {
		labels.Go(h.c.lctx, "hooks-decide", h.runDecide)
	}
	if tr {
		labels.Go(h.c.lctx, "hooks-trace", h.runTrace)
	}
}

// runDecide answers the scheduler's questions until none is pending.
func (h *hooks) runDecide() {
	for {
		h.mu.Lock()
		win, pk := h.wantWin, h.wantPk
		h.wantWin, h.wantPk = false, false
		if !win && !pk {
			h.decRun = false
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		h.decide(win, pk)
	}
}

// runTrace delivers the warnings and the tracer's calls until none is
// queued.
func (h *hooks) runTrace() {
	for {
		h.mu.Lock()
		warns := h.warns
		h.warns = nil
		it, ok := h.next()
		if !ok && len(warns) == 0 {
			h.trRun = false
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		for _, w := range warns {
			safeLog(h.log, "Logger", func() { h.log.Warn(w) })
		}
		if ok {
			h.deliver(it)
		}
	}
}

// decide answers a pending window proposal and Pick request.
func (h *hooks) decide(win, pk bool) {
	if win {
		h.window()
	}
	if pk {
		h.runPick()
	}
}

func (h *hooks) deliver(it traceItem) {
	t := h.tr
	switch {
	case it.est && t.Established != nil:
		safeLog(h.log, "Tracer.Established", func() { t.Established(h.c.id) })
	case it.closed && t.Closed != nil:
		safeLog(h.log, "Tracer.Closed", func() { t.Closed(h.c.id, it.err) })
	case it.lost > 0 && t.Dropped != nil:
		safeLog(h.log, "Tracer.Dropped", func() { t.Dropped(h.c.id, it.lost) })
	case !it.est && !it.closed && it.lost == 0 && t.Event != nil:
		safeLog(h.log, "Tracer.Event", func() { t.Event(h.c.id, it.ev) })
	}
}

// window asks allow_connection_window_increase about a pending growth.
func (h *hooks) window() {
	c := h.c
	var w, delta uint64
	var ok bool
	c.locked(func() { w, delta, ok = c.s.WindowProposal() })
	if !ok {
		return
	}
	allow := false
	safeLog(h.log, "Config.AllowConnectionWindowIncrease", func() { allow = h.allow(delta) })
	c.locked(func() {
		c.s.ApplyWindowProposal(w, allow)
	})
}

// runPick asks the custom policy for the directions that wait.
func (h *hooks) runPick() {
	c := h.c
	var req core.PickRequest
	var ok bool
	c.locked(func() { ok = c.s.TakePick(&req) })
	if !ok {
		return
	}
	reqs := make([]PickRequest, len(req.Frames))
	for i, f := range req.Frames {
		reqs[i] = PickRequest{Stream: StreamID(f.Stream), Urgency: Urgency(f.Urgency), Len: f.Len, LastPipe: f.LastPipe,
			HasLastPipe: f.HasLastPipe, EligibleTier: f.EligibleTier}
	}
	pipes := make([]PickPipe, len(req.Pipes))
	for i, p := range req.Pipes {
		pipes[i] = PickPipe{PipeID: PipeID(p.Pipe), State: OutboundState(p.State), SRTT: time.Duration(p.SRTT),
			RTTVar: time.Duration(p.RTTVar), Rate: rateBytes(p.Rate), Unsent: p.Unsent, InFlight: p.InFlight,
			Writable: p.Writable, Priority: p.Priority, Standby: p.Standby, Saturated: p.Saturated,
			Probation: p.Probation, Eligible: p.Eligible, Weight: p.Weight,
			ShapingActive: p.Shaping, ShapingWeight: p.ShapingW}
	}
	out := make([]PickAnswer, len(reqs))
	for i := range out {
		out[i].Wait = true
	}
	failed := !safeLog(h.log, "Pick", func() { h.pick.Pick(reqs, pipes, out) })
	ans := make([]core.PickAnswer, len(out))
	for i, a := range out {
		ans[i] = core.PickAnswer{Wait: a.Wait, Index: a.Index, Bytes: a.Bytes}
	}
	c.locked(func() {
		c.s.AnswerPick(&req, ans, failed)
	})
}

// safeLog runs the host callback f, containing a panic (ok false), which
// it logs as the panic of what to l (nil: not logged), outside every lock.
func safeLog(l *slog.Logger, what string, f func()) (ok bool) {
	return poolhost.Safe(l, what, f)
}

// satDouble doubles a backoff, saturating instead of wrapping negative for
// a very large configured maximum.
func satDouble(d time.Duration) time.Duration {
	if d > math.MaxInt64/2 {
		return math.MaxInt64
	}
	return 2 * d
}

// hostCall runs a host callback that fails what it serves when it panics:
// the panic is contained (and logged) and comes back as an error naming
// the callback, never ending the process from an internal goroutine.
func hostCall(l *slog.Logger, what string, f func() error) error {
	return poolhost.Call(l, what, f)
}

// ---- redial ----

// redialer runs at most one outage attempt and one replacement attempt.
type redialer struct {
	outage      redialWorker
	replacement redialWorker
	replace     *redialRequest
	cancel      context.CancelFunc // the current replacement attempt
	mu          sync.Mutex
}

type redialWorker struct {
	wake           chan struct{}
	state          redialState // owned by its running loop
	running, again bool
}

func (r *redialer) qualityChanged(id PipeID) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.replacement.running || r.replace == nil || r.replace.id != id {
		return
	}
	select {
	case r.replacement.wake <- struct{}{}:
	default:
	}
}

type redialRequest struct {
	after   time.Time
	id      PipeID
	episode core.Time
}

type redialDemand struct {
	replace *redialRequest
	pipe    *Pipe
	need    Direction
}

type replacingPipeKey struct{}

// ReplacingPipe returns the old pipe named by a host-requested replacement
// in a Config.Redial callback's context.
func ReplacingPipe(ctx context.Context) (PipeID, bool) {
	id, ok := ctx.Value(replacingPipeKey{}).(PipeID)
	return id, ok
}

// RequestPipeReplacement asks the configured Redial hook to try an advised
// replacement. The host calls this after reviewing EventPipePathQuality.
// Calls for the current episode coalesce. An outstanding request can finish
// or expire before the host submits a request for another pipe.
func (c *Conn) RequestPipeReplacement(id PipeID) bool {
	if c == nil || c.cfg.Redial == nil || c.closingNow() {
		return false
	}
	var episode core.Time
	var delay core.Duration
	var ok bool
	c.locked(func() {
		c.s.SetNow(c.clk.now())
		episode, delay, ok = c.s.ReplacementAdvice(uint64(id))
	})
	if !ok {
		return false
	}
	r := c.redial
	r.mu.Lock()
	if r.replace != nil {
		same := r.replace.id == id && r.replace.episode == episode
		r.mu.Unlock()
		return same
	}
	r.replace = &redialRequest{id: id, episode: episode, after: c.clk.source().Now().Add(time.Duration(delay))}
	r.mu.Unlock()
	c.startRedial(true)
	return true
}

type redialState struct {
	view     PoolView
	lastDial time.Time
	dec      PoolDecision
	failures int
	backoff  time.Duration // the current backoff; 0: not started
	wait     time.Duration // the wait after lastDial, jittered
}

// redialNeeded tests coverage before considering an advised replacement.
// An unconfirmed replacement does not cover a direction lost during its join.
func (c *Conn) redialNeeded(replacement bool) (redialDemand, bool) {
	r := c.redial
	r.mu.Lock()
	req := r.replace
	r.mu.Unlock()
	var out, in, pending bool
	var pipe *Pipe
	c.locked(func() {
		out, in = c.s.LiveDirections()
		if out && in {
			for id, pd := range c.pipes {
				if o := pd.end.join; o != nil && !o.settled && o.parent != nil {
					if _, ok := ReplacingPipe(o.parent); ok {
						out, in = c.s.LiveDirectionsWithout(id)
						break
					}
				}
			}
		}
		pending = c.s.JoinPendingNow()
		if !replacement || !out || !in || pending || req == nil {
			return
		}
		c.s.SetNow(c.clk.now())
		episode, _, ok := c.s.ReplacementAdvice(uint64(req.id))
		if pd := c.pipes[uint64(req.id)]; ok && episode == req.episode && pd != nil {
			pipe = &Pipe{c: c, id: uint64(req.id), end: pd.end}
		}
	})
	if pending {
		return redialDemand{}, false
	}
	if req != nil && (!out || !in || replacement && pipe == nil) {
		c.clearRedialRequest(req)
	}
	if replacement {
		if pipe == nil {
			return redialDemand{}, false
		}
		return redialDemand{need: pipe.Options().Direction, replace: req, pipe: pipe}, true
	}
	switch {
	case !out && !in:
		return redialDemand{need: DirectionBidirectional}, true
	case !out:
		return redialDemand{need: DirectionSendOnly}, true
	case !in:
		return redialDemand{need: DirectionReceiveOnly}, true
	default:
		return redialDemand{}, false
	}
}

func (c *Conn) clearRedialRequest(req *redialRequest) {
	r := c.redial
	r.mu.Lock()
	var cancel context.CancelFunc
	if r.replace == req {
		r.replace = nil
		cancel, r.cancel = r.cancel, nil
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *redialer) replacementContext(ctx context.Context, req *redialRequest) (context.Context, context.CancelFunc, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.replace != req {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(context.WithValue(ctx, replacingPipeKey{}, req.id))
	r.cancel = cancel
	return ctx, cancel, true
}

func (c *Conn) maybeRedial() {
	c.startRedial(false)
	c.startRedial(true)
}

func (c *Conn) startRedial(replacement bool) {
	if c.cfg.Redial == nil || c.closingNow() {
		return
	}
	r := c.redial
	r.mu.Lock()
	w := &r.outage
	if replacement {
		if r.replace == nil {
			r.mu.Unlock()
			return
		}
		w = &r.replacement
	}
	if w.running {
		w.again = true
		select {
		case w.wake <- struct{}{}:
		default:
		}
		r.mu.Unlock()
		return
	}
	w.running = true
	w.wake = make(chan struct{}, 1)
	r.mu.Unlock()
	if !c.goSide(c.lctx, "redial", func() { c.redialLoop(w, replacement) }) {
		r.mu.Lock()
		w.running, w.again = false, false
		r.mu.Unlock()
	}
}

func (c *Conn) closingNow() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Conn) redialLoop(worker *redialWorker, replacement bool) {
	r := c.redial
	clk := c.clk.source()
	defer func() {
		r.mu.Lock()
		again := worker.again
		worker.running, worker.again = false, false
		r.mu.Unlock()
		if again {
			c.startRedial(replacement)
		}
	}()
	bo := c.cfg.redialBackoff()
	lo, hi := bo.Min, bo.Max
	// delay is the wait after the previous attempt: the policy's
	// DialAfter, else the backoff.
	delay := func(st *redialState) time.Duration {
		d := st.wait
		pol := c.cfg.RedialPolicy
		if pol == nil {
			return d
		}
		v := &st.view
		v.Failures, v.SinceLastDial, v.Members = st.failures, sinceDial(clk.Now(), st.lastDial), v.Members[:0]
		for _, ps := range c.pipeStatsAll(false) {
			_, usable := pipeState(&ps)
			v.Members = append(v.Members, PoolMember{PipeID: ps.PipeID, Age: ps.AttachedFor, Idle: ps.SinceLastTraffic,
				Pipes: 1, BytesSent: ps.BytesSent, BytesReceived: ps.BytesReceived, Usable: usable})
		}
		if err := decide(pol, v, &st.dec); err != nil {
			st.dec = PoolDecision{} // rejected
			poolhost.ReportRejected(c.cfg.Logger, c.cfg.DecisionRejected, err)
		}
		if st.dec.DialAfter > 0 {
			d = min(st.dec.DialAfter, hi)
		}
		return d
	}
	for !c.closingNow() {
		demand, needed := c.redialNeeded(replacement)
		if !needed {
			return
		}
		st := &worker.state
		if st.backoff == 0 {
			st.backoff, st.wait = lo, bo.wait(lo)
		}
		w := c.refusalWait()
		if !st.lastDial.IsZero() {
			w = max(w, timeshim.Until(clk, st.lastDial.Add(delay(st))))
		} else if c.cfg.RedialPolicy != nil {
			delay(st)
		}
		if demand.replace != nil {
			w = max(w, timeshim.Until(clk, demand.replace.after))
		}
		if w > 0 {
			t := timeshim.NewTimer(clk, w)
			select {
			case <-t.C:
			case <-worker.wake:
				t.Stop()
				continue
			case <-c.done:
				t.Stop()
				return
			}
		}
		// Policy calls and the backoff can outlive the measured demand.
		current, needed := c.redialNeeded(replacement)
		if !needed || current.need != demand.need || current.replace != demand.replace {
			continue
		}
		st.lastDial = clk.Now()
		if c.redialAttempt(demand, st) {
			st.failures, st.backoff, st.wait = 0, lo, bo.wait(lo)
			continue
		}
		if st.failures > 0 {
			st.backoff = min(satDouble(st.backoff), hi)
		}
		st.failures++
		st.wait = bo.wait(st.backoff)
	}
}

// redialAttempt covers one dial and its attachment, outside the redial lock.
func (c *Conn) redialAttempt(demand redialDemand, st *redialState) bool {
	replacement := demand.replace != nil
	ctx := c.ctx
	if replacement {
		replacementCtx, cancel, ok := c.redial.replacementContext(ctx, demand.replace)
		if !ok {
			return false
		}
		defer cancel()
		ctx = replacementCtx
	}
	var p PipeIO
	var err error
	ok := safeLog(c.cfg.Logger, "Config.Redial", func() { p, err = c.cfg.Redial(ctx, demand.need) })
	if !ok || err != nil {
		return false
	}
	current, needed := c.redialNeeded(replacement)
	if ctx.Err() != nil || c.closingNow() || !needed || current.replace != demand.replace || current.need != demand.need {
		closePipe(p)
		return false
	}
	opts := PipeOptions{Direction: demand.need, ReturnOnReply: true}
	if demand.pipe != nil {
		opts = demand.pipe.Options()
		opts.ReturnOnReply = false
		if mode, set := opts.RecordMode.get(); set && mode == RecordUnmanaged {
			// The bootstrap mode is report-only. Let the configured join
			// policy select the replacement carrier's protection.
			opts.RecordMode = Change[RecordMode]{}
		}
	}
	added, err := c.AddPipe(ctx, p, st.dec.probeOptions(opts))
	if err != nil {
		return false
	}
	if replacement {
		c.finishReplacement(demand, added)
	}
	return true
}

// finishReplacement checks the confirmed join and retires one attachment
// through the ordinary removal path, with the advice still current under the lock.
func (c *Conn) finishReplacement(demand redialDemand, added *Pipe) {
	c.locked(func() {
		c.s.SetNow(c.clk.now())
		out, in, ok := c.s.PipeState(added.id)
		if !ok || (demand.need != DirectionReceiveOnly && out != core.OutActive) ||
			(demand.need != DirectionSendOnly && in != core.InOpen) {
			return
		}
		id := uint64(demand.replace.id)
		if c.s.ReplacementEpisode(id, demand.replace.episode) && !c.s.RemovesLast(id) {
			_ = c.s.RemovePipe(id, true)
			return
		}
		// Keep a confirmed replacement if removing it could give up the
		// only confirmed copy of a direction.
		if old, ok := c.s.PipeInfo(id); ok && old.PeerAttached &&
			(demand.need == DirectionReceiveOnly || old.OutState == core.OutActive || old.OutState == core.OutSuspect) &&
			(demand.need == DirectionSendOnly || old.InState == core.InOpen) {
			_ = c.s.RemovePipe(added.id, true)
		}
	})
	c.clearRedialRequest(demand.replace)
}

// refusalWait is what remains of the join refusal backoff, which a refused
// join started.
func (c *Conn) refusalWait() time.Duration {
	c.lock()
	defer c.unlock()
	return time.Duration(c.s.JoinBackoffLeft(c.clk.now()))
}
