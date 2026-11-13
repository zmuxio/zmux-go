package zmux

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// Tracer receives session events. Its functions
// run serially and in order on a goroutine of the session, never under a
// lock; the session never waits for them: a bounded queue holds events,
// and on overflow the events other than Closed are dropped and counted.
type Tracer struct {
	// Established: the peer's transport parameters were processed.
	Established func()
	// Event: a pipe or session event (as NextEvent reports them).
	Event func(Event)
	// Dropped: events lost to overflow since the previous call.
	Dropped func(count uint64)
	// Closed: the session ended; nothing follows.
	Closed func(err error)
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
	Rate      float64 // delivery rate estimate, bytes per second
	Unsent    uint64  // Q_p: handed to the carrier and not taken yet, plus its own unsent count where reported
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

// hookQueueMax bounds the tracer queue.
const hookQueueMax = 256

// hooks runs the application's hooks of one session on a goroutine of its
// own, started when the first one is needed (an idle session without hooks
// keeps its footprint). Its lock is a leaf.
type hooks struct {
	c     *Conn
	tr    *Tracer
	pick  Pick
	allow func(delta uint64) bool
	log   *slog.Logger
	warns []string // warnings for log, queued under the session lock

	mu      sync.Mutex
	q       []traceItem
	dropped uint64
	wantWin bool
	wantPk  bool
	running bool
	kick    chan struct{}
}

type traceItem struct {
	ev     Event
	est    bool
	closed bool
	err    error
}

func newHooks(c *Conn) *hooks {
	cfg := c.cfg
	h := &hooks{c: c, tr: cfg.Tracer, allow: cfg.AllowConnectionWindowIncrease, log: cfg.Logger, kick: make(chan struct{}, 1)}
	if cp, ok := cfg.Scheduler.(customPolicy); ok {
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
	if len(h.warns) < hookQueueMax {
		h.warns = append(h.warns, msg)
	}
	h.mu.Unlock()
}

// logger is h's Config.Logger; h may be nil.
func (h *hooks) logger() *slog.Logger {
	if h == nil {
		return nil
	}
	return h.log
}

// trace queues a tracer item (under the session lock; the leaf lock only).
func (h *hooks) trace(it traceItem) {
	if h == nil || h.tr == nil {
		return
	}
	h.mu.Lock()
	if len(h.q) >= hookQueueMax && !it.closed {
		h.dropped++
	} else {
		h.q = append(h.q, it)
	}
	h.mu.Unlock()
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

// start runs the hook goroutine when there is work (outside the lock).
func (h *hooks) start() {
	if h == nil {
		return
	}
	h.mu.Lock()
	work := len(h.q) > 0 || h.wantWin || h.wantPk || h.dropped > 0 || len(h.warns) > 0
	run := work && !h.running
	if run {
		h.running = true
	}
	h.mu.Unlock()
	if run {
		labels.Go(h.c.lctx, "hooks", func() { h.run() })
	} else if work {
		select {
		case h.kick <- struct{}{}:
		default:
		}
	}
}

// run serves the hooks until no work is left.
func (h *hooks) run() {
	for {
		h.mu.Lock()
		q := h.q
		h.q = nil
		dropped := h.dropped
		h.dropped = 0
		win, pk := h.wantWin, h.wantPk
		h.wantWin, h.wantPk = false, false
		warns := h.warns
		h.warns = nil
		if len(q) == 0 && dropped == 0 && !win && !pk && len(warns) == 0 {
			h.running = false
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		for _, w := range warns {
			h.log.Warn(w)
		}
		for _, it := range q {
			h.deliver(it)
		}
		if dropped > 0 && h.tr.Dropped != nil {
			safeLog(h.log, "Tracer.Dropped", func() { h.tr.Dropped(dropped) })
		}
		if win {
			h.window()
		}
		if pk {
			h.runPick()
		}
	}
}

func (h *hooks) deliver(it traceItem) {
	t := h.tr
	switch {
	case it.est && t.Established != nil:
		safeLog(h.log, "Tracer.Established", t.Established)
	case it.closed && t.Closed != nil:
		safeLog(h.log, "Tracer.Closed", func() { t.Closed(it.err) })
	case !it.est && !it.closed && t.Event != nil:
		safeLog(h.log, "Tracer.Event", func() { t.Event(it.ev) })
	}
}

// window asks allow_connection_window_increase about a pending growth.
func (h *hooks) window() {
	c := h.c
	c.lock()
	w, delta, ok := c.s.WindowProposal()
	c.unlock()
	if !ok {
		return
	}
	allow := false
	safeLog(h.log, "Config.AllowConnectionWindowIncrease", func() { allow = h.allow(delta) })
	c.lock()
	c.s.ApplyWindowProposal(w, allow)
	c.unlock()
}

// runPick asks the custom policy for the directions that wait.
func (h *hooks) runPick() {
	c := h.c
	var req core.PickRequest
	c.lock()
	ok := c.s.TakePick(&req)
	c.unlock()
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
			RTTVar: time.Duration(p.RTTVar), Rate: p.Rate, Unsent: p.Unsent, InFlight: p.InFlight,
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
	c.lock()
	c.s.AnswerPick(&req, ans, failed)
	c.unlock()
}

// safeLog runs the host callback f, containing a panic (ok false), which
// it logs as the panic of what to l (nil: not logged), outside every lock.
func safeLog(l *slog.Logger, what string, f func()) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			if l != nil {
				l.Warn("zmux: a host callback panicked; the panic was contained", "callback", what, "panic", fmt.Sprint(r))
			}
		}
	}()
	f()
	return true
}

// ---- redial ----

// redialer re-establishes pipes with the Redial hook while the session
// lacks a direction: at most one call in flight per direction, cancelled
// when the session ends; a carrier returned after the end is closed. Each
// direction keeps its failure count, last attempt and backoff across the
// losses of the direction, so that a wait counts from the previous
// attempt.
type redialer struct {
	mu       sync.Mutex
	inFlight [3]bool
	state    [3]redialState
}

type redialState struct {
	failures int
	lastDial time.Time
	backoff  time.Duration // the current backoff; 0: not started
	wait     time.Duration // the wait after lastDial, jittered
	view     PoolView
	dec      PoolDecision
}

// maybeRedial starts redials for the directions the session lacks.
func (c *Conn) maybeRedial() {
	if c.cfg.Redial == nil || c.closingNow() {
		return
	}
	c.lock()
	out, in := c.s.LiveDirections()
	pending := c.s.JoinPendingNow() // EvJoinKeys asks again
	c.unlock()
	if pending {
		return
	}
	var need Direction
	switch {
	case out && in:
		return
	case !out && !in:
		need = DirectionBidirectional
	case !out:
		need = DirectionSendOnly
	default:
		need = DirectionReceiveOnly
	}
	r := &c.redial
	r.mu.Lock()
	if r.inFlight[need] {
		r.mu.Unlock()
		return
	}
	r.inFlight[need] = true
	r.mu.Unlock()
	labels.Go(c.lctx, "redial", func() { c.redialLoop(need) })
}

func (c *Conn) closingNow() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Conn) redialLoop(need Direction) {
	r := &c.redial
	clk := c.clk.source()
	defer func() {
		r.mu.Lock()
		r.inFlight[need] = false
		r.mu.Unlock()
	}()
	// Only this loop touches st while inFlight[need] holds.
	r.mu.Lock()
	st := &r.state[need]
	r.mu.Unlock()
	lo, hi := c.cfg.redialBackoff()
	if st.backoff == 0 {
		st.backoff = lo
		st.wait = backoffWait(lo)
	}
	// delay is the wait after the previous attempt: the policy's
	// DialAfter, else the backoff.
	delay := func() time.Duration {
		d := st.wait
		pol := c.cfg.RedialPolicy
		if pol == nil {
			return d
		}
		v := &st.view
		v.Now, v.Failures, v.LastDial, v.Members = clk.Now(), st.failures, st.lastDial, v.Members[:0]
		for _, ps := range c.PipeStats() {
			v.Members = append(v.Members, PoolMember{PipeID: ps.PipeID, Age: ps.AttachedFor, Idle: ps.SinceLastTraffic,
				Pipes: 1, BytesSent: ps.BytesSent, BytesReceived: ps.BytesReceived})
		}
		if err := decide(c.cfg.Logger, pol, v, &st.dec); err != nil {
			st.dec = PoolDecision{} // as a policy that panics
			reportRejected(c.cfg.Logger, c.cfg.DecisionRejected, err)
		}
		if st.dec.DialAfter > 0 {
			d = min(st.dec.DialAfter, hi)
		}
		return d
	}
	for {
		if !st.lastDial.IsZero() {
			// The join refusal backoff a refused join started is separate:
			// the call waits for the longer of the two.
			if w := max(timeshim.Until(clk, st.lastDial.Add(delay())), c.refusalWait()); w > 0 {
				t := timeshim.NewTimer(clk, w)
				select {
				case <-t.C:
				case <-c.done:
					t.Stop()
					return
				}
			}
		} else if c.cfg.RedialPolicy != nil {
			delay() // the policy is asked before the first call
		}
		st.lastDial = clk.Now()
		var p PipeIO
		var err error
		ok := safeLog(c.cfg.Logger, "Config.Redial", func() { p, err = c.cfg.Redial(c.ctx, need) })
		if c.closingNow() {
			if ok && err == nil {
				closePipeIO(p)
			}
			return
		}
		if ok && err == nil {
			ctx, cancel := timeshim.WithDeadline(c.ctx, clk, clk.Now().Add(c.cfg.joinTimeout()))
			_, err = c.AddPipe(ctx, p, st.dec.probeOptions(PipeOptions{Direction: need, ReturnOnReply: true}))
			cancel()
			if err == nil {
				st.failures, st.backoff = 0, lo
				st.wait = backoffWait(lo)
				return
			}
		}
		c.lock()
		out, in := c.s.LiveDirections()
		c.unlock()
		if out && in {
			return
		}
		if st.failures > 0 {
			st.backoff = min(2*st.backoff, hi)
		}
		st.failures++
		st.wait = backoffWait(st.backoff)
	}
}

// refusalWait is what remains of the join refusal backoff, which a refused
// join started.
func (c *Conn) refusalWait() time.Duration {
	c.lock()
	defer c.unlock()
	return time.Duration(c.s.JoinBackoffLeft(c.clk.now()))
}
