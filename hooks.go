package zmux

import (
	"context"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Tracer receives session events (IMPLEMENTATION.md 2.5.7). Its functions
// run serially and in order on a goroutine of the session, never under a
// lock; the session never waits for them: a bounded queue holds events,
// and on overflow the events other than Closed are dropped and counted.
type Tracer struct {
	// Established: the peer's transport parameters were processed.
	Established func()
	// Event: a pipe or session event (as NextPipeEvent reports them).
	Event func(PipeEvent)
	// Dropped: events lost to overflow since the previous call.
	Dropped func(count uint64)
	// Closed: the session ended; nothing follows.
	Closed func(err error)
}

// Pick is a custom scheduling policy (API.md 5.2). It runs outside every
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
	Urgency      uint8
}

// PickPipe is a pipe as a custom policy sees it (values only).
type PickPipe struct {
	Pipe      uint64
	State     string
	SRTT      time.Duration
	RTTVar    time.Duration
	Rate      uint64 // bytes/s
	Unsent    uint64
	InFlight  uint64
	Priority  uint64
	Weight    uint64
	Writable  bool
	Standby   bool
	Saturated bool
	Probation bool
	Eligible  bool // under tier gating: the built-in policies choose among these
}

// PickAnswer is the answer for one direction: Pipe is an index into the
// pipes, Bytes the bytes it covers (0: until invalidated); Wait holds the
// direction until the session's next input.
type PickAnswer struct {
	Pipe  int
	Bytes uint64
	Wait  bool
}

// Custom returns a scheduler that asks p.
func Custom(p Pick) Scheduler { return customPolicy{p} }

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

	mu      sync.Mutex
	q       []traceItem
	dropped uint64
	wantWin bool
	wantPk  bool
	running bool
	kick    chan struct{}
}

type traceItem struct {
	ev     PipeEvent
	est    bool
	closed bool
	err    error
}

func newHooks(c *Conn) *hooks {
	cfg := c.cfg
	h := &hooks{c: c, tr: cfg.Tracer, allow: cfg.AllowConnectionWindowIncrease, kick: make(chan struct{}, 1)}
	if cp, ok := cfg.Scheduler.(customPolicy); ok {
		h.pick = cp.p
	}
	if h.tr == nil && h.pick == nil && h.allow == nil {
		return nil
	}
	return h
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
	work := len(h.q) > 0 || h.wantWin || h.wantPk || h.dropped > 0
	run := work && !h.running
	if run {
		h.running = true
	}
	h.mu.Unlock()
	if run {
		go h.run()
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
		if len(q) == 0 && dropped == 0 && !win && !pk {
			h.running = false
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		for _, it := range q {
			h.deliver(it)
		}
		if dropped > 0 && h.tr.Dropped != nil {
			safeCall(func() { h.tr.Dropped(dropped) })
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
		safeCall(t.Established)
	case it.closed && t.Closed != nil:
		safeCall(func() { t.Closed(it.err) })
	case !it.est && !it.closed && t.Event != nil:
		safeCall(func() { t.Event(it.ev) })
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
	safeCall(func() { allow = h.allow(delta) })
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
		reqs[i] = PickRequest{Stream: StreamID(f.Stream), Urgency: f.Urgency, Len: f.Len, LastPipe: f.LastPipe,
			HasLastPipe: f.HasLastPipe, EligibleTier: f.EligibleTier}
	}
	pipes := make([]PickPipe, len(req.Pipes))
	for i, p := range req.Pipes {
		pipes[i] = PickPipe{Pipe: p.Pipe, State: p.State, SRTT: time.Duration(p.SRTT),
			RTTVar: time.Duration(p.RTTVar), Rate: p.Rate, Unsent: p.Unsent, InFlight: p.InFlight,
			Writable: p.Writable, Priority: p.Priority, Standby: p.Standby, Saturated: p.Saturated,
			Probation: p.Probation, Eligible: p.Eligible, Weight: p.Weight}
	}
	out := make([]PickAnswer, len(reqs))
	for i := range out {
		out[i].Wait = true
	}
	failed := !safeCall(func() { h.pick.Pick(reqs, pipes, out) })
	ans := make([]core.PickAnswer, len(out))
	for i, a := range out {
		ans[i] = core.PickAnswer{Wait: a.Wait, Index: a.Pipe, Bytes: a.Bytes}
	}
	c.lock()
	c.s.AnswerPick(&req, ans, failed)
	c.unlock()
}

// safeCall runs application code; a panic is contained (and reported as
// false).
func safeCall(f func()) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	f()
	return true
}

// ---- redial (RESUME.md 4.3, API.md 5.5) ----

// redialer re-establishes pipes with the Redial hook while the session
// lacks a direction: at most one call in flight per direction, cancelled
// when the session ends; a carrier returned after the end is closed.
type redialer struct {
	mu       sync.Mutex
	inFlight [3]bool
}

// maybeRedial starts redials for the directions the session lacks.
func (c *Conn) maybeRedial() {
	if c.cfg.Redial == nil || c.closingNow() {
		return
	}
	c.lock()
	out, in := c.s.LiveDirections()
	c.unlock()
	var need Direction
	switch {
	case out && in:
		return
	case !out && !in:
		need = Bidirectional
	case !out:
		need = SendOnly
	default:
		need = ReceiveOnly
	}
	r := &c.redial
	r.mu.Lock()
	if r.inFlight[need] {
		r.mu.Unlock()
		return
	}
	r.inFlight[need] = true
	r.mu.Unlock()
	go c.redialLoop(need)
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
	defer func() {
		c.redial.mu.Lock()
		c.redial.inFlight[need] = false
		c.redial.mu.Unlock()
	}()
	backoff := 250 * time.Millisecond
	for {
		var p PipeIO
		var err error
		ok := safeCall(func() { p, err = c.cfg.Redial(c.ctx, need) })
		if c.closingNow() {
			if ok && err == nil {
				closePipeIO(p)
			}
			return
		}
		if ok && err == nil {
			ctx, cancel := context.WithTimeout(c.ctx, c.handshakeTimeout())
			_, err = c.AddPipe(ctx, p, PipeOptions{Direction: need})
			cancel()
			if err == nil {
				return
			}
		}
		c.lock()
		out, in := c.s.LiveDirections()
		c.unlock()
		if out && in {
			return
		}
		select {
		case <-time.After(backoff):
		case <-c.done:
			return
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

func (c *Conn) handshakeTimeout() time.Duration {
	if c.cfg.HandshakeTimeout > 0 {
		return c.cfg.HandshakeTimeout
	}
	return 10 * time.Second
}
