package zmux

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
)

// PipePool keeps pipes of one path up.
type PipePool struct {
	// Dial opens a carrier of the path. Its context bounds the dial and
	// ends once the join completes; the carrier then lives until the pipe
	// is closed.
	Dial func(ctx context.Context) (PipeIO, error)
	// Join, in place of Dial, joins the pipes of one member of the path
	// itself, such as an uplink and a downlink of a split carrier
	// (zmuxhttp.Split.Join), with the options given; the member is usable while
	// all of them are, and they are replaced together. ctx bounds the
	// joins; o has ReturnOnReply set, as the pool's own joins do.
	Join func(ctx context.Context, c *Conn, o PipeOptions) ([]*Pipe, error)
	// Choice, in place of Dial and Join, dials each member over a carrier
	// kind of the mode's candidates (a split candidate joins as a split
	// carrier), presenting the session's Config.CarrierCredential. Stable first,
	// then upgrade: a member is dialed over the most robust kind available
	// (a kind CarrierChoicePolicy.Cache holds as usable, else the exchange carrier,
	// else the last), skipping the kinds the cache holds as failed unless
	// all are; a member of a kind that a preferred kind not backing off
	// could replace is rotated to it, make before break, and the
	// replacement takes over once it has stayed usable for
	// CarrierChoicePolicy.Settle. A kind whose join fails, whose replacement ends
	// before it settled, or whose member fails within
	// CarrierChoicePolicy.DropWindow of being joined, is recorded as failed. Each
	// try is reported as an EventCarrierCandidate, and a member joined
	// over a kind other than the session's carrier as an
	// EventCarrierChosen. ExplicitChoice dials its one kind only, without
	// the cache.
	Choice CarrierChoice
	// ChoicePolicy is the policy of Choice (nil: the defaults, with a cache
	// private to the pool); its Join does not apply.
	ChoicePolicy *CarrierChoicePolicy
	// Count is the number of usable pipes to keep (0: 1, as a pool of no
	// pipes has nothing to maintain).
	Count int
	// Options of the pipes the pool joins.
	Options PipeOptions
	// Rotate replaces pipes that carried MaxBytes or are MaxAge old.
	Rotate Rotation
	// Backoff bounds the redial backoff (from 1 s, at most 60 s, jitter
	// 0.2 when unset), counted from the start of the previous dial attempt,
	// whether it succeeded or failed, or from the start of the pool before
	// its first; each wait is reduced by a random fraction of up to the
	// jitter, drawn once per wait. The zero value selects the defaults: no
	// backoff would redial a failing path in a busy loop.
	Backoff Backoff
	// Policy, when set, is asked on every pass of the pool; its
	// DialAfter replaces the backoff, bounded by Backoff.Max, and its
	// Liveness applies to the pipes dialled after it.
	// A decision that fails PoolDecision.Validate is rejected: the pass
	// goes on as with the zero decision and the error is reported to
	// DecisionRejected.
	Policy PoolPolicy
	// DecisionRejected, when set, receives the PoolDecision.Validate error
	// (ErrInvalidConfig) of each decision of Policy the pool rejected, on
	// the goroutine of MaintainPipes and outside every lock; it must not
	// block, and a panic in it is contained. The pool goes on maintaining
	// its pipes whether it is set or not.
	DecisionRejected func(err error)
}

// Rotation replaces pipes before a lifetime limit: when a pipe has carried
// MaxBytes in either direction or is MaxAge old (each reduced by a random
// fraction of up to Jitter), a replacement is dialled and joined, and the
// old pipe is closed gracefully once the replacement is usable and attached
// by the peer (Pipe.WaitAttached): make before break.
type Rotation struct {
	MaxBytes int64         // 0: no byte limit
	MaxAge   time.Duration // 0: no age limit
	Jitter   float64       // 0 to 1
}

// memberState sums the statistics of a member's pipes (SinceLastTraffic:
// the least) and reports whether every pipe is alive (its outbound
// direction neither failed nor closed, or its inbound one open for a
// downlink) and usable (active outbound out of probation, or open inbound,
// and attached by the peer: PipeStats.PeerAttached, the state WaitAttached
// waits for). A joiner reads a join as joined at its verified Reply, before
// the acceptor has its attachment state, so closing the pipe it replaces
// any earlier could remove the acceptor's last pipe.
func memberState(ps []*Pipe) (sum PipeStats, alive, usable bool) {
	alive, usable = len(ps) > 0, len(ps) > 0
	for i, p := range ps {
		st, ok := p.Stats()
		if !ok {
			return sum, false, false
		}
		sum.BytesSent += st.BytesSent
		sum.BytesReceived += st.BytesReceived
		if i == 0 || st.SinceLastTraffic < sum.SinceLastTraffic {
			sum.SinceLastTraffic = st.SinceLastTraffic
		}
		usable = usable && st.PeerAttached
		if st.Outbound == OutboundNone {
			alive = alive && st.Inbound == InboundOpen || alive && st.Inbound == InboundJoining
			usable = usable && st.Inbound == InboundOpen
			continue
		}
		alive = alive && st.Outbound != OutboundFailed && st.Outbound != OutboundClosed
		usable = usable && st.Outbound == OutboundActive && st.ProbationUntil.IsZero()
	}
	return sum, alive, usable
}

// MaintainPipes keeps pool.Count usable pipes dialled by pool.Dial: it
// redials failed pipes with exponential backoff and rotates pipes by
// pool.Rotate, until ctx ends or the session closes. The backoff, or the
// policy's DialAfter in its place, counts from the start of the previous
// dial attempt (a rotation's included), or from the start of the pool
// before the first. The pool decides again at once on pipe failures, the
// loss of a direction and the suspension of the session, keeps dialling
// while the session is suspended, and dials outside every lock. It
// returns ctx's error or the session's; when ctx ends, a replacement still
// being rotated in is closed and the pool's other pipes stay.
func (c *Conn) MaintainPipes(ctx context.Context, pool PipePool) error {
	return c.maintainPipes(ctx, pool, nil)
}

// maintainPipes is MaintainPipes; seed, when set, is the pool's mode and
// its first member (DialChoice).
func (c *Conn) maintainPipes(ctx context.Context, pool PipePool, seed *modeSeed) error {
	if c == nil {
		return net.ErrClosed
	}
	c.lock()
	multipath := c.s.Features().Multipath
	c.unlock()
	if !multipath {
		return ErrNotNegotiated
	}
	if pool.Count < 0 || pool.Rotate.MaxAge < 0 || pool.Rotate.MaxBytes < 0 || !(pool.Rotate.Jitter >= 0 && pool.Rotate.Jitter <= 1) {
		return fmt.Errorf("%w: PipePool with a negative field or a Rotate.Jitter outside [0, 1]", ErrInvalidConfig)
	}
	if pool.Count == 0 {
		pool.Count = 1
	}
	if err := pool.Backoff.validate("PipePool.Backoff"); err != nil {
		return err
	}
	pool.Backoff = pool.Backoff.filled()
	var mp *modePool
	switch {
	case seed != nil:
		mp = seed.mp
	case len(pool.Choice.candidates) > 0:
		var err error
		if mp, err = newModePool(pool.Choice, pool.ChoicePolicy, c.cfg.CarrierCredential); err != nil {
			return err
		}
	}
	if n := btoi(pool.Dial != nil) + btoi(pool.Join != nil) + btoi(mp != nil); n != 1 {
		return fmt.Errorf("%w: PipePool needs exactly one of Dial, Join and Mode", ErrInvalidConfig)
	}
	c.pools.Add(1) // a running pool attends a suspension (EventResumeUnattended)
	defer c.pools.Add(-1)
	type member struct {
		ps     []*Pipe
		born   time.Time
		limitB uint64
		limitT time.Duration
		repl   []*Pipe // its replacement while rotating
		idle   time.Duration
		retire bool // the policy asked for its replacement
		// With a mode: the kind of ps and of repl, when ps were joined
		// and repl became usable (wall clock, for the cache).
		kind, replKind   int
		joined, replSeen time.Time
	}
	closeAll := func(ps []*Pipe) {
		for _, p := range ps {
			_ = p.Close()
		}
	}
	var members []*member
	clk := c.clk.source()
	start := clk.Now()
	var (
		view     PoolView
		dec      PoolDecision
		failures int
		lastDial time.Time
		backoff  = pool.Backoff.Min
		wait     time.Duration // the built-in wait after lastDial
	)
	jitter := func(v float64) float64 {
		if pool.Rotate.Jitter == 0 {
			return v
		}
		return v * (1 - rand.Float64()*pool.Rotate.Jitter)
	}
	// Pipe state changes, the loss of a direction and the suspension of
	// the session wake the pool at once.
	sig := core.NewSignal()
	defer c.unwatchPipes(sig)
	tick := timeshim.NewTicker(clk, 250*time.Millisecond)
	defer tick.Stop()
	timer := timeshim.NewTimer(clk, time.Hour) // the next recheck or dial, per pass
	timer.Stop()
	defer timer.Stop()
	join := func(kind int) ([]*Pipe, error) {
		dctx, cancel := timeshim.WithDeadline(ctx, clk, clk.Now().Add(c.cfg.joinTimeout()))
		defer cancel()
		o := pool.Options
		// A replacement closes its old pipe only once attached by the peer
		// (memberState), and a redial must not wait for an acknowledgement
		// that only the direction it restores could carry.
		o.ReturnOnReply = true
		if pool.Policy != nil {
			o = dec.probeOptions(o)
		}
		if mp != nil {
			return mp.join(dctx, c, kind, o)
		}
		if pool.Join != nil {
			var ps []*Pipe
			var err error
			if !safeLog(c.cfg.Logger, "PipePool.Join", func() { ps, err = pool.Join(dctx, c, o) }) {
				err = errors.New("zmux: PipePool.Join panicked")
			}
			if err == nil && len(ps) == 0 {
				err = errors.New("zmux: PipePool.Join joined no pipe")
			}
			return ps, err
		}
		p, err := pool.Dial(dctx)
		if err != nil {
			return nil, err
		}
		pp, err := c.AddPipe(dctx, p, o)
		if err != nil {
			return nil, err
		}
		return []*Pipe{pp}, nil
	}
	// dial joins a member now, counting the attempt for the backoff that
	// every dial of the pool, redial or rotation, shares.
	dial := func(kind int) ([]*Pipe, error) {
		lastDial = clk.Now()
		ps, err := join(kind)
		if err != nil {
			failures++
			wait = pool.Backoff.wait(backoff)
			backoff = min(2*backoff, pool.Backoff.Max)
			return nil, err
		}
		backoff, failures = pool.Backoff.Min, 0
		wait = pool.Backoff.wait(backoff)
		return ps, nil
	}
	newMember := func(ps []*Pipe, kind int) *member {
		return &member{ps: ps, born: clk.Now(), limitB: uint64(jitter(float64(pool.Rotate.MaxBytes))),
			limitT: time.Duration(jitter(float64(pool.Rotate.MaxAge))), kind: kind, joined: time.Now()}
	}
	if seed != nil {
		m := newMember(seed.pipes, seed.kind)
		m.joined = seed.born
		members = append(members, m)
	}
	// With a mode, a session that fails counts against the kinds of the
	// members joined less than DropWindow ago.
	sessionEnded := func() error {
		err := c.closedErr()
		var ae *ApplicationError
		if mp != nil && !errors.As(c.Wait(), &ae) {
			for _, m := range members {
				mp.dropped(m.kind, m.joined)
			}
		}
		return err
	}
	// A replacement still being rotated in is the pool's own: it goes when
	// the pool stops; the members' pipes stay.
	stopped := func() error {
		for _, m := range members {
			if m.repl != nil {
				closeAll(m.repl)
				m.repl = nil
			}
		}
		return ctx.Err()
	}
	for {
		select {
		case <-ctx.Done():
			return stopped()
		case <-c.done:
			return sessionEnded()
		default:
		}
		// Watch before reading the state: a change after the read wakes
		// the pass's wait.
		c.lock()
		sig.Drain()
		c.watchPipes(sig)
		c.unlock()
		// Drop members whose pipe is gone or failed.
		live := members[:0]
		now := clk.Now()
		view.Members = view.Members[:0]
		for _, m := range members {
			st, alive, _ := memberState(m.ps)
			if alive {
				live = append(live, m)
				m.idle = st.SinceLastTraffic
				view.Members = append(view.Members, PoolMember{PipeID: m.ps[0].ID(), Age: now.Sub(m.born),
					Idle: m.idle, Pipes: len(m.ps), BytesSent: st.BytesSent, BytesReceived: st.BytesReceived})
			} else {
				closeAll(m.ps) // the rest of a member goes with its failed pipe
				if mp != nil && !c.isDone() {
					mp.dropped(m.kind, m.joined)
				}
				if m.repl != nil {
					r := newMember(m.repl, m.replKind)
					if mp != nil {
						mp.established(c, r.kind)
					}
					live = append(live, r)
				}
			}
		}
		members = live
		count := pool.Count
		delay := wait
		if pool.Policy != nil {
			view.Now, view.Failures, view.LastDial = now, failures, lastDial
			if err := decide(c.cfg.Logger, pool.Policy, &view, &dec); err != nil {
				dec = PoolDecision{RetirePipes: dec.RetirePipes[:0]} // rejected, as a policy that panics
				reportRejected(c.cfg.Logger, pool.DecisionRejected, err)
			}
			if dec.DialAfter > 0 {
				delay = min(dec.DialAfter, pool.Backoff.Max)
			}
			if dec.Count > 0 {
				count = dec.Count
			}
			for _, id := range dec.RetirePipes {
				for _, m := range members {
					if m.ps[0].ID() == id {
						m.retire = true
					}
				}
			}
			// Members beyond the count close, the longest idle first.
			for len(members) > count {
				k := 0
				for i, m := range members {
					if m.retire && !members[k].retire || m.retire == members[k].retire && m.idle > members[k].idle {
						k = i
					}
				}
				m := members[k]
				closeAll(m.ps)
				closeAll(m.repl)
				members = append(members[:k], members[k+1:]...)
			}
		}
		out, in := false, false
		c.lock()
		if c.s.Features().Multipath {
			out, in = c.s.LiveDirections()
		}
		c.unlock()
		lacking := !out || !in
		// Redial the missing ones, delay after the previous attempt.
		from := lastDial
		if from.IsZero() {
			from = start
		}
		// The join refusal backoff holds joins on its own (JoinReady): a
		// dial waits for the longer of the two.
		dialDue := now.Sub(from) >= delay
		var dialAt time.Time
		if len(members) < count && c.joinReady() {
			if dialDue {
				kind := -1
				if mp != nil {
					kind = mp.pick(time.Now())
				}
				if ps, err := dial(kind); err == nil {
					members = append(members, newMember(ps, kind))
					if mp != nil {
						mp.established(c, kind)
					}
				}
				continue
			}
			dialAt = from.Add(delay)
		}
		// Rotate: only usable pipes, never while a direction is lacking, one
		// dial per pass under the same backoff as a redial.
		// With a mode, a member is also rotated to a preferred kind.
		dialed := false
		for _, m := range members {
			upgrade := -1
			if mp != nil && m.repl == nil {
				upgrade = mp.better(m.kind, time.Now())
			}
			if lacking || !c.joinReady() || m.repl != nil ||
				!m.retire && upgrade < 0 && pool.Rotate.MaxBytes == 0 && pool.Rotate.MaxAge == 0 {
				continue
			}
			st, _, usable := memberState(m.ps)
			if !usable {
				continue
			}
			due := m.retire || upgrade >= 0 || pool.Rotate.MaxBytes > 0 && max(st.BytesSent, st.BytesReceived) >= m.limitB ||
				pool.Rotate.MaxAge > 0 && timeshim.Since(clk, m.born) >= m.limitT
			if !due {
				continue
			}
			if !dialDue {
				dialAt = from.Add(delay)
				break
			}
			kind := upgrade
			if mp != nil && kind < 0 {
				kind = mp.pick(time.Now())
			}
			if r, err := dial(kind); err == nil {
				m.repl, m.replKind, m.replSeen = r, kind, time.Time{}
			}
			dialed = true
			break
		}
		if dialed {
			continue // decide again before the next dial
		}
		// Close an old pipe once its replacement is usable (with a mode,
		// once it has stayed usable for Settle); a replacement that ends
		// first is dropped.
		for _, m := range members {
			if m.repl == nil {
				continue
			}
			_, alive, usable := memberState(m.repl)
			if !alive {
				closeAll(m.repl)
				if mp != nil {
					mp.record(m.replKind, false)
				}
				m.repl = nil
				continue
			}
			if !usable {
				continue // a state change or the peer's attachment wakes the pass
			}
			if mp != nil {
				if m.replSeen.IsZero() {
					m.replSeen = time.Now()
				}
				if time.Since(m.replSeen) < mp.p.settle {
					continue // the pass tick looks again
				}
			}
			closeAll(m.ps)
			*m = *newMember(m.repl, m.replKind)
			if mp != nil {
				mp.established(c, m.kind)
			}
		}
		next := time.Duration(-1)
		if r := dec.RecheckAfter(); r > 0 && pool.Policy != nil {
			next = r
		}
		if !dialAt.IsZero() {
			if d := max(timeshim.Until(clk, dialAt), MinRecheck); next < 0 || d < next {
				next = d
			}
		}
		var wake <-chan time.Time
		if next >= 0 {
			timer.Reset(next)
			wake = timer.C
		}
		select {
		case <-sig.C:
		case <-tick.C:
		case <-wake:
		case <-ctx.Done():
			return stopped()
		case <-c.done:
			return sessionEnded()
		}
		timer.Stop()
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// isDone reports whether the session has ended.
func (c *Conn) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
