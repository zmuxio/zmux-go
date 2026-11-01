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
	// (HTTPSplitJoin), with the options given; the member is usable while
	// all of them are, and they are replaced together. ctx bounds the
	// joins.
	Join func(ctx context.Context, c *Conn, o PipeOptions) ([]*Pipe, error)
	// Count is the number of usable pipes to keep (0: 1, as a pool of no
	// pipes has nothing to maintain).
	Count int
	// Options of the pipes the pool joins.
	Options PipeOptions
	// Rotate replaces pipes that carried MaxBytes or are MaxAge old.
	Rotate Rotation
	// Backoff bounds the redial backoff (from 1 s, at most 60 s), counted
	// from the start of the previous dial attempt, whether it succeeded or
	// failed, or from the start of the pool before its first; each wait is
	// reduced by a random fraction of up to 0.2, drawn once per wait. 0
	// selects the default: no backoff would redial a failing path in a
	// busy loop.
	MinBackoff, MaxBackoff time.Duration
	// Policy, when set, is asked on every pass of the pool; its
	// DialAfter replaces the backoff, bounded by MaxBackoff, and its
	// ProbeInterval and ProbeJitter apply to the pipes dialled after it.
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
// old pipe is closed gracefully once the replacement is usable (make
// before break).
type Rotation struct {
	MaxBytes uint64        // 0: no byte limit
	MaxAge   time.Duration // 0: no age limit
	Jitter   float64       // 0 to 1
}

// memberState sums the statistics of a member's pipes (SinceLastTraffic:
// the least) and reports whether every pipe is alive (its outbound
// direction neither failed nor closed, or its inbound one open for a
// downlink) and usable (active outbound out of probation, or open
// inbound).
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

// backoffJitter is the jitter of the built-in redial backoffs: a wait is
// the backoff reduced by a random fraction of up to it, drawn once per
// wait, as a pool policy's dial_jitter.
const backoffJitter = 0.2

func backoffWait(b time.Duration) time.Duration {
	return time.Duration(float64(b) * (1 - backoffJitter*rand.Float64()))
}

// MaintainPipes keeps pool.Count usable pipes dialled by pool.Dial: it
// redials failed pipes with exponential backoff and rotates pipes by
// pool.Rotate, until ctx ends or the session closes. The backoff, or the
// policy's DialAfter in its place, counts from the start of the previous
// dial attempt (a rotation's included), or from the start of the pool
// before the first. The pool decides again at once on pipe failures, the
// loss of a direction and the suspension of the session, keeps dialling
// while the session is suspended, and dials outside every lock. It
// returns ctx's error or the session's.
func (c *Conn) MaintainPipes(ctx context.Context, pool PipePool) error {
	if c == nil {
		return net.ErrClosed
	}
	c.lock()
	multipath := c.s.Features().Multipath
	c.unlock()
	if !multipath {
		return ErrNotNegotiated
	}
	if pool.Count < 0 || pool.MinBackoff < 0 || pool.MaxBackoff < 0 || pool.Rotate.MaxAge < 0 || !(pool.Rotate.Jitter >= 0 && pool.Rotate.Jitter <= 1) {
		return fmt.Errorf("%w: PipePool with a negative field or a Rotate.Jitter outside [0, 1]", ErrInvalidConfig)
	}
	if pool.Count == 0 {
		pool.Count = 1
	}
	if pool.MinBackoff == 0 {
		pool.MinBackoff = time.Second
	}
	if pool.MaxBackoff == 0 {
		pool.MaxBackoff = DefaultMaxBackoff
	}
	if (pool.Dial == nil) == (pool.Join == nil) {
		return fmt.Errorf("%w: PipePool needs exactly one of Dial and Join", ErrInvalidConfig)
	}
	type member struct {
		ps     []*Pipe
		born   time.Time
		limitB uint64
		limitT time.Duration
		repl   []*Pipe // its replacement while rotating
		idle   time.Duration
		retire bool // the policy asked for its replacement
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
		backoff  = pool.MinBackoff
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
	join := func() ([]*Pipe, error) {
		dctx, cancel := timeshim.WithDeadline(ctx, clk, clk.Now().Add(c.cfg.handshakeTimeout()))
		defer cancel()
		o := pool.Options
		if pool.Policy != nil {
			o = dec.probeOptions(o)
		}
		if pool.Join != nil {
			var ps []*Pipe
			var err error
			if !safeCall(func() { ps, err = pool.Join(dctx, c, o) }) {
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
			closePipeIO(p)
			return nil, err
		}
		return []*Pipe{pp}, nil
	}
	// dial joins a member now, counting the attempt for the backoff that
	// every dial of the pool, redial or rotation, shares.
	dial := func() ([]*Pipe, error) {
		lastDial = clk.Now()
		ps, err := join()
		if err != nil {
			failures++
			wait = backoffWait(backoff)
			backoff = min(2*backoff, pool.MaxBackoff)
			return nil, err
		}
		backoff, failures = pool.MinBackoff, 0
		wait = backoffWait(backoff)
		return ps, nil
	}
	newMember := func(ps []*Pipe) *member {
		return &member{ps: ps, born: clk.Now(), limitB: uint64(jitter(float64(pool.Rotate.MaxBytes))),
			limitT: time.Duration(jitter(float64(pool.Rotate.MaxAge)))}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.closedErr()
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
				view.Members = append(view.Members, PoolMember{ID: m.ps[0].ID(), Age: now.Sub(m.born),
					Idle: m.idle, Pipes: len(m.ps), BytesSent: st.BytesSent, BytesReceived: st.BytesReceived})
			} else {
				closeAll(m.ps) // the rest of a member goes with its failed pipe
				if m.repl != nil {
					live = append(live, newMember(m.repl))
				}
			}
		}
		members = live
		count := pool.Count
		delay := wait
		if pool.Policy != nil {
			view.Now, view.Failures, view.LastDial = now, failures, lastDial
			if err := decide(pool.Policy, &view, &dec); err != nil {
				dec = PoolDecision{Retire: dec.Retire[:0]} // rejected, as a policy that panics
				reportRejected(pool.DecisionRejected, err)
			}
			if dec.DialAfter > 0 {
				delay = min(dec.DialAfter, pool.MaxBackoff)
			}
			if dec.Count > 0 {
				count = dec.Count
			}
			for _, id := range dec.Retire {
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
		dialDue := now.Sub(from) >= delay
		var dialAt time.Time
		if len(members) < count && c.JoinReady() {
			if dialDue {
				if ps, err := dial(); err == nil {
					members = append(members, newMember(ps))
				}
				continue
			}
			dialAt = from.Add(delay)
		}
		// Rotate: only usable pipes, never while a direction is lacking, one
		// dial per pass under the same backoff as a redial.
		dialed := false
		for _, m := range members {
			if lacking || !c.JoinReady() || m.repl != nil || !m.retire && pool.Rotate.MaxBytes == 0 && pool.Rotate.MaxAge == 0 {
				continue
			}
			st, _, usable := memberState(m.ps)
			if !usable {
				continue
			}
			due := m.retire || pool.Rotate.MaxBytes > 0 && max(st.BytesSent, st.BytesReceived) >= m.limitB ||
				pool.Rotate.MaxAge > 0 && timeshim.Since(clk, m.born) >= m.limitT
			if !due {
				continue
			}
			if !dialDue {
				dialAt = from.Add(delay)
				break
			}
			if r, err := dial(); err == nil {
				m.repl = r
			}
			dialed = true
			break
		}
		if dialed {
			continue // decide again before the next dial
		}
		// Close an old pipe once its replacement is usable.
		for _, m := range members {
			if m.repl == nil {
				continue
			}
			if _, _, usable := memberState(m.repl); usable {
				closeAll(m.ps)
				*m = *newMember(m.repl)
			}
		}
		next := time.Duration(-1)
		if r := recheckAfter(&dec); r > 0 && pool.Policy != nil {
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
			return ctx.Err()
		case <-c.done:
			return c.closedErr()
		}
		timer.Stop()
	}
}
