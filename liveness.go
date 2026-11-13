package zmux

import (
	"fmt"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Liveness are the settings that judge whether a session and its pipes are
// alive: Config.Liveness, and Conn.SetLiveness while the session runs. The
// zero value of each field is its default and a negative value its off
// setting where it has one; other values outside a field's range are
// invalid (ErrInvalidConfig).
type Liveness struct {
	// KeepAlivePeriod sends a QX_PING when the session has had no activity
	// this long. Zero is 15 s (DefaultKeepAlivePeriod); a negative value
	// turns keep-alive off; otherwise at least 1 s. The period in effect is
	// capped at half the effective idle timeout, and each wait is drawn
	// from [k x (1 - j), k] with j the ProbeJitter. On, an idle session
	// stays open while the peer answers and closes within the idle timeout
	// once its carrier dies; off, the idle timeout ends any session without
	// application activity.
	KeepAlivePeriod time.Duration
	// SuspectFloor is the least time before a pipe without
	// acknowledgements turns suspect. Zero is 250 ms; otherwise at least
	// 50 ms.
	SuspectFloor time.Duration
	// DeadTimeout is how long a suspect pipe stays suspect before it
	// fails. Zero is derived from the RTT and the idle timeout; otherwise
	// at least 1 s.
	DeadTimeout time.Duration
	// StandbyProbe is the probe interval of standby pipes and of pipes of
	// a less-preferred tier while the session carries traffic. Zero is
	// 1 s; a negative value turns these probes off (such pipes are then
	// probed at ProbeInterval); otherwise at least 1 s.
	StandbyProbe time.Duration
	// ProbeInterval is the probe interval k of idle pipe pairs, measured
	// to the carrier. Zero is 15 s; a negative value turns these idle
	// probes off, which leaves the PIPE_PING of an answer, the standby
	// probes and the probes of pipes without an outbound direction at the
	// peer; otherwise at least 1 s.
	ProbeInterval time.Duration
	// ProbeJitter is the probe jitter j, below 1: each probe waits a
	// uniform draw from [k x (1 - j), k], the first one of pipe 0 from
	// [0, k]. Zero is 0.25 (DefaultProbeJitter); a negative value is a
	// jitter of 0, the fixed schedule.
	ProbeJitter float64
}

// core validates l and resolves it for the core, zero fields to the
// defaults.
func (l *Liveness) core() (core.Liveness, error) {
	d := core.DefaultConfig()
	cl := d.Liveness()
	switch {
	case l.KeepAlivePeriod < 0:
		cl.KeepAlive = 0
	case l.KeepAlivePeriod > 0 && l.KeepAlivePeriod < time.Second:
		return cl, fmt.Errorf("%w: Liveness.KeepAlivePeriod %v below 1 s", ErrInvalidConfig, l.KeepAlivePeriod)
	case l.KeepAlivePeriod > 0:
		cl.KeepAlive = core.Duration(l.KeepAlivePeriod)
	}
	pl := PipeLiveness{SuspectFloor: l.SuspectFloor, DeadTimeout: l.DeadTimeout, ProbeInterval: l.ProbeInterval, ProbeJitter: l.ProbeJitter}
	p, err := pl.core()
	if err != nil {
		return cl, err
	}
	if p.SuspectFloor != 0 {
		cl.SuspectFloor = p.SuspectFloor
	}
	if p.DeadTimeout != 0 {
		cl.DeadTimeout = p.DeadTimeout
	}
	if err := checkProbeInterval(l.StandbyProbe, "Liveness.StandbyProbe"); err != nil {
		return cl, err
	}
	switch {
	case l.StandbyProbe < 0:
		cl.StandbyProbe = 0
	case l.StandbyProbe > 0:
		cl.StandbyProbe = core.Duration(l.StandbyProbe)
	}
	switch {
	case p.ProbeInterval < 0:
		cl.ProbeInterval = 0
	case p.ProbeInterval > 0:
		cl.ProbeInterval = p.ProbeInterval
	}
	switch {
	case p.ProbeJitter < 0:
		cl.ProbeJitter = 0
	case p.ProbeJitter > 0:
		cl.ProbeJitter = p.ProbeJitter
	}
	return cl, nil
}

// Liveness returns the session's liveness settings in effect, resolved:
// defaults are filled in, a setting that is off is negative (a jitter of
// 0 included), and a derived DeadTimeout is zero, so that SetLiveness with
// the result changes nothing.
func (c *Conn) Liveness() Liveness {
	c.lock()
	cl := c.s.Liveness()
	c.unlock()
	off := func(d core.Duration) time.Duration {
		if d == 0 {
			return -1
		}
		return time.Duration(d)
	}
	j := cl.ProbeJitter
	if j == 0 {
		j = -1
	}
	return Liveness{KeepAlivePeriod: off(cl.KeepAlive), SuspectFloor: time.Duration(cl.SuspectFloor),
		DeadTimeout: time.Duration(cl.DeadTimeout), StandbyProbe: off(cl.StandbyProbe),
		ProbeInterval: off(cl.ProbeInterval), ProbeJitter: j}
}

// SetLiveness replaces the session's liveness settings as a whole, under
// the rules of Liveness: zero fields are the defaults, not the values the
// session was started with, and an invalid l is an ErrInvalidConfig error
// that changes nothing. The timers are rearmed at once: the keep-alive
// period (still capped at half the effective idle timeout), the probe
// intervals and the suspicion floor apply to the waits in progress, a
// jitter from the next draw, and the DeadTimeout to pipes that turn
// suspect from then on; a pipe already suspect keeps the SuspectFloor and
// DeadTimeout it turned suspect under. A pipe's own settings
// (PipeOptions.Liveness, PipeUpdate.Liveness) keep winning; every other
// pipe, and those joined later, follow the new values. Nothing is sent.
func (c *Conn) SetLiveness(l Liveness) error {
	cl, err := l.core()
	if err != nil {
		return err
	}
	c.lock()
	defer c.unlock()
	c.s.SetLiveness(cl)
	return nil
}

// PipeLiveness are a pipe's own liveness settings (PipeOptions.Liveness,
// PipeUpdate.Liveness, PoolDecision.Liveness), the Liveness fields that
// apply to one pipe with their meanings and bounds; a zero field follows
// the session's.
type PipeLiveness struct {
	// SuspectFloor is at least 50 ms.
	SuspectFloor time.Duration
	// DeadTimeout is at least 1 s.
	DeadTimeout time.Duration
	// ProbeInterval is the probe interval of the pipe while it is idle,
	// measured to the carrier: with a shaping delay of up to H, probes wait
	// k_eff = max(1 s, k - H). A negative value turns the pipe's idle
	// probes off; otherwise at least 1 s. On a pipe without an outbound
	// direction at the peer at most 15 s takes effect, and the probes stay.
	ProbeInterval time.Duration
	// ProbeJitter is below 1; a negative value is a jitter of 0.
	ProbeJitter float64
}

// core validates l and resolves it for the core: 0 the session's, a
// negative interval off, a negative jitter 0.
func (l *PipeLiveness) core() (core.PipeLiveness, error) {
	var cl core.PipeLiveness
	if l.SuspectFloor < 0 || l.DeadTimeout < 0 {
		return cl, fmt.Errorf("%w: negative SuspectFloor or DeadTimeout", ErrInvalidConfig)
	}
	if l.SuspectFloor != 0 && l.SuspectFloor < 50*time.Millisecond {
		return cl, fmt.Errorf("%w: SuspectFloor below 50 ms", ErrInvalidConfig)
	}
	if l.DeadTimeout != 0 && l.DeadTimeout < time.Second {
		return cl, fmt.Errorf("%w: DeadTimeout below 1 s", ErrInvalidConfig)
	}
	if err := checkProbeInterval(l.ProbeInterval, "ProbeInterval"); err != nil {
		return cl, err
	}
	if err := checkProbeJitter(l.ProbeJitter, "ProbeJitter"); err != nil {
		return cl, err
	}
	cl.SuspectFloor, cl.DeadTimeout = core.Duration(l.SuspectFloor), core.Duration(l.DeadTimeout)
	cl.ProbeInterval = core.Duration(max(l.ProbeInterval, -1))
	cl.ProbeJitter = max(l.ProbeJitter, -1)
	return cl, nil
}

// RaiseIncomingStreams raises the number of bidirectional and of
// unidirectional streams the peer may have open (MaxIncomingStreams,
// MaxIncomingUniStreams) to bidi and uni while the session runs, and
// sends the credit as MAX_STREAMS. The credit only grows: a
// value at or below the current one leaves that type as it is, so 0
// raises nothing. While the unidirectional credit is still staged
// (MaxIncomingUniStreams zero), uni raises the value the first
// AcceptUniStream moves it to. After GOAWAY no credit is sent. A negative
// value or one above 2^60 is an ErrInvalidConfig error that changes
// nothing.
func (c *Conn) RaiseIncomingStreams(bidi, uni int64) error {
	if bidi < 0 || uni < 0 || bidi > 1<<60 || uni > 1<<60 {
		return fmt.Errorf("%w: RaiseIncomingStreams %d, %d outside [0, 2^60]", ErrInvalidConfig, bidi, uni)
	}
	c.lock()
	defer c.unlock()
	c.s.RaiseIncomingStreams(true, uint64(bidi))
	c.s.RaiseIncomingStreams(false, uint64(uni))
	return nil
}
