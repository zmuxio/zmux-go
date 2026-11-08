package zmux

import (
	"fmt"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Liveness are the settings of a session that keep it and its pipes
// alive and detect their failure: Config.KeepAlivePeriod and the
// SchedulerParams fields of the same names, with their meanings, their
// defaults for zero values and their bounds. Conn.SetLiveness replaces
// them while the session runs.
type Liveness struct {
	// KeepAlivePeriod is Config.KeepAlivePeriod: zero is 15 s, a negative
	// value off, otherwise at least 1 s; the period in effect is capped at
	// half the effective idle timeout.
	KeepAlivePeriod time.Duration
	// SuspectFloor is SchedulerParams.SuspectFloor: zero is 250 ms,
	// otherwise at least 50 ms.
	SuspectFloor time.Duration
	// DeadTimeout is SchedulerParams.DeadTimeout: zero is derived from the
	// RTT, otherwise at least 1 s.
	DeadTimeout time.Duration
	// StandbyProbe is SchedulerParams.StandbyProbe: zero is 1 s, a
	// negative value off, otherwise at least 1 s.
	StandbyProbe time.Duration
	// ProbeInterval is SchedulerParams.ProbeInterval: zero is 15 s, a
	// negative value off, otherwise at least 1 s.
	ProbeInterval time.Duration
	// ProbeJitter is SchedulerParams.ProbeJitter, from 0 to below 1: zero
	// is 0.25; a jitter of 0 takes NoProbeJitter.
	ProbeJitter float64
	// NoProbeJitter makes the jitter 0, the fixed schedule.
	NoProbeJitter bool
}

// liveness returns the Liveness fields of c.
func (c *Config) liveness() Liveness {
	p := &c.SchedulerParams
	return Liveness{KeepAlivePeriod: c.KeepAlivePeriod, SuspectFloor: p.SuspectFloor, DeadTimeout: p.DeadTimeout,
		StandbyProbe: p.StandbyProbe, ProbeInterval: p.ProbeInterval, ProbeJitter: p.ProbeJitter,
		NoProbeJitter: p.NoProbeJitter}
}

// core validates l and resolves it for the core, zero fields to the
// defaults.
func (l *Liveness) core() (core.Liveness, error) {
	d := core.DefaultConfig()
	cl := d.Liveness()
	if l.SuspectFloor < 0 || l.DeadTimeout < 0 {
		return cl, fmt.Errorf("%w: negative SuspectFloor or DeadTimeout", ErrInvalidConfig)
	}
	switch {
	case l.KeepAlivePeriod < 0:
		cl.KeepAlive = 0
	case l.KeepAlivePeriod > 0 && l.KeepAlivePeriod < time.Second:
		return cl, fmt.Errorf("%w: KeepAlivePeriod %v neither 0 nor at least 1 s", ErrInvalidConfig, l.KeepAlivePeriod)
	case l.KeepAlivePeriod > 0:
		cl.KeepAlive = core.Duration(l.KeepAlivePeriod)
	}
	if l.SuspectFloor != 0 {
		if l.SuspectFloor < 50*time.Millisecond {
			return cl, fmt.Errorf("%w: SuspectFloor below 50 ms", ErrInvalidConfig)
		}
		cl.SuspectFloor = core.Duration(l.SuspectFloor)
	}
	if l.DeadTimeout != 0 {
		if l.DeadTimeout < time.Second {
			return cl, fmt.Errorf("%w: DeadTimeout below 1 s", ErrInvalidConfig)
		}
		cl.DeadTimeout = core.Duration(l.DeadTimeout)
	}
	if err := checkProbeInterval(l.StandbyProbe, "StandbyProbe"); err != nil {
		return cl, err
	}
	switch {
	case l.StandbyProbe < 0:
		cl.StandbyProbe = 0
	case l.StandbyProbe > 0:
		cl.StandbyProbe = core.Duration(l.StandbyProbe)
	}
	if err := checkProbeInterval(l.ProbeInterval, "ProbeInterval"); err != nil {
		return cl, err
	}
	switch {
	case l.ProbeInterval < 0:
		cl.ProbeInterval = 0
	case l.ProbeInterval > 0:
		cl.ProbeInterval = core.Duration(l.ProbeInterval)
	}
	if err := checkProbeJitter(l.ProbeJitter, "ProbeJitter"); err != nil {
		return cl, err
	}
	switch {
	case l.NoProbeJitter:
		cl.ProbeJitter = 0
	case l.ProbeJitter > 0:
		cl.ProbeJitter = l.ProbeJitter
	}
	return cl, nil
}

// Liveness returns the session's liveness settings in effect, resolved:
// defaults are filled in, a setting that is off is negative (a jitter of
// 0 is NoProbeJitter), and a derived DeadTimeout is zero, so that
// SetLiveness with the result changes nothing.
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
	return Liveness{KeepAlivePeriod: off(cl.KeepAlive), SuspectFloor: time.Duration(cl.SuspectFloor),
		DeadTimeout: time.Duration(cl.DeadTimeout), StandbyProbe: off(cl.StandbyProbe),
		ProbeInterval: off(cl.ProbeInterval), ProbeJitter: cl.ProbeJitter, NoProbeJitter: cl.ProbeJitter == 0}
}

// SetLiveness replaces the session's liveness settings as a whole, under
// the rules of Config: zero fields are the defaults, not the values the
// session was started with, and an invalid l is an ErrInvalidConfig error
// that changes nothing. The timers are rearmed at once: the keep-alive
// period (still capped at half the effective idle timeout), the probe
// intervals and the suspicion floor apply to the waits in progress, a
// jitter from the next draw, and the DeadTimeout to pipes that turn
// suspect from then on; a pipe already suspect keeps the SuspectFloor and
// DeadTimeout it turned suspect under. A pipe's own settings
// (PipeOptions, Pipe.SetLiveness) keep winning; every other pipe, and
// those joined later, follow the new values. Nothing is sent.
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

// PipeLiveness are a pipe's own liveness settings, the fields of
// PipeOptions of the same names with their meanings: a zero field follows
// the session's (SchedulerParams, Conn.SetLiveness).
type PipeLiveness struct {
	// SuspectFloor (at least 50 ms) and DeadTimeout (at least 1 s).
	SuspectFloor time.Duration
	DeadTimeout  time.Duration
	// ProbeInterval is the probe interval of the pipe while it is idle:
	// a negative value turns its idle probes off, otherwise at least 1 s.
	ProbeInterval time.Duration
	// ProbeJitter is from 0 to below 1; a jitter of 0 takes NoProbeJitter.
	ProbeJitter float64
	// NoProbeJitter makes the pipe's jitter 0, the fixed schedule.
	NoProbeJitter bool
}

// core validates l and resolves it for the core: 0 the session's, a
// negative interval off, a negative jitter 0.
func (l *PipeLiveness) core() (core.PipeLiveness, error) {
	var cl core.PipeLiveness
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
	cl.ProbeInterval, cl.ProbeJitter = core.Duration(max(l.ProbeInterval, -1)), l.ProbeJitter
	if l.NoProbeJitter {
		cl.ProbeJitter = -1
	}
	return cl, nil
}

// SetLiveness replaces the pipe's own liveness settings as a whole (the
// PipeOptions it was added with included); zero fields follow the
// session's, and an invalid l is an ErrInvalidConfig error that changes
// nothing. It takes effect as Conn.SetLiveness does: a pipe already
// suspect keeps the SuspectFloor and DeadTimeout it turned suspect under.
// Nothing is sent.
func (p *Pipe) SetLiveness(l PipeLiveness) error {
	cl, err := l.core()
	if err != nil {
		return err
	}
	return p.set(core.PipeSettings{Liveness: &cl})
}

// ClearLiveness makes the pipe follow the session's liveness settings.
func (p *Pipe) ClearLiveness() error { return p.SetLiveness(PipeLiveness{}) }

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
