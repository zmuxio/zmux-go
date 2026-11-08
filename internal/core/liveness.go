package core

// Liveness are the session's liveness settings: the keep-alive period
// (0: off) and the failure detection and probes of the path
// layer, with the values of Config.
type Liveness struct {
	KeepAlive     Duration
	SuspectFloor  Duration
	DeadTimeout   Duration // 0: derived
	StandbyProbe  Duration // 0: off
	ProbeInterval Duration // 0: off
	ProbeJitter   float64
}

// PipeLiveness are a pipe's own liveness settings; a zero field is the
// session's, a negative ProbeInterval turns the pipe's idle probes off
// and a negative ProbeJitter is a jitter of 0.
type PipeLiveness struct {
	SuspectFloor  Duration
	DeadTimeout   Duration
	ProbeInterval Duration
	ProbeJitter   float64
}

// Liveness returns c's liveness settings.
func (c *Config) Liveness() Liveness {
	return Liveness{KeepAlive: c.KeepAlive, SuspectFloor: c.SuspectFloor, DeadTimeout: c.DeadTimeout,
		StandbyProbe: c.StandbyProbe, ProbeInterval: c.ProbeInterval, ProbeJitter: c.ProbeJitter}
}

// SetLiveness replaces c's liveness settings.
func (c *Config) SetLiveness(l Liveness) {
	c.KeepAlive, c.SuspectFloor, c.DeadTimeout = l.KeepAlive, l.SuspectFloor, l.DeadTimeout
	c.StandbyProbe, c.ProbeInterval, c.ProbeJitter = l.StandbyProbe, l.ProbeInterval, l.ProbeJitter
}

// Liveness returns the session's liveness settings.
func (s *Session) Liveness() Liveness { return s.cfg.Liveness() }

// SetLiveness replaces the session's liveness settings while it runs.
// Every deadline is computed from them anew (NextTimeout): the keep-alive
// period, still capped at half the idle timeout, the probe interval and
// the suspicion floor apply at once, a jitter from the next draw, and the
// DeadTimeout to pipes that turn suspect from now on; a pipe already
// suspect keeps the SuspectFloor and DeadTimeout it turned suspect under.
// Pipes joined later have the new settings, as every pipe without its own.
func (s *Session) SetLiveness(l Liveness) {
	s.cfg.SetLiveness(l)
	s.wakeAllWriters()
}

// RaiseIncomingStreams raises the concurrency of the peer's streams of one
// type to n: MAX_STREAMS only grows, so an n at or below the
// current concurrency changes nothing. While unidirectional credit is
// staged, n raises the target the first accept moves it to. After GOAWAY
// the concurrency is recorded but no credit is sent. It reports whether
// the concurrency changed.
func (s *Session) RaiseIncomingStreams(bidi bool, n uint64) bool {
	t := typeIndex(bidi)
	rc := &s.rem[t]
	if !bidi && rc.staged {
		if n <= s.cfg.UniTarget {
			return false
		}
		s.cfg.UniTarget = n
		return true
	}
	if n <= rc.initial {
		return false
	}
	rc.initial = n
	if s.ga.sent || s.state >= StateClosing {
		return true
	}
	if nl := min(n+rc.discarded, maxStreamCount); nl > rc.limit {
		rc.limit = nl
		s.pend.maxStreams[t] = true
		s.wakeAllWriters()
	}
	return true
}

// maxStreamCount is the largest MAX_STREAMS value (2^60).
const maxStreamCount = 1 << 60
