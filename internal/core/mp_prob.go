package core

// Probation, its history and revalidation.

const (
	probationFloor = Second
	probationProbe = 200 * Millisecond
	probMaxK       = 6
	probRecent     = 10 // bases after a probation within which trouble raises k
)

// probState is the probation of one outbound pipe.
type probState struct {
	start     Time
	k         int
	ping      Time
	upSince   Time
	on        bool
	pinged    bool
	upCounted bool // the current ACTIVE period counts for the uptime cap
}

// history is the probation history of a path: a pipe of fate group 0, or a
// non-zero fate group of a joiner in one direction.
type history struct {
	k       int
	endAt   Time
	endBase Duration
	uMax    Duration
	ended   bool
	pending bool // trouble in or soon after a probation: the next one raises k
	trouble bool // the path has been suspect or failed
	hasUMax bool
}

type fateKey struct {
	local bool
	group uint64
}

func (h *history) recent(at Time) bool {
	return h.ended && at <= h.endAt.Add(MulD(probRecent, h.endBase))
}

func (h *history) troubleAt(inProb, upCounted bool, upSince, at Time) {
	if upCounted {
		up := at.Sub(upSince)
		if !h.hasUMax || up > h.uMax {
			h.uMax, h.hasUMax = up, true
		}
	}
	h.trouble = true
	if inProb || h.recent(at) {
		h.pending = true
	}
}

func (h *history) nextK() int {
	if h.pending {
		h.pending = false
		h.k = min(h.k+1, probMaxK)
	}
	return h.k
}

func (h *history) decay(quiet bool, now Time) {
	if quiet && !h.pending && (h.k > 0 || h.hasUMax) && h.ended && !h.recent(now) {
		h.k = 0
		h.hasUMax = false
	}
}

// hist returns the probation history of p's path.
func (s *Session) hist(p *Pipe) *history {
	a := p.mp
	if a.fate == 0 {
		return &a.hist
	}
	m := s.mp
	key := fateKey{a.joinerLocal, a.fate}
	h := m.fateHist[key]
	if h == nil {
		if m.fateHist == nil {
			m.fateHist = map[fateKey]*history{}
		}
		h = &history{}
		m.fateHist[key] = h
	}
	return h
}

// probationBase is the base now; <0 when probation is disabled.
func (s *Session) probationBase(p *Pipe) Duration {
	a := p.mp
	opt := a.probOpt
	if opt < 0 {
		opt = s.cfg.Probation
	}
	switch {
	case opt == 0:
		return -1
	case opt > 0:
		return opt
	}
	srtt4 := 4 * a.rtt.smoothed
	if s.hist(p).trouble {
		return maxD(probationFloor, srtt4)
	}
	return srtt4
}

// backoffBase is the base of the 10 x base windows: always with the floor.
func (s *Session) backoffBase(p *Pipe) Duration {
	a := p.mp
	opt := a.probOpt
	if opt < 0 {
		opt = s.cfg.Probation
	}
	if opt > 0 {
		return opt
	}
	return maxD(probationFloor, 4*a.rtt.smoothed)
}

// probationEnd is start + min(base x 2^k, max(base, 2 x U_max)).
func (s *Session) probationEnd(p *Pipe) (Time, bool) {
	a := p.mp
	if !a.prob.on {
		return 0, false
	}
	base := s.probationBase(p)
	if base < 0 {
		return 0, false
	}
	l := base
	for i := 0; i < a.prob.k && l < Duration(Never)/4; i++ {
		l *= 2
	}
	if h := s.hist(p); h.hasUMax {
		l = minD(l, maxD(base, 2*h.uMax))
	}
	return a.prob.start.Add(l), true
}

// startProbation: the pipe joined, or returned from SUSPECT to ACTIVE.
func (s *Session) startProbation(p *Pipe) {
	a := p.mp
	if p.out == nil || p.id == 0 && !a.wasSuspect {
		return
	}
	h := s.hist(p)
	if !a.prob.on && a.out != OutSuspect {
		h.decay(true, s.now)
	}
	k := h.nextK()
	a.prob = probState{on: s.probationBase(p) >= 0, start: s.now, k: k,
		upCounted: h.trouble, upSince: s.now}
	if a.prob.on && a.liveOut() {
		// The first probe goes at once.
		a.prob.ping, a.prob.pinged = s.now, true
		if !a.needPing {
			a.needPing = true
			a.pingSince = s.now
			s.armPathDeadline(p, pathProbe)
		}
		s.wakePipe(p)
	}
	s.armPathDeadline(p, pathProbationEnd)
	s.armPathDeadline(p, pathProbationProbe)
	s.capacityChanged()
}

// probationSuspect: the pipe became suspect.
func (s *Session) probationSuspect(p *Pipe) {
	a := p.mp
	a.wasSuspect = true
	inProb := a.prob.on
	a.prob.on = false
	s.syncPipeHealth(p)
	counted, since := a.prob.upCounted, a.prob.upSince
	a.prob.upCounted = false
	if a.draining || a.resLimited {
		return
	}
	s.hist(p).troubleAt(inProb, counted, since, s.now)
}

// probationFailed: a failure counts like a suspicion in a fate group's
// history, so that a rejoin on the same path starts with the raised backoff.
func (s *Session) probationFailed(p *Pipe) {
	a := p.mp
	if a.fate == 0 || a.draining || a.resLimited {
		return
	}
	inProb := a.prob.on
	counted, since := a.prob.upCounted, a.prob.upSince
	a.prob.upCounted = false
	s.hist(p).troubleAt(inProb, counted, since, s.now)
}

// ---- revalidation ----

// RevalidatePipes asks for revalidation of one pipe (all == false) or of
// every live outbound pipe. An outbound direction probes with PIPE_PING.
// An explicitly selected receive-only pipe waits for its next valid record.
func (s *Session) RevalidatePipes(id uint64, all bool, now Time) error {
	s.SetNow(now)
	if s.mp == nil {
		return ErrNotNegotiated
	}
	if !all && s.pipe(id) == nil {
		return ErrInvalid
	}
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || (!all && p.id != id) || a.reval {
			continue
		}
		if p.out == nil {
			if !all && a.in == InOpen {
				a.reval = true
				s.syncPipeHealth(p)
			}
			continue
		}
		if a.out != OutActive && a.out != OutJoining {
			continue
		}
		a.reval, a.revalPing = true, true
		s.syncPipeHealth(p)
		if !a.needPing {
			a.needPing = true
			a.pingSince = s.now
			s.armPathDeadline(p, pathProbe)
		}
	}
	s.capacityChanged()
	s.checkTakeover()
	s.wakeAllWriters()
	return nil
}

// revalPingWritten records the end of the batch that carried the PIPE_PING.
func (s *Session) revalPingWritten(p *Pipe) {
	a := p.mp
	a.revalPing = false
	a.revalEnd = p.out.snd.so
}

// pingsAcked counts the PIPE_PINGs on p that the acknowledged position
// now covers as answered.
func (s *Session) pingsAcked(p *Pipe) {
	a := p.mp
	n := 0
	for n < len(a.pingEnds) && a.pingEnds[n] <= p.out.snd.cum {
		n++
	}
	if n > 0 {
		a.pingsAnswered += uint64(n)
		position := a.pingEnds[n-1]
		if a.health.send.running && position > a.health.send.startPosition {
			s.healthProof(p, DirOut)
		}
		a.health.proofAsked = false
		a.pingEnds = append(a.pingEnds[:0], a.pingEnds[n:]...)
	}
}

// revalAcked ends a revalidation whose PIPE_PING is acknowledged.
func (s *Session) revalAcked(p *Pipe) {
	a := p.mp
	if a.reval && !a.revalPing && p.out.snd.cum >= a.revalEnd {
		s.endRevalidation(p)
		// The ACK covers the probe that ended revalidation. The new
		// evidence generation starts at the current sender position.
		s.healthProof(p, DirOut)
	}
}

func (s *Session) endRevalidation(p *Pipe) {
	a := p.mp
	if a == nil || !a.reval {
		return
	}
	a.reval, a.revalPing = false, false
	s.syncPipeHealth(p)
	s.capacityChanged()
	s.checkTakeover()
	s.wakeAllWriters()
}

// ---- backup probes ----

// backup reports a pipe probed every standby_probe while the session is
// busy: standby, or of a less-preferred tier than the most-preferred usable.
func (s *Session) backup(p *Pipe) bool {
	pr, sb := p.mp.effPrio()
	if sb {
		return true
	}
	best, ok := ^uint64(0), false
	for _, q := range s.pipes {
		if q.mp == nil || q.out == nil || !s.pipeUsable(q, true) {
			continue
		}
		if qp, qs := q.mp.effPrio(); !qs && qp < best {
			best, ok = qp, true
		}
	}
	return ok && pr > best
}

// markBusy notes a traffic frame; when the session was quiet, backup pipes
// unprobed for longer than standby_probe get a PIPE_PING at once.
func (s *Session) markBusy() {
	m := s.mp
	quiet := s.now >= m.busyUntil
	m.busyUntil = s.now.Add(15 * Second)
	if !quiet || s.cfg.StandbyProbe <= 0 {
		return
	}
	s.refreshProbeDeadlines()
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || p.out == nil || a.needPing || a.unacked || (a.out != OutActive && a.out != OutSuspect) || !s.backup(p) {
			continue
		}
		ref := maxT(a.lastElicit, a.liveAt)
		if s.now >= ref.Add(s.cfg.StandbyProbe) {
			a.needPing = true
			a.pingSince = s.now
			s.armPathDeadline(p, pathProbe)
			s.wakeAllWriters()
		}
	}
}
