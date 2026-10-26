package core

// Probation, its history and revalidation (SCHEDULER.md 11, 11.1).

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

func (h *history) troubleAt(inProb bool, upCounted bool, upSince, at Time) {
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
	k := h.nextK()
	a.prob = probState{on: s.probationBase(p) >= 0, start: s.now, k: k,
		upCounted: h.trouble, upSince: s.now}
	if a.prob.on && a.liveOut() {
		// The first probe goes at once.
		a.prob.ping, a.prob.pinged = s.now, true
		if !a.needPing {
			a.needPing = true
			a.pingSince = s.now
		}
		s.wakePipe(p)
	}
}

// probationSuspect: the pipe became suspect.
func (s *Session) probationSuspect(p *Pipe) {
	a := p.mp
	a.wasSuspect = true
	inProb := a.prob.on
	a.prob.on = false
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

// probationTimers ends probations that are due, decays histories and asks
// for the probes probation needs; it returns the next deadline.
func (s *Session) probationTimers(now Time, apply bool) Time {
	next := Never
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || p.out == nil || a.retired {
			continue
		}
		if end, ok := s.probationEnd(p); ok {
			if now >= end {
				if apply {
					a.prob.on = false
					h := s.hist(p)
					h.ended, h.endAt, h.endBase = true, end, s.backoffBase(p)
					s.checkTakeover()
					s.wakeAllWriters()
				}
			} else {
				next = minT(next, end)
				// Probes at least every max(srtt, 200 ms).
				every := maxD(a.rtt.smoothed, probationProbe)
				due := a.prob.ping.Add(every)
				if !a.prob.pinged || now >= due {
					if apply && a.liveOut() {
						a.prob.ping, a.prob.pinged = now, true
						if !a.needPing {
							a.needPing = true
							a.pingSince = now
						}
						s.wakeAllWriters()
						next = minT(next, now.Add(every))
					}
				} else {
					next = minT(next, due)
				}
			}
		}
		if a.fate == 0 && apply {
			busy := a.prob.on || a.out == outSuspect
			a.hist.decay(!busy, now)
		}
	}
	if apply {
		for key, h := range s.mp.fateHist {
			busy := false
			used := false
			for _, p := range s.pipes {
				a := p.mp
				if a != nil && a.fate == key.group && a.joinerLocal == key.local && a.out != outFailed {
					used = true
					if a.prob.on || a.out == outSuspect {
						busy = true
					}
				}
			}
			h.decay(!busy, now)
			if !used && h.k == 0 && !h.pending && !h.trouble && !h.recent(now) {
				delete(s.mp.fateHist, key)
			}
		}
	}
	return next
}

// ---- revalidation (SCHEDULER.md 11.1) ----

// RevalidatePipes asks for revalidation of one pipe (all == false) or of
// every live outbound pipe: each sends a PIPE_PING at once and awaits
// revalidation until its acknowledged position reaches it.
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
		if a == nil || p.out == nil || (!all && p.id != id) || a.reval {
			continue
		}
		if a.out != outActive && a.out != outJoining {
			continue
		}
		a.reval, a.revalPing = true, true
		if !a.needPing {
			a.needPing = true
			a.pingSince = s.now
		}
	}
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

// revalAcked ends a revalidation whose PIPE_PING is acknowledged.
func (s *Session) revalAcked(p *Pipe) {
	a := p.mp
	if a.reval && !a.revalPing && p.out.snd.cum >= a.revalEnd {
		s.endRevalidation(p)
	}
}

func (s *Session) endRevalidation(p *Pipe) {
	a := p.mp
	if a == nil || !a.reval {
		return
	}
	a.reval, a.revalPing = false, false
	s.checkTakeover()
	s.wakeAllWriters()
}

// ---- backup probes (MULTIPATH.md 7.1, SCHEDULER.md 10 T4) ----

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
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || p.out == nil || a.needPing || a.unacked || (a.out != outActive && a.out != outSuspect) || !s.backup(p) {
			continue
		}
		ref := maxT(a.lastElicit, a.liveAt)
		if s.now >= ref.Add(s.cfg.StandbyProbe) {
			a.needPing = true
			a.pingSince = s.now
			s.wakeAllWriters()
		}
	}
}
