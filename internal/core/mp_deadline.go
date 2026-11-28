package core

import "github.com/zmuxio/zmux-go/internal/wire"

type pathDeadlineKind uint8

const (
	pathHealth pathDeadlineKind = iota
	pathJoin
	pathStall
	pathDrain
	pathRepairHold
	pathProbe
	pathUnilateral
	pathLinger
	pathSilence
	pathEpochRepeat
	pathProbationEnd
	pathProbationProbe
	pathDeadlineCount
)

type pathDeadline struct {
	pipe *Pipe
	kind pathDeadlineKind
}

type pathClock struct {
	due                      DeadlineQueue[pathDeadline]
	rtts, ptos               DeadlineQueue[*Pipe]
	inOnly, unilateral, ping pathControls
	inbound, activeOut       int
	ackWork                  int
	stallOrder               []*Pipe
	references               []*Pipe
	relativeAt               Time
	relativeDirty            bool
}

// refreshPathTopology runs on membership or direction state changes. Those
// changes can advance another attachment's last-direction deadline.
func (s *Session) refreshPathTopology() {
	if s.mp == nil {
		return
	}
	s.mp.control = controlOrder{}
	s.mp.owdFloorValid = false
	s.mp.invalidateCandidates()
	s.relativeStallChanged()
	c := &s.mp.clock
	if s.workProbe != nil {
		s.workProbe.pathScans++
	}
	c.inbound, c.activeOut = 0, 0
	clear(c.inOnly)
	clear(c.unilateral)
	c.inOnly = c.inOnly[:0]
	c.unilateral = c.unilateral[:0]
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || a.retired {
			continue
		}
		if p.in != nil && (a.in == InOpen || a.in == InJoining) {
			c.inbound++
			if p.out == nil && a.in == InOpen {
				c.inOnly = append(c.inOnly, p)
			}
		}
		if p.out != nil && a.out == OutActive {
			c.activeOut++
		}
		if a.abandonAcked && !a.abandonRecv && !a.unilateral {
			c.unilateral = append(c.unilateral, p)
		}
		s.indexPathRTT(p)
	}
	for _, p := range s.pipes {
		s.refreshPathDeadlines(p)
	}
	s.updateDatagramReportLimit()
}

func (s *Session) indexPathRTT(p *Pipe) {
	a, c := p.mp, &s.mp.clock
	if a.retired || p.out == nil || !a.liveOut() {
		c.rtts.Cancel(&a.rttIndex)
		c.ptos.Cancel(&a.ptoIndex)
		return
	}
	a.rttIndex.Value, a.ptoIndex.Value = p, p
	c.rtts.Set(&a.rttIndex, -Time(a.rtt.smoothed))
	c.ptos.Set(&a.ptoIndex, -Time(s.pto(a)))
}

// pathRTTChanged publishes potentially earlier deadlines on other pipes
// before the acknowledgement transaction becomes visible to the driver.
func (s *Session) pathRTTChanged(p *Pipe, minimumChanged bool) {
	if p.mp == nil {
		return
	}
	s.relativeStallChanged()
	s.wakePrefix()
	if minimumChanged {
		s.mp.control.valid = false
		s.mp.owdFloorValid = false
	}
	s.refreshDatagramReports(p)
	s.armPathDeadline(p, pathProbationEnd)
	s.armPathDeadline(p, pathProbationProbe)
	c := &s.mp.clock
	if len(c.inOnly) == 0 && len(c.unilateral) == 0 {
		return
	}
	oldRTT, oldPTO := c.rtts.Next(), c.ptos.Next()
	s.indexPathRTT(p)
	if oldRTT != c.rtts.Next() || oldPTO != c.ptos.Next() {
		for _, q := range c.inOnly {
			s.armPathDeadline(q, pathSilence)
		}
	}
	if oldPTO != c.ptos.Next() {
		for _, q := range c.unilateral {
			s.armPathDeadline(q, pathUnilateral)
		}
	}
}

func (s *Session) refreshProbeDeadlines() {
	if s.mp == nil {
		return
	}
	for _, p := range s.pipes {
		s.armPathDeadline(p, pathProbe)
	}
}

func (s *Session) refreshPathDeadlines(p *Pipe) {
	if p.mp != nil {
		for kind := range pathDeadlineCount {
			s.armPathDeadline(p, kind)
		}
	}
}

func (s *Session) armPathDeadline(p *Pipe, kind pathDeadlineKind) {
	if s.mp == nil || p.mp == nil {
		return
	}
	if kind == pathStall {
		a, c := p.mp, &s.mp.clock
		watching := s.ackStall(p) && a.out == OutActive
		if a.ackWatching != watching {
			a.ackWatching = watching
			if watching {
				c.ackWork++
			} else {
				c.ackWork--
			}
		}
		s.relativeStallChanged()
	}
	if kind == pathProbe {
		c := &s.mp.clock
		a := p.mp
		if a.needPing && !a.topologyOut && !a.retired && !a.hs && p.out != nil && !p.out.failed {
			c.ping.add(p)
		} else {
			c.ping.remove(p)
		}
	}
	s.indexPathDeadline(p, kind)
}

func (s *Session) indexPathDeadline(p *Pipe, kind pathDeadlineKind) {
	t := &p.mp.deadlines[kind]
	t.Value = pathDeadline{p, kind}
	t.Order = int(kind)
	s.mp.clock.due.Set(t, s.pathDeadlineAt(p, kind))
}

func (s *Session) removePathDeadlines(p *Pipe) {
	c := &s.mp.clock
	c.ping.remove(p)
	if p.mp.ackWatching {
		p.mp.ackWatching = false
		c.ackWork--
	}
	s.relativeStallChanged()
	for i := range p.mp.deadlines {
		c.due.Cancel(&p.mp.deadlines[i])
	}
	c.rtts.Cancel(&p.mp.rttIndex)
	c.ptos.Cancel(&p.mp.ptoIndex)
}

// TightenJoinDeadline advances a pending local Join's confirmation fence.
func (s *Session) TightenJoinDeadline(id uint64, deadline Time) {
	if p := s.pipe(id); p != nil && p.mp != nil && p.mp.joinerLocal &&
		p.mp.peer == peerPending && deadline < p.mp.joinDeadline {
		p.mp.joinDeadline = deadline
		s.armPathDeadline(p, pathJoin)
	}
}

func (s *Session) pathDeadlineAt(p *Pipe, kind pathDeadlineKind) Time {
	a, c := p.mp, &s.mp.clock
	if a.retired || s.state == StateClosed {
		return Never
	}
	switch kind {
	case pathHealth:
		return s.healthDeadline(p)
	case pathJoin:
		if a.joinerLocal && a.peer == peerPending && a.joinDeadline != 0 {
			return a.joinDeadline
		}
	case pathStall:
		if p.out == nil || a.topologyOut || !a.liveOut() {
			return Never
		}
		if a.out == OutActive {
			return s.activeStallDeadline(p)
		}
		ws, writing := s.writeStall(p)
		hasAck := s.ackStall(p)
		if !hasAck && !writing {
			return Never
		}
		start := a.tref()
		if writing && (!hasAck || ws < start) {
			start = ws
		}
		switch a.out {
		case OutActive:
			return Never
		case OutSuspect:
			if s.stallDeathOff(a) {
				return Never
			}
			if c.activeOut > 0 {
				return start.Add(s.suspectFail(a))
			}
			return start.Add(s.tDead(a))
		case OutDraining:
			if !a.drainSuspect {
				return start.Add(s.tSuspect(a))
			}
		case OutJoining, OutNone, OutFailed, OutClosed, OutRetired:
		}
	case pathDrain:
		if a.out == OutDraining {
			return a.drainStart.Add(s.tFail(a))
		}
	case pathRepairHold:
		if a.held1 {
			return a.holdUntil
		}
	case pathProbe:
		if p.out == nil || a.topologyOut || a.out != OutActive && a.out != OutSuspect || a.unacked || a.needPing {
			return Never
		}
		wait := s.probeWait(p)
		if sp := s.cfg.StandbyProbe; sp > 0 && s.now < s.mp.busyUntil && s.backup(p) && (wait == 0 || sp < wait) {
			wait = sp
		}
		if wait > 0 {
			return max(a.liveAt, a.lastElicit).Add(wait)
		}
	case pathUnilateral:
		if a.abandonAcked && !a.abandonRecv && !a.unilateral {
			return a.ackedAbandonAt.Add(maxD(2*Second, 3*s.maxPTO()))
		}
	case pathLinger:
		if a.lingering && a.in == InOpen {
			return a.lingerUntil
		}
	case pathSilence:
		if p.out == nil && !a.topologyIn && a.in == InOpen {
			lim := 15*Second + Duration(c.inbound)*Second/8 +
				maxD(2*Second, maxD(2*s.srttMaxOut(), a.maxGap-15*Second))
			if c.inbound <= 1 {
				lim = maxD(lim, s.lastInDead(a))
			}
			return a.lastRecord.Add(lim)
		}
	case pathEpochRepeat:
		if !s.isClosing() && s.mp.ep.frame != nil && a.epochSent && epochPipe(p) {
			return a.epochAt
		}
	case pathProbationEnd, pathProbationProbe:
		if p.out == nil {
			return Never
		}
		end, ok := s.probationEnd(p)
		if !ok {
			return Never
		}
		if kind == pathProbationEnd {
			return end
		}
		if end > s.now && a.liveOut() {
			if !a.prob.pinged {
				return s.now
			}
			return a.prob.ping.Add(maxD(a.rtt.smoothed, probationProbe))
		}
	case pathDeadlineCount:
	}
	return Never
}

func (s *Session) expirePathDeadlines(now Time) {
	q := &s.mp.clock.due
	for {
		s.refreshRelativeStalls(now)
		t := q.PopDue(now)
		if t == nil {
			return
		}
		if t.Value.kind == pathStall && t.Value.pipe.mp.out == OutActive {
			// The heap orders wakeups by time. At a cut, commit due active
			// stalls along the same cost order used to resolve references.
			for _, path := range s.mp.clock.stallOrder {
				candidate := &path.mp.deadlines[pathStall]
				if candidate == t {
					break
				}
				if candidate.index != 0 && candidate.at <= now {
					q.Set(t, t.at)
					q.Cancel(candidate)
					t = candidate
					break
				}
			}
		}
		p, kind := t.Value.pipe, t.Value.kind
		at := s.pathDeadlineAt(p, kind)
		if at != Never && at <= now {
			out := p.mp.out
			s.firePathDeadline(p, kind)
			if at == s.pathDeadlineAt(p, kind) && out == p.mp.out {
				panic("zmux: path deadline did not advance")
			}
		}
		s.indexPathDeadline(p, kind)
	}
}

func (s *Session) firePathDeadline(p *Pipe, kind pathDeadlineKind) {
	a := p.mp
	switch kind {
	case pathHealth:
		s.syncPipeHealth(p)
	case pathJoin:
		s.abortUnconfirmed(p, ErrJoinTimeout)
	case pathStall:
		switch a.out {
		case OutActive:
			a.health.reason = HealthAckStall
			if s.writeStallDeadline(p) < s.ackStallDeadline(p) {
				a.health.reason = HealthWriteStall
			}
			s.makeSuspect(p)
		case OutSuspect:
			s.failPipe(p, wire.PipeFailed, nil)
		case OutDraining:
			a.drainSuspect = true
			s.mp.control.valid = false
			s.reinjectPipe(p)
		case OutJoining, OutNone, OutFailed, OutClosed, OutRetired:
		}
	case pathDrain:
		s.failPipe(p, wire.PipeFailed, nil)
	case pathRepairHold:
		a.held1 = false
		s.wakePrefix()
		s.wakeAllWriters()
	case pathProbe:
		a.needPing, a.pingSince = true, s.now
		s.wakePipe(p)
	case pathUnilateral:
		a.unilateral = true
		s.act(Action{Kind: ActAbort, Pipe: p.id, Dir: DirBidi})
		if p.out != nil && a.out != OutFailed {
			a.out = OutClosed
		}
		if p.in != nil {
			a.in = InEnded
		}
		s.maybeRetire(p)
		s.refreshPathTopology()
	case pathLinger:
		a.in = InEnded
		s.act(Action{Kind: ActClose, Pipe: p.id, Dir: DirIn})
		s.maybeRetire(p)
		s.refreshPathTopology()
	case pathSilence:
		a.health.reason = HealthInboundSilence
		s.failPipe(p, wire.PipeFailed, nil)
	case pathEpochRepeat:
		a.epochSent = false
		s.wakePipe(p)
	case pathProbationEnd:
		end, _ := s.probationEnd(p)
		a.prob.on = false
		s.armPathDeadline(p, pathProbationProbe)
		s.syncPipeHealth(p)
		s.wakePrefix()
		h := s.hist(p)
		h.ended, h.endAt, h.endBase = true, end, s.backoffBase(p)
		s.checkTakeover()
		s.wakeAllWriters()
		s.capacityChanged()
	case pathProbationProbe:
		a.prob.ping, a.prob.pinged = s.now, true
		if !a.needPing {
			a.needPing, a.pingSince = true, s.now
			s.armPathDeadline(p, pathProbe)
		}
		s.wakePipe(p)
	case pathDeadlineCount:
	}
}
