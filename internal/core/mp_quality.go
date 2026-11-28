package core

// PathQualityReason is evidence for an advisory on an outbound direction.
// The advisory is independent of the wire protocol and does not own pipe state.
type PathQualityReason uint8

const (
	PathQualityUnknown PathQualityReason = iota + 1
	PathQualityHealthySibling
	PathQualityLocalStall
	PathQualityShared
	PathQualityRestored
)

const pathQualityInterval = Second

func (s *Session) qualityRTT(p *Pipe) (rtt, reference Duration, fresh bool) {
	q := p.mp.quality
	if q != nil {
		reference = q.reference
		if reference == 0 {
			var known bool
			reference, known = q.floor(s.now)
			if !known {
				reference = 0
			}
		}
		fresh = q.freshRTT(s.now)
	}
	return p.mp.currentRTT(s.now), reference, fresh
}

func (s *Session) qualityElevated(p *Pipe) bool {
	rtt, ref, fresh := s.qualityRTT(p)
	return fresh && ref > 0 && rtt > 2*ref+50*Millisecond
}

func (s *Session) qualityLocalStall(p *Pipe) bool {
	if since, stalled := s.writeStall(p); stalled && s.now >= since.Add(s.tSuspect(p.mp)) {
		return true
	}
	n, known := s.carrierUnsent(p)
	return known && n >= notsentLowat && float64(n) > s.recentRate(p)
}

func (s *Session) qualityBad(p *Pipe) bool {
	a := p.mp
	if s.qualityElevated(p) || s.qualityLocalStall(p) {
		return true
	}
	if a.unacked && s.hasLiveInbound() && s.now >= a.tref().Add(s.tSuspect(a)) {
		return true
	}
	e := &p.out.snd.re
	q := a.quality
	return q != nil && q.freshRate(s.now) && 4*q.rate < e.value() &&
		p.out.snd.so-p.out.snd.cum >= notsentLowat && 4*q.rate < e.freshPeak(s.now)
}

func qualityPeer(p *Pipe) bool {
	return p.mp != nil && !p.mp.retired && p.mp.peerAttached(p.id) && p.out != nil && !p.out.failed && (p.mp.out == OutActive || p.mp.out == OutSuspect)
}

// qualityReason compares the directions in the existing FateGroup namespace.
// A stale peer is not evidence that its path is healthy.
func (s *Session) qualityReason(p *Pipe) PathQualityReason {
	a := p.mp
	siblings, bad := 0, 0
	healthy := false
	for _, q := range s.pipes {
		if p == q || !qualityPeer(q) || a.fate == 0 ||
			q.mp.fate != a.fate || q.mp.joinerLocal != a.joinerLocal {
			continue
		}
		siblings++
		if s.qualityBad(q) {
			bad++
		} else if q.mp.quality != nil && q.mp.quality.freshRTT(s.now) && q.mp.quality.rttAt >= a.quality.since && q.mp.out == OutActive {
			healthy = true
		}
	}
	switch {
	case healthy:
		return PathQualityHealthySibling
	case siblings > 0 && siblings == bad:
		return PathQualityShared
	case siblings == 0 && a.quality.freshRTT(s.now) && !s.qualityElevated(p) && s.qualityLocalStall(p):
		for _, q := range s.pipes {
			if p != q && qualityPeer(q) && s.qualityElevated(q) {
				return PathQualityUnknown
			}
		}
		return PathQualityLocalStall
	default:
		return PathQualityUnknown
	}
}

func (s *Session) qualityHasDemand(p *Pipe) bool {
	return unackedStream(p) || s.qualityLocalStall(p)
}

// ReplacementAdvice revalidates a previously observed connection episode.
// The episode timestamp prevents delayed events authorizing a later episode.
func (s *Session) ReplacementAdvice(id uint64) (Time, Duration, bool) {
	p := s.pipe(id)
	if p == nil || !qualityPeer(p) || p.mp.quality == nil || !s.cfg.PathAdvice || s.state != StateActive {
		return 0, 0, false
	}
	q := p.mp.quality
	reason := q.advice
	if (reason != PathQualityHealthySibling && reason != PathQualityLocalStall) ||
		q.recoverAt != 0 || !s.qualityHasDemand(p) || !s.qualityBad(p) {
		return 0, 0, false
	}
	return q.since, s.qualityBackoff(p), true
}

// ReplacementEpisode reports whether an already authorized replacement
// still belongs to the old direction's active degradation episode. Joining
// a replacement itself can change the set of siblings and their load.
func (s *Session) ReplacementEpisode(id uint64, episode Time) bool {
	p := s.pipe(id)
	if p == nil || !qualityPeer(p) || p.mp.quality == nil || !s.cfg.PathAdvice || s.state != StateActive {
		return false
	}
	return episode != 0 && p.mp.quality.since == episode
}

func (s *Session) qualityBackoff(p *Pipe) Duration {
	return minD(60*Second, MulD(maxD(Second, s.backoffBase(p)), 1<<min(p.mp.suspects, 6)))
}

func (s *Session) reportPathQuality(p *Pipe, reason PathQualityReason) {
	q := p.mp.quality
	if reason == q.advice {
		return
	}
	q.advice = reason
	q.generation++
	s.NotifyCapacity()
	var retry Duration
	if reason == PathQualityHealthySibling || reason == PathQualityLocalStall {
		retry = s.qualityBackoff(p)
	}
	unsent, _ := s.carrierUnsent(p)
	s.emit(Event{Kind: EvPipePathQuality, Pipe: p.id, Code: p.mp.fate, Count: unsent,
		RTT: p.mp.currentRTT(s.now), Rate: s.recentRate(p), Wait: retry, PathQuality: reason})
}

// observePathQuality confirms a new classification in two consecutive
// timer observations. Once published, that reason is authoritative until
// another classification is confirmed.
func (s *Session) observePathQuality(p *Pipe, reason PathQualityReason) {
	q := p.mp.quality
	switch reason {
	case q.advice:
		q.pending = 0
	case q.pending:
		q.pending = 0
		s.reportPathQuality(p, reason)
	default:
		q.pending = reason
	}
}

// pathQualityTimers observes changes even when there are no placement calls.
// ACKs and carrier telemetry fill the existing measurements; PIPE_PING keeps
// idle observations fresh under the existing probe limiter.
func (s *Session) pathQualityTimers(now Time, apply bool) Time {
	if !s.feat.Multipath || s.state != StateActive && s.state != StateDraining {
		return Never
	}
	var refreshAt *Time
	switch {
	case len(s.pipes) > 1 && s.mp.additionalPipes != nil:
		refreshAt = &s.mp.qualityAt
	case s.cfg.PathAdvice && len(s.pipes) > 0 && s.pipes[0].mp.quality != nil:
		refreshAt = &s.pipes[0].mp.quality.refreshAt
	default:
		return Never
	}
	next := *refreshAt
	if next == 0 {
		next = s.start.Add(pathQualityInterval)
	}
	if !apply || now < next {
		return next
	}
	*refreshAt = now.Add(pathQualityInterval)
	for _, p := range s.pipes {
		if !s.cfg.PathAdvice || !qualityPeer(p) || p.mp.quality == nil {
			continue
		}
		q := p.mp.quality
		if !s.qualityBad(p) {
			q.pending = 0
			if q.advice == 0 {
				q.since, q.reference = 0, 0
				continue
			}
			rtt, ref, fresh := s.qualityRTT(p)
			if !fresh || ref != 0 && rtt > ref+ref/2+25*Millisecond {
				q.recoverAt = 0
				continue
			}
			if q.recoverAt == 0 {
				q.recoverAt = now
			} else if now >= q.recoverAt.Add(pathQualityInterval) && q.rttAt > q.recoverAt {
				s.reportPathQuality(p, PathQualityRestored)
				q.pathAdvice = pathAdvice{}
			}
			continue
		}
		q.recoverAt = 0
		if q.since == 0 {
			_, ref, _ := s.qualityRTT(p)
			q.since, q.reference = now, ref
		}
		reason := s.qualityReason(p)
		if (reason == PathQualityHealthySibling || reason == PathQualityLocalStall) && !s.qualityHasDemand(p) {
			reason = PathQualityUnknown
		}
		s.observePathQuality(p, reason)
	}
	if s.sched.mask != 0 {
		s.wakeAllWriters() // wake queued data as the age of its estimates changes
	}
	return *refreshAt
}
