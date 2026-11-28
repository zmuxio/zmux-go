package core

// replicaBudget is the shared outgoing allowance for STREAM and DATAGRAM
// payload. The oldest partial second may contain extras, but cannot earn
// credit. Originals earn space; extras spend it once.
type replicaBudget struct {
	buckets [11]replicaBucket
	burst   uint64
	ready   bool
}

type replicaBucket struct {
	second          Time
	original, extra uint64
}

func (s *Session) replicaBytes(n uint64, extra bool) {
	if s.mp == nil || n == 0 {
		return
	}
	sec := s.replicaSecond()
	r := &s.mp.replicaBudget
	b := &r.buckets[sec%Time(len(r.buckets))]
	if b.second != sec {
		*b = replicaBucket{second: sec}
	}
	if extra {
		b.extra = satAdd(b.extra, n)
	} else {
		b.original = satAdd(b.original, n)
	}
}

func (s *Session) replicaRoom() uint64 {
	_, _, room := s.replicaBalance()
	return room
}

func (s *Session) replicaSecond() Time { return Time(s.now.Sub(s.start) / Second) }

func (s *Session) replicaBalance() (original, extra, room uint64) {
	sec := s.replicaSecond()
	r := &s.mp.replicaBudget
	if !r.ready && s.peerSeen {
		r.ready = true
		frameCap := s.cfg.DatagramFrameCap
		if s.peerMaxDgram > 0 {
			frameCap = min(frameCap, s.peerMaxDgram)
		}
		header := uint64(1) // DATAGRAM type
		if s.feat.DatagramFamily {
			header += 2 // shortest sequence and session Flow
		}
		r.burst = frameCap - min(frameCap, header)
	}
	for _, b := range r.buckets {
		if b.second >= sec-10 {
			extra = satAdd(extra, b.extra)
			if b.second > sec-10 {
				original = satAdd(original, b.original)
			}
		}
	}
	if p := uint64(s.cfg.ReplicaPercent); p != 0 {
		allowance := satAdd(original/100*p+original%100*p/100, r.burst)
		room = allowance - min(allowance, extra)
	}
	return original, extra, room
}

// replicaTier is the next ordinary tier after the original's.
func (s *Session) replicaTier(first uint64) uint64 {
	next := ^uint64(0)
	for _, p := range s.pipes {
		if p.mp != nil && s.pipeUsable(p, false) {
			tier, standby := p.mp.effPrio()
			if !standby && tier > first && tier < next {
				next = tier
			}
		}
	}
	return next
}

func (s *Session) replicaEligible(original, p *Pipe, next uint64, independent bool) bool {
	if p == original || p.mp == nil || !s.pipeUsable(p, false) {
		return false
	}
	tier, standby := p.mp.effPrio()
	first, _ := original.mp.effPrio()
	if standby {
		if !s.cfg.ReplicaStandby {
			return false
		}
	} else if tier != first && tier != next {
		return false
	}
	return !independent || independentCarriers(original, p)
}

func independentCarriers(a, b *Pipe) bool {
	if a.mp.fate != 0 && a.mp.fate == b.mp.fate && a.mp.joinerLocal == b.mp.joinerLocal {
		return false
	}
	if a.out == nil || b.out == nil {
		return false
	}
	left, right := a.out.lane(0), b.out.lane(0)
	if left == nil || right == nil {
		return false
	}
	x, y := left.info, right.info
	if x.CarrierIdentity != nil && x.CarrierIdentity == y.CarrierIdentity {
		return false
	}
	return x.FateDomain != "" && y.FateDomain != "" && x.FateDomain != y.FateDomain
}
