package core

// PipeInfo is the snapshot of one attachment that PipeStats and Stats
// report: exact counts and settings, and the sender's RTT, one-way delay
// and delivery-rate estimates.
type PipeInfo struct {
	ID           uint64
	OutState     string
	InState      string
	AttachedFor  Duration
	SuspectCount uint64
	FailureCount uint64

	SendPriority    uint64
	PrioritySource  string // local, peer, default
	StandbySource   string
	ReceivePriority uint64
	StatusSeq       uint64
	PeerPriority    uint64
	Weight          uint64
	ProbationUntil  Time

	SmoothedRTT  Duration
	MinRTT       Duration
	RTTVar       Duration
	OWD          Duration
	DeliveryRate float64 // bytes/s
	RTTSource    string  // ack, kernel, ping

	QueuedBytes   uint64
	BytesInFlight uint64

	BytesSent          uint64
	BytesReceived      uint64
	BytesAcked         uint64
	RecordsSent        uint64
	RecordsReceived    uint64
	BytesReinjected    uint64
	BytesOpportunistic uint64
	PingsSent          uint64

	SinceLastAck     Duration
	SinceLastReceive Duration
	Lanes            int

	HasOut         bool
	HasIn          bool
	Revalidating   bool
	Standby        bool
	ReceiveStandby bool
	PeerRequest    bool // a peer request exists
	PeerStandby    bool
	InProbation    bool
	Eligible       bool
	HasRTT         bool
	Writable       bool
	Saturated      bool
}

// PipeIDs returns the IDs of the session's attachments.
func (s *Session) PipeIDs(dst []uint64) []uint64 {
	for _, p := range s.pipes {
		dst = append(dst, p.id)
	}
	return dst
}

// PipeInfo returns the snapshot of an attachment.
func (s *Session) PipeInfo(id uint64) (PipeInfo, bool) {
	p := s.pipe(id)
	if p == nil {
		return PipeInfo{}, false
	}
	var pi PipeInfo
	pi.ID = id
	pi.HasOut, pi.HasIn = p.out != nil, p.in != nil
	pi.OutState, pi.InState, _ = s.PipeState(id)
	pi.PrioritySource, pi.StandbySource = "default", "default"
	pi.Weight = 1
	if p.out != nil {
		d := p.out
		for _, l := range d.lanes {
			pi.BytesSent += l.pos
		}
		pi.Lanes = len(d.lanes)
		pi.BytesAcked = d.snd.cum
		pi.RecordsSent = d.records
		pi.BytesInFlight = d.snd.so - min64(d.snd.so, d.snd.cum)
		pi.DeliveryRate = d.snd.re.value()
		pi.Writable = !d.lanes[0].notWritable && !d.lanes[0].writing && !s.limHeld(p)
		r := &d.snd.rtt
		if p.mp != nil {
			r = &p.mp.rtt
		}
		if r.has || p.mp != nil {
			pi.SmoothedRTT, pi.MinRTT, pi.RTTVar, pi.HasRTT = r.smoothed, r.min, r.rttvar, r.has
			pi.RTTSource = "ack"
		}
		if s.tn.carrierMin > 0 && p.mp == nil {
			pi.MinRTT, pi.RTTSource = s.tn.carrierMin, "kernel"
		}
		if !pi.HasRTT && p.mp == nil && s.ping.lastRTT > 0 {
			pi.SmoothedRTT, pi.MinRTT, pi.HasRTT, pi.RTTSource = s.ping.lastRTT, s.ping.lastRTT, true, "ping"
		}
	}
	if p.in != nil {
		for _, l := range p.in.lanes {
			pi.BytesReceived += l.pos
			pi.RecordsReceived += l.recs
		}
	}
	a := p.mp
	if a == nil {
		return pi, true
	}
	pi.AttachedFor = s.now.Sub(a.attachedAt)
	pi.Revalidating = a.reval
	pi.SuspectCount, pi.FailureCount = a.suspects, a.failures
	pi.SendPriority, pi.Standby = a.effPrio()
	switch {
	case a.sendPrio != nil:
		pi.PrioritySource = "local"
	case a.peerSeq > 0 || a.peerPrio != 0:
		pi.PrioritySource = "peer"
	}
	switch {
	case a.sendStandby != nil:
		pi.StandbySource = "local"
	case a.peerSeq > 0 || a.peerStandby:
		pi.StandbySource = "peer"
	}
	pi.ReceivePriority, pi.ReceiveStandby, pi.StatusSeq = a.recvPrio, a.recvStandby, a.statusSeq
	pi.PeerRequest, pi.PeerStandby, pi.PeerPriority = a.peerSeq > 0, a.peerStandby, a.peerPrio
	pi.Weight = a.weight
	pi.InProbation = a.prob.on
	if end, ok := s.probationEnd(p); ok {
		pi.ProbationUntil = end
	}
	if p.out != nil {
		pi.OWD = s.owd(p)
		pi.QueuedBytes = a.unsentAhead
		pi.SinceLastAck = s.now.Sub(a.lastAdvance)
		if g := s.buildGate(); g != nil {
			pi.Eligible = g.admits(s, p)
			if ti := g.tierOf(p); ti >= 0 && ti <= g.last {
				pi.Saturated = g.tiers[ti].sat
			}
		}
		pi.BytesReinjected, pi.BytesOpportunistic, pi.PingsSent = a.reinjectedBytes, a.oppBytes, a.pingsSent
	}
	if p.in != nil {
		pi.SinceLastReceive = s.now.Sub(a.lastRecord)
	}
	return pi, true
}
