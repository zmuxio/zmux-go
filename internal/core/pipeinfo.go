package core

import (
	"cmp"
	"slices"
)

// PipeInfo is the snapshot of one attachment that PipeStats and Stats
// report: exact counts and settings, and the sender's RTT, one-way delay
// and delivery-rate estimates.
type PipeInfo struct {
	ID             uint64
	Carrier        uint8 // SetPipeKind
	OutState       OutState
	InState        InState
	Local          bool // this endpoint joined the attachment
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
	AttachedFor    Duration
	SuspectCount   uint64
	FailureCount   uint64

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
	LatestRTT    Duration
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
	PingsAnswered      uint64
	BytesRedundant     uint64
	BytesDuplicateRecv uint64
	// ProbeInterval and ProbeJitter are the effective k and j of the
	// outbound direction's idle probes (MULTIPATH.md 7.1).
	ProbeInterval Duration
	ProbeJitter   float64

	SinceLastAck     Duration
	SinceLastReceive Duration
	SinceLastTraffic Duration
	LanesOut         int
	LanesIn          int
	LanesRefused     int
	// Lanes are the positions of each lane, for a pipe with lanes (nil
	// without: lane 0 is then the pipe).
	Lanes []LaneInfo

	FateGroup uint64
	Failure   error // why the attachment failed, once it did
}

// LaneInfo is the positions of one lane of a pipe; a direction the lane
// does not have reports 0.
type LaneInfo struct {
	Lane                        uint64
	PositionSent, PositionAcked uint64
	PositionReceived            uint64
	HasOut, HasIn               bool
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
	pi.Carrier = p.carrier
	pi.HasOut, pi.HasIn = p.out != nil, p.in != nil
	pi.SinceLastTraffic = s.sinceTraffic(p)
	pi.OutState, pi.InState, _ = s.PipeState(id)
	pi.PrioritySource, pi.StandbySource = "default", "default"
	pi.Weight = 1
	if p.out != nil {
		d := p.out
		for _, l := range d.lanes {
			pi.BytesSent += l.pos
		}
		pi.LanesOut = len(d.lanes)
		for _, l := range d.lanes {
			if l.refused {
				pi.LanesRefused++
			}
		}
		pi.BytesAcked = d.snd.cum
		pi.RecordsSent = d.records
		pi.BytesInFlight = d.snd.so - min64(d.snd.so, d.snd.cum)
		pi.DeliveryRate = d.snd.re.value()
		pi.QueuedBytes, _ = s.carrierUnsent(p)
		pi.Writable = !d.lanes[0].notWritable && !d.lanes[0].writing && !s.limHeld(p)
		if _, ok := p.srtt(); ok {
			r := p.rtt()
			pi.SmoothedRTT, pi.MinRTT, pi.RTTVar, pi.LatestRTT, pi.HasRTT = r.smoothed, r.min, r.rttvar, r.latest, r.has
			pi.RTTSource = "ack"
		}
		if s.tn.carrierMin > 0 && p.mp == nil {
			pi.MinRTT, pi.RTTSource = s.tn.carrierMin, "kernel"
		}
		if !pi.HasRTT && p.mp == nil && s.ping.lastRTT > 0 {
			pi.SmoothedRTT, pi.MinRTT, pi.LatestRTT, pi.HasRTT, pi.RTTSource = s.ping.lastRTT, s.ping.lastRTT, s.ping.lastRTT, true, "ping"
		}
	}
	if p.in != nil {
		pi.LanesIn = len(p.in.lanes)
		for _, l := range p.in.lanes {
			pi.BytesReceived += l.pos
			pi.RecordsReceived += l.recs
		}
	}
	pi.Lanes = laneInfos(p)
	a := p.mp
	if a == nil {
		pi.Local = s.role == RoleInitiator
		return pi, true
	}
	pi.Local, pi.FateGroup, pi.Failure = a.joinerLocal, a.fate, a.failCause
	pi.BytesRedundant, pi.BytesDuplicateRecv, pi.PingsAnswered = a.redundantBytes, a.dupBytes, a.pingsAnswered
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
		pi.SinceLastAck = s.now.Sub(a.lastAdvance)
		if g := s.buildGate(); g != nil {
			pi.Eligible = g.admits(s, p)
			if ti := g.tierOf(p); ti >= 0 && ti <= g.last {
				pi.Saturated = g.tiers[ti].sat
			}
		}
		pi.BytesReinjected, pi.BytesOpportunistic, pi.PingsSent = a.reinjectedBytes, a.oppBytes, a.pingsSent
		pi.ProbeInterval, pi.ProbeJitter = s.probeInterval(p), s.probeJitter(a)
	}
	if p.in != nil {
		pi.SinceLastReceive = s.now.Sub(a.lastRecord)
	}
	return pi, true
}

// laneInfos returns the positions of p's lanes, nil for a pipe without
// lanes (lane 0 alone and none offered).
func laneInfos(p *Pipe) []LaneInfo {
	offered := p.out != nil && (p.out.lo.offered || len(p.out.lanes) > 1) || p.in != nil && len(p.in.lanes) > 1
	if !offered {
		return nil
	}
	var ls []LaneInfo
	at := func(lane uint64) *LaneInfo {
		for i := range ls {
			if ls[i].Lane == lane {
				return &ls[i]
			}
		}
		ls = append(ls, LaneInfo{Lane: lane})
		return &ls[len(ls)-1]
	}
	if p.out != nil {
		for _, l := range p.out.lanes {
			li := at(l.lane)
			li.HasOut, li.PositionSent, li.PositionAcked = true, l.pos, l.acked
		}
	}
	if p.in != nil {
		for _, l := range p.in.lanes {
			li := at(l.lane)
			li.HasIn, li.PositionReceived = true, l.pos
		}
	}
	slices.SortFunc(ls, func(a, b LaneInfo) int { return cmp.Compare(a.Lane, b.Lane) })
	return ls
}

// SessionGauges are the current values of SessionStats that are not
// counts (API.md 7.2), taken in one pass under the lock.
type SessionGauges struct {
	StreamsActive uint64
	SendBuffered  uint64 // written stream bytes not yet sent
	RecvBuffered  uint64 // received stream bytes not yet read
	Retained      uint64 // sent stream bytes not yet acknowledged (multipath)
	PipesActive   uint64 // ACTIVE outbound pipes
	// The RTT of pipe 0 outside multipath, of the primary pipe in a
	// multipath session; HasRTT is false before the first sample.
	SmoothedRTT, MinRTT, LatestRTT, MeanDeviation Duration
	HasRTT                                        bool
}

// Gauges returns the session's gauges.
func (s *Session) Gauges() SessionGauges {
	var g SessionGauges
	g.StreamsActive = uint64(len(s.streams))
	for _, st := range s.streams {
		g.SendBuffered += st.unsentPayload()
		g.RecvBuffered += st.rcv.buf.bytes
	}
	for _, st := range s.provisional {
		g.SendBuffered += st.unsentPayload()
	}
	src := s.pipe(0)
	if s.mp != nil {
		g.Retained = s.mp.retained
		src, _ = s.primary()
		for _, p := range s.pipes {
			if p.mp != nil && p.out != nil && p.mp.out == OutActive {
				g.PipesActive++
			}
		}
	} else if p := s.pipe(0); p != nil && p.out != nil && !p.out.failed && s.state < StateClosed {
		g.PipesActive = 1
	}
	if src != nil {
		if pi, ok := s.PipeInfo(src.id); ok && pi.HasRTT {
			g.HasRTT = true
			g.SmoothedRTT, g.MinRTT, g.LatestRTT, g.MeanDeviation = pi.SmoothedRTT, pi.MinRTT, pi.LatestRTT, pi.RTTVar
		}
	}
	return g
}

// unsentPayload is the written payload of st's sending part not yet sent.
func (st *Stream) unsentPayload() uint64 {
	if !st.hasS {
		return 0
	}
	sp := &st.snd
	from := max64(sp.sent, sp.preLen)
	if end := sp.sendEnd(); end > from {
		return end - from
	}
	return 0
}

// CountCreditWait counts an open that waits for stream-count credit.
func (s *Session) CountCreditWait() { s.stats.StreamCreditWaits++ }

// StreamInfo is the snapshot of one stream that StreamStats reports, in
// payload bytes (API.md 7.2). A released stream keeps its final values.
type StreamInfo struct {
	BytesWritten, BytesSent, BytesAcknowledged uint64
	BytesReceived, BytesRead                   uint64
	DatagramsSent, DatagramsReceived           uint64
	DatagramsDroppedRecv                       uint64
	DatagramBytesSent, DatagramBytesReceived   uint64
	SendBuffered, RecvBuffered, RecvWindow     uint64
	Pipe                                       uint64 // the sending direction's pipe under affinity
	PipeBound                                  bool   // Pipe is set: the sending direction is bound
}

// StreamInfo returns st's snapshot.
func (s *Session) StreamInfo(st *Stream) StreamInfo {
	c := &st.cnt
	in := StreamInfo{BytesSent: c.bytesSent, BytesRead: c.appRead,
		DatagramsSent: c.dgSent, DatagramsReceived: c.dgReceived, DatagramsDroppedRecv: c.dgDropped,
		DatagramBytesSent: c.dgBytesSent, DatagramBytesReceived: c.dgBytesReceived}
	if st.hasS {
		sp := &st.snd
		in.BytesWritten = sp.appWritten
		if m := sp.mps; s.feat.Multipath {
			if m != nil {
				in.BytesAcknowledged = m.acked.total() - m.acked.covered(0, sp.preLen)
				if m.bind.set {
					in.Pipe, in.PipeBound = m.bind.pipe, true
				}
			}
		} else {
			// Outside multipath what reached the carrier is delivered
			// (SPEC.md 7.4); after a reset, up to the final size.
			in.BytesAcknowledged = sp.sent - min64(sp.sent, sp.preLen)
		}
		if term := sp.state == sendDataRecvd || sp.state == sendResetRecvd; !term {
			in.SendBuffered = sp.q.end - min64(sp.q.end, max64(sp.q.base, sp.preLen))
		}
	}
	if st.hasR {
		r := &st.rcv
		in.RecvBuffered, in.RecvWindow = r.buf.bytes, r.window
		switch {
		case !st.local && s.feat.Preamble && !r.preDone:
			// Nothing counts before a peer stream's preamble.
		case r.stopped:
			in.BytesReceived = r.highest - min64(r.highest, r.preSize)
		default:
			in.BytesReceived = r.arrived.total() - r.arrived.covered(0, r.preSize)
		}
	}
	return in
}
