package core

// LoadEvidence describes the constraints observed during an ACK delivery interval.
type LoadEvidence uint8

const (
	LoadUnknown LoadEvidence = iota
	LoadApplicationLimited
	LoadCreditLimited
	LoadLocallyLimited
	LoadBacklogged
)

// MeasuredDelivery is an ACK interval, independent of any scheduler estimate.
type MeasuredDelivery struct {
	Start, End     Time
	BytesPerSecond float64
	Load           LoadEvidence
}

type QualityObservation struct {
	Member   MemberID
	RTT      Duration
	At       Time
	Rate     float64
	ValidFor Duration
}

type MeasuredQuality struct {
	Generation                                  uint64
	RTTVersion, MinimumVersion, DeliveryVersion uint64
	Advisory                                    PathQualityReason
	RTT, Minimum                                Duration
	RTTAt, MinimumAt, RTTExpiry, MinimumExpiry  Time
	Delivery                                    MeasuredDelivery
	DeliveryExpiry                              Time
}

// A byte already queued and within credit at the start, and still unsent
// at the end, proves continuous backlog. Keep offsets, never a stream lease.
type backlogWitness struct {
	at                                Time
	stream, end, connCredit, topology uint64
	eligible                          bool
}

// A fixed affinity binding gives the stream unrestricted share on its
// attachment. Moving bindings and explicit scheduling shares are unproven.
func (s *Session) unrestrictedDeliveryStream(p *Pipe, st *Stream) bool {
	policy := s.policyOf(st)
	if len(s.pipes) == 1 {
		return policy != PolicyCustom
	}
	if p == nil || p.mp == nil || !p.mp.peerAttached(p.id) || policy != PolicyAffinity || st.snd.mps == nil {
		return false
	}
	b := st.snd.mps.bind
	return b.set && !b.moving && b.pipe == p.id
}

func (s *Session) deliveryUnrestricted(p *Pipe) bool {
	return p.sh == nil && p.out.snd.re.lo == 0 && !s.paced(p) &&
		(p.mp == nil || p.mp.peerAttached(p.id) && maySend(p, false))
}

func (s *Session) deliveryShareAvailable(p *Pipe) bool {
	if len(s.pipes) == 1 {
		return true
	}
	class, ok := s.usableClass()
	return ok && s.classUsable(class, p)
}

func (s *Session) rememberBacklog(p *Pipe, q *pathQuality) {
	w := backlogWitness{at: s.now, topology: s.mp.stateGen, connCredit: s.conn.sendMax}
	if s.deliveryUnrestricted(p) && s.deliveryShareAvailable(p) && s.conn.sent < w.connCredit {
		for it := s.sched.iter(); ; {
			st := it.next()
			if st == nil {
				break
			}
			end := min(st.snd.sendEnd(), st.snd.maxData)
			if st.idSet && !st.snd.reset && end > st.snd.sent && s.unrestrictedDeliveryStream(p, st) {
				w.stream, w.end, w.eligible = st.id, end, true
				break
			}
		}
	}
	q.witness = w
}

func (s *Session) invalidateDeliveryBinding(id uint64) {
	if p := s.pipe(id); p != nil && p.pathQuality() != nil {
		p.mp.quality.witness = backlogWitness{}
	}
}

func (s *Session) deliveryLoad(p *Pipe, q *pathQuality, start Time, bytes uint64, bubble bool) LoadEvidence {
	if s.conn.sent >= s.conn.sendMax && s.unsent != 0 {
		return LoadCreditLimited
	}
	if p.sh != nil || p.out.snd.re.lo > 0 || s.paced(p) {
		return LoadLocallyLimited
	}
	ps := &p.out.snd
	unrestricted := ps.delivered-min(ps.delivered, bytes) >= ps.deliveryRestrictedThrough
	w := q.witness
	if unrestricted && w.eligible && w.at <= start && w.topology == s.mp.stateGen && s.deliveryUnrestricted(p) && s.deliveryShareAvailable(p) && s.conn.sent < w.connCredit {
		if st := s.streams[w.stream]; st != nil && !st.snd.reset && max(st.snd.sent, st.snd.q.base) < w.end && st.snd.sendEnd() >= w.end && s.unrestrictedDeliveryStream(p, st) {
			return LoadBacklogged
		}
	}
	if bubble && s.unsent == 0 {
		return LoadApplicationLimited
	}
	return LoadUnknown
}

func (s *Session) DirectionQuality(id uint64) MeasuredQuality {
	var m MeasuredQuality
	p := s.pipe(id)
	if p == nil || p.out == nil || p.pathQuality() == nil {
		return m
	}
	q := p.mp.quality
	m = q.measured
	m.Generation, m.Advisory = q.generation, q.advice
	return m
}

// MeasurementField distinguishes the evidence used to authorize a change.
type MeasurementField uint8

const (
	DirectionEvidence MeasurementField = iota
	CurrentRTTEvidence
	MinimumRTTEvidence
	DeliveryEvidence
)

// WithdrawMeasurement invalidates all outstanding observations of the field.
// A skipped or empty sample does not withdraw earlier observations.
func (s *Session) WithdrawMeasurement(id uint64, field MeasurementField) {
	p := s.pipe(id)
	if p == nil || p.pathQuality() == nil {
		return
	}
	m := &p.mp.quality.measured
	switch field {
	case CurrentRTTEvidence:
		if m.RTT == 0 {
			return
		}
		m.RTT = 0
		m.RTTVersion++
	case MinimumRTTEvidence:
		if m.Minimum == 0 {
			return
		}
		m.Minimum = 0
		m.MinimumVersion++
	case DeliveryEvidence:
		if m.Delivery.End <= m.Delivery.Start {
			return
		}
		m.Delivery = MeasuredDelivery{}
		m.DeliveryVersion++
	default:
		return
	}
	s.NotifyCapacity()
}

// RecordProtocolRTT publishes a same-direction protocol estimate at its source time.
func (s *Session) RecordProtocolRTT(id uint64, rtt Duration, at, expires Time) {
	p := s.pipe(id)
	if p == nil || p.pathQuality() == nil {
		return
	}
	m := &p.mp.quality.measured
	if rtt <= 0 {
		return
	}
	m.RTT, m.RTTAt, m.RTTExpiry = rtt, at, expires
	var valid Duration
	if expires != 0 {
		valid = expires.Sub(at)
	}
	s.observedQuality(p, at, rtt, 0, valid)
}

func (s *Session) RecordMinimumRTT(id uint64, rtt Duration, at, expires Time) {
	p := s.pipe(id)
	if p == nil || p.pathQuality() == nil {
		return
	}
	m := &p.mp.quality.measured
	if rtt <= 0 {
		return
	}
	m.Minimum, m.MinimumAt, m.MinimumExpiry = rtt, at, expires
	s.NotifyCapacity()
}

// RecordDelivery records the raw ACK interval. Only proven unrestricted demand
// supplies a rate estimate to the candidate cache.
func (s *Session) RecordDelivery(id uint64, start, end Time, bytes uint64, expires Time, bubble bool) {
	p := s.pipe(id)
	if p == nil || p.out == nil || p.pathQuality() == nil {
		return
	}
	q := p.mp.quality
	if end <= start {
		return
	}
	rate := float64(bytes) * 1e9 / float64(end.Sub(start))
	load := s.deliveryLoad(p, q, start, bytes, bubble)
	q.measured.Delivery = MeasuredDelivery{Start: start, End: end, BytesPerSecond: rate, Load: load}
	q.measured.DeliveryExpiry = expires
	capacity := 0.0
	if load == LoadBacklogged {
		capacity = rate
	}
	var valid Duration
	if expires != 0 {
		valid = expires.Sub(end)
	}
	s.observedQuality(p, end, 0, capacity, valid)
	s.rememberBacklog(p, q)
}

func (s *Session) observedQuality(p *Pipe, at Time, rtt Duration, rate float64, valid Duration) {
	if !s.memberFeedback || p.member == nil {
		return
	}
	s.NotifyCapacity()
	if rtt > 0 || rate > 0 {
		s.qualityUpdates = append(s.qualityUpdates, QualityObservation{Member: p.member.ID, RTT: rtt, At: at, Rate: rate, ValidFor: valid})
	}
}

func (s *Session) TakeQualityUpdates() []QualityObservation {
	updates := s.qualityUpdates
	s.qualityUpdates = nil
	return updates
}
