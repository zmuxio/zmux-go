package zmux

import (
	"errors"
	"slices"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/internal/core"
)

// ErrStaleDecision indicates that a quality observation can no longer authorize retirement.
var ErrStaleDecision = errors.New("zmux: stale_decision")

// LoadEvidence identifies an interval-wide constraint on delivery.
type LoadEvidence = core.LoadEvidence

// Load classifications for a measured ACK interval.
const (
	LoadUnknown            = core.LoadUnknown
	LoadApplicationLimited = core.LoadApplicationLimited
	LoadCreditLimited      = core.LoadCreditLimited
	LoadLocallyLimited     = core.LoadLocallyLimited
	LoadBacklogged         = core.LoadBacklogged
)

// QualityReference identifies the binding, direction, generation and validity
// of an observation. Copy it into ReplaceIntent.Observations.
type QualityReference struct {
	version                                                    uint64
	carrier                                                    CarrierID
	counterGeneration                                          uint64
	bindingID                                                  *byte
	member                                                     MemberID
	pipe                                                       PipeID
	binding, target, policy, authorization, generation, health uint64
	observed, until                                            time.Time
	field                                                      core.MeasurementField
	direction                                                  Directions
}

// A field version changes on explicit withdrawal. New samples retain the
// version so each earlier observation keeps its own validity interval.
func (r QualityReference) withField(f core.MeasurementField, v uint64) QualityReference {
	r.field, r.version = f, v
	return r
}

// QualityMeasurement contains an observed value over its original interval and validity.
type QualityMeasurement[T any] struct {
	Value          T
	At, ValidUntil time.Time
	Interval       time.Duration
	Reference      QualityReference
}

// DirectionQuality contains only evidence attributable to this local direction.
type DirectionQuality struct {
	Health          DirectionHealth
	Generation      uint64
	RTT, MinimumRTT *QualityMeasurement[time.Duration]
	Delivery        *QualityMeasurement[float64] // locally ACK-delivered bytes per second
	PhysicalLoss    *CarrierLoss
	LossReference   QualityReference
	Reference       QualityReference
	Advisory        PipePathQualityReason
	Load            LoadEvidence
}

// AttachmentQuality projects the present directions of one member attachment.
type AttachmentQuality struct {
	Pipe          PipeID
	Send, Receive *DirectionQuality
}

func qualityMeasurement[T any](ref QualityReference, value T, at, until time.Time, interval time.Duration) *QualityMeasurement[T] {
	ref.observed, ref.until = at, until
	return &QualityMeasurement[T]{Value: value, At: at, ValidUntil: until, Interval: interval, Reference: ref}
}

func (c *Conn) attachmentQualityLocked(member MemberID, pipe PipeID, physical map[CarrierID]*CarrierLoss) AttachmentQuality {
	out := AttachmentQuality{Pipe: pipe}
	h, ok := c.s.PipeHealth(uint64(pipe))
	if !ok {
		return out
	}
	policy := c.s.SessionPolicy()
	ref := QualityReference{member: member, pipe: pipe, policy: policy.Revision, authorization: policy.AuthGeneration}
	if sc := c.control; sc != nil {
		ref.bindingID = sc.identity
		ref.binding, ref.target = sc.view.BindingGeneration, sc.target.Revision
	}
	if h.Receive.State != core.HealthAbsent {
		ref.direction, ref.health = Receive, h.Receive.Generation
		out.Receive = &DirectionQuality{Health: c.directionHealth(h.Receive), Reference: ref}
	}
	if h.Send.State == core.HealthAbsent {
		return out
	}
	m := c.s.DirectionQuality(uint64(pipe))
	ref.direction, ref.generation, ref.health = Send, m.Generation, h.Send.Generation
	d := &DirectionQuality{Health: c.directionHealth(h.Send), Generation: m.Generation, Reference: ref, Advisory: PipePathQualityReason(m.Advisory)}
	now := c.clk.now()
	window := time.Minute
	if c.control != nil {
		window = c.control.ctl.cache.config().performanceWindow
	}
	expiry := func(at, end core.Time) core.Time {
		if end == 0 {
			return at.Add(core.Duration(window))
		}
		return end
	}
	m.RTTExpiry = expiry(m.RTTAt, m.RTTExpiry)
	m.MinimumExpiry = expiry(m.MinimumAt, m.MinimumExpiry)
	m.DeliveryExpiry = expiry(m.Delivery.End, m.DeliveryExpiry)
	if m.RTT > 0 && now < m.RTTExpiry {
		d.RTT = qualityMeasurement(ref.withField(core.CurrentRTTEvidence, m.RTTVersion), time.Duration(m.RTT), c.clk.wall(m.RTTAt), c.clk.wall(m.RTTExpiry), 0)
	}
	if m.Minimum > 0 && now < m.MinimumExpiry {
		d.MinimumRTT = qualityMeasurement(ref.withField(core.MinimumRTTEvidence, m.MinimumVersion), time.Duration(m.Minimum), c.clk.wall(m.MinimumAt), c.clk.wall(m.MinimumExpiry), 0)
	}
	if m.Delivery.End > m.Delivery.Start && now < m.DeliveryExpiry {
		d.Load = m.Delivery.Load
		d.Delivery = qualityMeasurement(ref.withField(core.DeliveryEvidence, m.DeliveryVersion), m.Delivery.BytesPerSecond, c.clk.wall(m.Delivery.End), c.clk.wall(m.DeliveryExpiry), time.Duration(m.Delivery.End.Sub(m.Delivery.Start)))
	}
	if pd := c.pipes[uint64(pipe)]; pd != nil && pd.sampler != nil {
		obs := carrierstats.Cached(pd.sampler)
		loss, seen := physical[obs.ID]
		if !seen {
			loss = physicalLoss(obs, window, c.clk.wall(now))
			physical[obs.ID] = loss
		}
		d.PhysicalLoss = loss
		if loss != nil {
			d.LossReference = ref
			d.LossReference.carrier, d.LossReference.counterGeneration = loss.CarrierID, loss.Generation
			d.LossReference.version = loss.version
			d.LossReference.observed = loss.SampledAt
			d.LossReference.until = loss.ValidUntil
		}
	}
	out.Send = d
	return out
}

func (sc *SessionControl) checkQualityLocked(t SessionTarget, accept bool) error {
	c := sc.c
	now := c.clk.source().Now()
	for _, intent := range t.Replace {
		for _, ref := range intent.Observations {
			if ref.bindingID != sc.identity || ref.binding != sc.view.BindingGeneration || accept && ref.target != sc.target.Revision || !slices.Contains(intent.Members, ref.member) || c.s.MemberOf(uint64(ref.pipe)) != ref.member || !ref.until.IsZero() && !now.Before(ref.until) {
				return ErrStaleDecision
			}
			p := c.s.SessionPolicy()
			if p.Revision != ref.policy || p.AuthGeneration != ref.authorization {
				return ErrStaleDecision
			}
			h, ok := c.s.PipeHealth(uint64(ref.pipe))
			if !ok {
				return ErrStaleDecision
			}
			if ref.carrier != (CarrierID{}) {
				pd := c.pipes[uint64(ref.pipe)]
				if pd == nil || pd.sampler == nil {
					return ErrStaleDecision
				}
				obs := carrierstats.Cached(pd.sampler)
				if !ref.matchesLoss(obs) {
					return ErrStaleDecision
				}
			}
			switch ref.direction {
			case Send:
				m := c.s.DirectionQuality(uint64(ref.pipe))
				if h.Send.Generation != ref.health || m.Generation != ref.generation || !ref.available(m) {
					return ErrStaleDecision
				}
			case Receive:
				if h.Receive.Generation != ref.health {
					return ErrStaleDecision
				}
			default:
				return ErrStaleDecision
			}
		}
	}
	return nil
}

func (r QualityReference) matchesLoss(obs carrierstats.Observation) bool {
	return obs.ID == r.carrier && obs.Generation == r.counterGeneration &&
		obs.Known && obs.Interval > 0 && obs.LossVersion == r.version
}

func (r QualityReference) available(m core.MeasuredQuality) bool {
	switch r.field {
	case core.DirectionEvidence:
		return true
	case core.CurrentRTTEvidence:
		return m.RTT > 0 && m.RTTVersion == r.version
	case core.MinimumRTTEvidence:
		return m.Minimum > 0 && m.MinimumVersion == r.version
	case core.DeliveryEvidence:
		return m.Delivery.End > m.Delivery.Start && m.DeliveryVersion == r.version
	default:
		return false
	}
}

type bindingObservation struct {
	cache       *CarrierCache
	key         CarrierCacheKey
	observation CarrierObservation
}

func (sc *SessionControl) observeMemberLocked(o core.QualityObservation) *bindingObservation {
	if f := sc.feedback[o.Member]; f != nil {
		return &bindingObservation{cache: sc.ctl.cache, key: f.key, observation: CarrierObservation{At: sc.c.clk.wall(o.At), RTT: time.Duration(o.RTT), Rate: o.Rate, ValidFor: time.Duration(o.ValidFor)}}
	}
	return nil
}

func (o *bindingObservation) record() {
	if o != nil {
		_ = o.cache.update(o.key, CachePerformance, o.observation)
	}
}

func (sc *SessionControl) observeQualityLocked() {
	for _, o := range sc.c.s.TakeQualityUpdates() {
		sc.observeMemberLocked(o).record()
	}
}
