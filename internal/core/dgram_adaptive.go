package core

import (
	"cmp"
	"errors"
	"math/bits"

	"github.com/zmuxio/zmux-go/shaping"
)

const (
	adaptiveHold             = 100 * Millisecond
	datagramLossMinFinalized = 32
)

var (
	ErrDatagramEvidence    = errors.New("zmux: invalid or stale datagram evidence")
	ErrDatagramStaleSource = errors.New("zmux: datagram evidence source expired")
)

// A provider generation is scoped to one outgoing representation. The
// revision also tracks engine replacements, even with no registered provider.
type datagramSource struct {
	generation, revision uint64
	expired              bool
}

type datagramPairKey struct {
	other                         *Pipe
	originalChannel, otherChannel bool
}

type datagramPair struct {
	loss, timing datagramLatch
}

type datagramLatch struct {
	until                 Time
	originalRun, otherRun uint64
	active                bool
}

func (p *Pipe) datagramRevision(channel bool) uint64 {
	return p.datagramSource(channel).revision
}

func (s *Session) SetDatagramSourceGeneration(id uint64, channel bool, generation uint64) error {
	p := s.pipe(id)
	if p == nil || p.out == nil || p.retired() ||
		channel && !p.out.lanes[0].info.Channel || generation <= p.datagramSource(channel).generation {
		return ErrDatagramEvidence
	}
	s.resetDatagramEvidence(p, channel, generation, false)
	return nil
}

func (p *Pipe) datagramSource(channel bool) datagramSource {
	if p.datagramFeedback != nil {
		return p.datagramFeedback.sent[b2i(channel)].source
	}
	return datagramSource{}
}

// ReportDatagramLoss records one authoritative finalized-original delta.
// A zero-count report without an observation time (Never) can declare a
// strictly newer source.
func (s *Session) ReportDatagramLoss(id, generation uint64, channel bool, at Time, finalized, lost uint64) error {
	p := s.pipe(id)
	if p == nil || p.out == nil || p.retired() || generation == 0 || lost > finalized {
		return ErrDatagramEvidence
	}
	source := p.datagramSource(channel)
	if generation < source.generation {
		return ErrDatagramEvidence
	}
	if generation == source.generation && source.expired {
		return ErrDatagramStaleSource
	}
	advance := generation > source.generation
	announcement := advance && finalized == 0
	if channel && !p.out.lanes[0].info.Channel {
		return ErrDatagramEvidence
	}
	if at == Never {
		if !announcement {
			return ErrDatagramEvidence
		}
	} else if at > s.now || s.now.Sub(at) >= datagramObservationWindow {
		return ErrDatagramEvidence
	}
	h := &p.feedback().sent[b2i(channel)]
	if advance {
		if source.generation != 0 || !announcement {
			s.resetDatagramEvidence(p, channel, generation, false)
		} else {
			h.source.generation = generation
		}
	}
	if announcement {
		return nil
	}
	if h.loss.set && at <= h.loss.at {
		return ErrDatagramEvidence
	}
	if h.loss.set && (!h.loss.fresh(s.now) || finalized < datagramLossMinFinalized) {
		h.lossRun++
	}
	h.loss = datagramLoss{at: at, finalized: finalized, lost: lost, set: true}
	return nil
}

func (s *Session) resetDatagramEvidence(p *Pipe, channel bool, generation uint64, expired bool) {
	h := &p.feedback().sent[b2i(channel)]
	*h = datagramDispatches{source: datagramSource{
		generation: generation, revision: h.source.revision + 1, expired: expired,
	}}
	s.clearDatagramPairs(p, channel)
}

func (s *Session) expireDatagramSource(p *Pipe, channel bool) {
	generation := p.datagramSource(channel).generation
	s.resetDatagramEvidence(p, channel, generation, generation != 0)
}

func (s *Session) replaceDatagramPath(p *Pipe) {
	if p.sh != nil {
		p.sh.cancelNative(shaping.ReasonStaleGeneration)
	}
	s.expireDatagramSource(p, false)
	s.expireDatagramSource(p, true)
	p.chBinding, p.chUse = datagramChannelBinding{}, ChannelUse{}
	s.wake(p.chSig)
}

func (p *Pipe) clearDatagramOriginPairs(channel bool) {
	if f := p.datagramFeedback; f != nil {
		for key := range f.pairs {
			if key.originalChannel == channel {
				delete(f.pairs, key)
			}
		}
	}
}

// clearDatagramPairs discards only entries depending on the replaced or
// retired representation. In particular, retired pipes cannot remain keys.
func (s *Session) clearDatagramPairs(p *Pipe, channel bool) {
	p.clearDatagramOriginPairs(channel)
	for _, original := range s.pipes {
		if f := original.datagramFeedback; f != nil {
			delete(f.pairs, datagramPairKey{p, false, channel})
			delete(f.pairs, datagramPairKey{p, true, channel})
		}
	}
}

func (l datagramLoss) fresh(now Time) bool {
	return l.finalized >= datagramLossMinFinalized && now.Sub(l.at) < datagramObservationWindow
}

func (l datagramLoss) comparePercent(percent uint64) int {
	hi1, lo1 := bits.Mul64(l.lost, 100)
	hi2, lo2 := bits.Mul64(l.finalized, percent)
	return cmp.Or(cmp.Compare(hi1, hi2), cmp.Compare(lo1, lo2))
}

func (l datagramLatch) evaluate(now Time, originalRun, otherRun uint64, enter, retain bool) datagramLatch {
	if l.active && l.originalRun == originalRun && l.otherRun == otherRun {
		if now < l.until || retain {
			return l
		}
	} else if enter {
		return datagramLatch{until: now.Add(adaptiveHold), originalRun: originalRun, otherRun: otherRun, active: true}
	}
	return datagramLatch{}
}

// adaptiveDatagramPair evaluates this pair alone. Callers must resolve the
// budget and any Urgent override before evaluating impairment.
func (s *Session) adaptiveDatagramPair(original, other *Pipe, originalChannel, otherChannel bool) bool {
	left, right := original.datagramFeedback, other.datagramFeedback
	if left == nil || right == nil {
		return false
	}
	key := datagramPairKey{other, originalChannel, otherChannel}
	pair := left.pairs[key]
	a, b := &left.sent[b2i(originalChannel)], &right.sent[b2i(otherChannel)]
	if a.loss.fresh(s.now) && b.loss.fresh(s.now) && b.loss.comparePercent(2) <= 0 {
		pair.loss = pair.loss.evaluate(s.now, a.lossRun, b.lossRun, a.loss.comparePercent(5) >= 0, a.loss.comparePercent(2) > 0)
	} else {
		pair.loss = datagramLatch{}
	}
	if a.timingFresh(s.now) && b.timingFresh(s.now) {
		at, bt := a.rtt+4*a.variation, b.rtt+4*b.variation
		pair.timing = pair.timing.evaluate(s.now, a.timingRun, b.timingRun, 2*at > 3*bt, 4*at > 5*bt)
	} else {
		pair.timing = datagramLatch{}
	}
	if !pair.loss.active && !pair.timing.active {
		delete(left.pairs, key)
		return false
	}
	if left.pairs == nil {
		left.pairs = make(map[datagramPairKey]datagramPair)
	}
	left.pairs[key] = pair
	return true
}
