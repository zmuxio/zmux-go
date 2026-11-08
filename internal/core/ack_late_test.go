package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// An entry whose ACK Delay is above the peer's maximum gives an RTT sample
// on a pipe that never had one and on a pipe without one for 1 s, and no
// other: a peer whose acknowledgements are always late must
// not leave the pipe, and the window auto-tuning it feeds, without an RTT.
func TestLateAckSamplesOnlyStalePipes(t *testing.T) {
	s := capacitySession()
	a := newAckState(s)
	p := s.pipes[0]
	ol := p.out.lanes[0]
	ps := &p.out.snd
	var end uint64
	ack := func(sent, now Time, delay Duration) {
		t.Helper()
		start := end
		end += 100
		ol.pos, ol.acked, ps.so = end, end, end
		ol.segs = []sendSeg{{pos: start, end: end, so: start}}
		ps.samples = []sample{{end: end, sent: sent, firstSentAt: sent, deliveredAt: sent}}
		a.advanced(p, ol, wire.AckEntry{Position: end, Delay: uint64(delay / Microsecond)}, now)
	}
	late := 2 * a.peerDelay

	first := Time(200 * Millisecond)
	ack(Time(100*Millisecond), first, late)
	if ps.rttAt != first || !p.rtt().has || !s.tn.loopMin.has {
		t.Fatalf("first late entry gave no sample: rttAt %d, has %v, tune %v", ps.rttAt, p.rtt().has, s.tn.loopMin.has)
	}

	soon := first.Add(500 * Millisecond)
	ack(first, soon, late)
	if ps.rttAt != first {
		t.Fatalf("late entry within 1 s of a sample was sampled at %d", ps.rttAt)
	}

	stale := first.Add(Second)
	ack(soon, stale, late)
	if ps.rttAt != stale {
		t.Fatalf("late entry 1 s after the last sample was not sampled: %d", ps.rttAt)
	}
}
