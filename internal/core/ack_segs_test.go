package core

import "testing"

// Two lanes interleaving one batch each far past ackKeepMany unacknowledged
// batches (an honest peer on a long round trip at a high batch rate) keep
// the session up and the send-order map bounded; the cumulative point
// derived from it never passes the true one and reaches it once both lanes
// are acknowledged.
func TestSendSegsInterleavedBounded(t *testing.T) {
	s := capacitySession()
	a := newAckState(s)
	p := s.pipes[0]
	ol0 := p.out.lanes[0]
	ol1 := &outLane{lane: 1, confirmed: true}
	p.out.lanes = append(p.out.lanes, ol1)
	ps := &p.out.snd
	var w recWriter
	type mark struct{ pos, so uint64 } // lane 0 position and the true cumulative point at it
	var marks []mark
	const batches = 3 * ackKeepMany
	for range batches {
		for _, ol := range []*outLane{ol0, ol1} {
			start := ol.pos
			ol.pos += 10
			a.onSent(p, ol, start, &w, 0, aheadSnap{})
			if ol == ol1 { // with lane 1 acknowledged, the next lane 0 byte's send order
				marks = append(marks, mark{ol0.pos, ps.so})
			}
		}
	}
	if s.err != nil {
		t.Fatalf("session failed: %v", s.err)
	}
	if len(ol0.segs) > ackKeepMany || len(ol1.segs) > ackKeepMany {
		t.Fatalf("segments %d %d", len(ol0.segs), len(ol1.segs))
	}
	ol1.acked = ol1.pos
	cumPoint(p.out, ol1)
	prev := uint64(0)
	for _, m := range marks {
		ol0.acked = m.pos
		c := cumPoint(p.out, ol0)
		if c > m.so || c < prev {
			t.Fatalf("cumulative point %d at lane position %d: true %d, previous %d", c, m.pos, m.so, prev)
		}
		prev = c
	}
	if prev != ps.so {
		t.Fatalf("all acknowledged: %d, want %d", prev, ps.so)
	}
}
