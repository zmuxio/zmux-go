package core

import "testing"

// TestRoundRobinTurnsAreWholeChunks: spread-round-robin shares bytes by
// chunks of SpreadChunk, not by write calls. Stream credit arrives as a
// whole chunk at pipe 0's write opportunity and as 1 KiB at pipe 2's: a
// pipe whose write the credit cuts short keeps the turn until its chunk is
// full, so every round gives both pipes one chunk.
func TestRoundRobinTurnsAreWholeChunks(t *testing.T) {
	s, g := mpBenchSession(2)
	s.cfg.Policy = PolicySpreadRoundRobin
	chunk := uint64(s.cfg.SpreadChunk)
	st := &Stream{id: 4, hasS: true}
	st.snd.q.end = 1 << 30 // far more data ready than credit
	var credit, off uint64
	sent := map[uint64]uint64{}
	for range 40 {
		for _, step := range []struct {
			pipe, grant uint64
		}{{0, chunk}, {2, 1024}} {
			credit += step.grant
			p := s.pipe(step.pipe)
			if s.rrPick(g, 0, st) != p {
				continue
			}
			d := &decision{p: p, g: g}
			n := min64(credit, s.chunkRoom(d, st))
			s.rrSent(g, 0, p, st, off, n)
			credit -= n
			off += n
			sent[p.id] += n
		}
		// Both pipes carried equal bytes but for the chunk being filled.
		if d := int64(sent[0]) - int64(sent[2]); d > int64(chunk) || d < -int64(chunk) {
			t.Fatalf("pipe 0 sent %d, pipe 2 sent %d", sent[0], sent[2])
		}
	}
	if sent[2] < 10*chunk {
		t.Fatalf("pipe 2 carried %d bytes", sent[2])
	}
}
