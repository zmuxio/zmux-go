package core

import "github.com/zmuxio/zmux-go/internal/wire"

type streamGapTrace struct {
	gaps            map[uint64]streamGap
	started, filled uint64
	wait            Duration
}

type streamGap struct {
	offset uint64
	since  Time
}

// traceStreamGap follows the contiguous wire prefix, independent of how
// promptly the application reads. An advancing prefix ends the wait for its
// previous first missing byte, even when another hole still follows it.
func (s *Session) traceStreamGap(st *Stream, f *wire.Frame) {
	if s.mp == nil {
		return
	}
	r := &st.rcv
	off := r.arrived.prefix()
	trace := s.mp.gapTrace
	if trace != nil {
		if gap, ok := trace.gaps[st.id]; ok {
			if off <= gap.offset {
				return
			}
			wait := s.now.Sub(gap.since)
			trace.filled++
			trace.wait = Duration(Time(trace.wait).Add(wait))
			s.emit(Event{Kind: EvStreamGapFilled, Pipe: s.rxPipe.id, Stream: st.id,
				Offset: gap.offset, At: s.now, Wait: wait})
			delete(trace.gaps, st.id)
		}
	}
	if len(f.Data) == 0 && !f.Fin {
		return // empty non-FIN frames cannot expose a hole
	}
	if (r.arrived.len() == 0 || r.arrived.q.last().hi <= off) && (!r.finKnown || r.final <= off) {
		return
	}
	if trace == nil {
		trace = &streamGapTrace{gaps: make(map[uint64]streamGap)}
		s.mp.gapTrace = trace
	}
	trace.gaps[st.id] = streamGap{offset: off, since: s.now}
	trace.started++
	s.emit(Event{Kind: EvStreamGapStarted, Pipe: s.rxPipe.id, Stream: st.id,
		Offset: off, At: s.now})
}

func (s *Session) forgetStreamGap(id uint64) {
	if s.mp != nil && s.mp.gapTrace != nil {
		delete(s.mp.gapTrace.gaps, id)
	}
}
