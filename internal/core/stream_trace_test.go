package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestStreamGapTracingUsesInitializedPathState(t *testing.T) {
	s := capacitySession()
	s.cfg.TraceStreams = true
	s.feat.Multipath = true // negotiated, before the path layer starts
	s.rxPipe = s.pipes[0]
	f := &wire.Frame{Kind: wire.KindStream, StreamID: 1, Offset: 1, Data: []byte{1}}
	if _, err := s.onStream(f, &Buf{B: f.Data}); err != nil {
		t.Fatal(err)
	}
	st := s.streams[f.StreamID]
	if st == nil || st.rcv.arrived.len() == 0 {
		t.Fatal("fixture did not expose a receive gap")
	}
	s.traceStreamGap(st, f)
	if s.SessionStats().StreamGapsStarted != 0 {
		t.Fatal("uninitialized path layer recorded a gap")
	}

	s.mp = &multiPath{}
	s.feat.Multipath = false // the path layer itself is the tracing authority
	s.traceStreamGap(st, f)
	if s.SessionStats().StreamGapsStarted != 1 || s.mp.gapTrace == nil {
		t.Fatal("initialized path layer did not record the gap")
	}
}

func TestStreamGapStatisticsMonotonic(t *testing.T) {
	s := capacityMP()
	s.feat.Multipath = true
	s.cfg.TraceStreams = true
	s.rxPipe = s.pipes[0]
	s.SetNow(1)
	input := func(off uint64) {
		t.Helper()
		f := &wire.Frame{Kind: wire.KindStream, StreamID: 1, Offset: off, Data: []byte{1}}
		if _, err := s.onStream(f, &Buf{B: f.Data}); err != nil {
			t.Fatal(err)
		}
	}
	input(1)
	if st := s.SessionStats(); st.StreamGapsStarted != 1 || st.StreamGapsFilled != 0 || st.StreamGapWait != 0 {
		t.Fatalf("open gap: %+v", st)
	}
	s.SetNow(20)
	input(0)
	if st := s.SessionStats(); st.StreamGapsStarted != 1 || st.StreamGapsFilled != 1 || st.StreamGapWait != 19 {
		t.Fatalf("filled gap: %+v", st)
	}
	input(3)
	s.mp.gapTrace.wait = Duration(Never) - 1
	s.SetNow(30)
	input(2)
	if st := s.SessionStats(); st.StreamGapsStarted != 2 || st.StreamGapsFilled != 2 || st.StreamGapWait != Duration(Never) {
		t.Fatalf("saturated wait: %+v", st)
	}
}
