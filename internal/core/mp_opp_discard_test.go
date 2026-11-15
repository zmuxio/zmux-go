package core

import "testing"

// TestBlockingFrameSkipsEndedStreams: a STREAM frame retained on another
// pipe after its stream was discarded (or reset) blocks nothing, so the
// opportunistic re-send never reaches for the stream.
func TestBlockingFrameSkipsEndedStreams(t *testing.T) {
	s := New(DefaultConfig(), 0)
	f := &Pipe{id: 1, out: &outDir{lanes: []*outLane{{}}}, mp: &attach{}}
	q := &Pipe{id: 2, out: &outDir{lanes: []*outLane{{log: []retained{{d: frameDesc{kind: dStream, id: 4, n: 10}}}}}}, mp: &attach{}}
	s.pipes = []*Pipe{f, q}
	if p, e := s.blockingFrame(f, nil); p != nil || e != nil {
		t.Fatal("a frame of a discarded stream counts as blocking")
	}
	if !s.oppSend(nil, f, nil) {
		t.Fatal("oppSend gave up on a discarded stream's frame")
	}
	st := &Stream{id: 4, hasS: true}
	s.streams[4] = st
	if p, _ := s.blockingFrame(f, nil); p != q {
		t.Fatal("a live stream's frame does not count")
	}
	st.snd.reset = true
	if p, _ := s.blockingFrame(f, nil); p != nil {
		t.Fatal("a reset stream's frame counts as blocking")
	}
}

// TestAbandonWithoutStateResent: a PIPE_ABANDON for an ID without
// attachment state is sent again when the pipe that carried it is lost,
// as every PIPE_ABANDON until acknowledged (MULTIPATH.md Section 7.4).
func TestAbandonWithoutStateResent(t *testing.T) {
	s, _ := statsMP()
	id := uint64(3)
	s.lost(&frameDesc{kind: dAbandonID, id: id, val: 9})
	if code, ok := s.mp.pendAbandon[id]; !ok || code != 9 {
		t.Fatalf("not owed again: %v %v", code, ok)
	}
}
