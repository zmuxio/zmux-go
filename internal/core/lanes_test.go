package core

import "testing"

// Data lanes take the paced room in turns; lane 0
// and the copy lane are never held back.
func TestDataLaneTurns(t *testing.T) {
	s := &Session{}
	l0, l1, l2, l3 := &outLane{lane: 0}, &outLane{lane: 1}, &outLane{lane: 2}, &outLane{lane: 3}
	p := &Pipe{out: &outDir{lanes: []*outLane{l0, l1, l2, l3}}}
	p.out.lo.copyLane = l3
	take := func(l *outLane) bool {
		if !s.dataTurn(p, l) {
			return false
		}
		s.tookTurn(p, l)
		return true
	}
	if !take(l1) {
		t.Fatal("first turn refused")
	}
	if !take(l1) { // nobody else asked: a new round
		t.Fatal("lone lane refused")
	}
	if !take(l2) {
		t.Fatal("unserved lane refused")
	}
	l2.served = false
	l2.waiting = true // lane 2 asked and waits
	if take(l1) {
		t.Fatal("served lane took the turn of a waiting one")
	}
	if !take(l0) || !take(l3) || !take(l3) {
		t.Fatal("lane 0 or the copy lane held back")
	}
	if !take(l2) || !take(l1) {
		t.Fatal("turns did not advance")
	}
}

// A record batch must retain the direction's lane until its final position is accounted.
func TestBatchKeepsDirectionOnItsPhysicalLane(t *testing.T) {
	s := New(DefaultConfig(), 0)
	info := CarrierInfo{Lanes: true}
	p := s.AttachBootstrap(info, info, nil)
	s.peerSeen = true
	s.state = StateActive
	s.peer.MaxLanes = 3
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	s.feat = Features{PipeAck: true, LanesOut: true, LanesIn: true}
	p.out.lo.offered = true
	for i := uint64(1); i <= 2; i++ {
		p.out.lanes = append(p.out.lanes, &outLane{lane: i, info: info, confirmed: true})
	}
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	st.urgency = 3
	if _, err := s.Write(st, make([]byte, 48<<10), false); err != nil {
		t.Fatal(err)
	}
	var b Batch
	var w recWriter
	w.init(&b, info.Framing, 65536, 65536)
	defer b.Discard()
	s.writeData(&w, p, p.out.lanes[1])
	w.end()
	if len(w.touched) == 0 {
		t.Fatal("batch did not send stream data")
	}
	if st.snd.lane != 1 {
		t.Fatalf("batch rebound its direction to lane %d, want 1", st.snd.lane)
	}
	s.afterBatch(&w, p, p.out.lanes[1])
	if st.snd.laneEnd <= p.out.lanes[1].acked {
		t.Fatal("batch already covered by old acknowledgement")
	}
	if got, _, _ := s.laneFor(p, st); got != p.out.lanes[1] {
		t.Fatal("direction moved before its batch was acknowledged")
	}
	s.Release(st)
}
