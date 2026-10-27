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
