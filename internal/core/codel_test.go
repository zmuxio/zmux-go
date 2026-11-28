package core

import "testing"

// CoDel drops nothing below the target or for a moment above it, starts
// once the sojourn has stood above it for an interval, drops faster the
// longer it stands, and stops at once when the delay falls.
func TestCodel(t *testing.T) {
	const target = 20 * Millisecond
	var c codel
	now := Time(Second)
	leave := func(sojourn Duration, alone bool) bool { return c.drop(now.Add(-sojourn), now, target, alone) }

	if leave(10*Millisecond, false) || leave(19*Millisecond, false) {
		t.Fatal("dropped below the target")
	}
	if leave(30*Millisecond, false) {
		t.Fatal("dropped on the first sojourn above the target")
	}
	now = now.Add(codelInterval - Millisecond)
	if leave(30*Millisecond, false) {
		t.Fatal("dropped before an interval above the target")
	}
	now = now.Add(Millisecond)
	if leave(30*Millisecond, true) {
		t.Fatal("dropped the only datagram queued")
	}
	if leave(30*Millisecond, false) {
		t.Fatal("a lone datagram did not reset the standing state")
	}
	now = now.Add(codelInterval)
	if !leave(30*Millisecond, false) {
		t.Fatal("no drop after an interval above the target")
	}
	// Dropping: the next drop is an interval later, then interval/sqrt(2).
	if leave(30*Millisecond, false) {
		t.Fatal("dropped again before the control law allows")
	}
	now = now.Add(codelInterval)
	if !leave(30*Millisecond, false) || c.count != 2 {
		t.Fatalf("second drop: count %d", c.count)
	}
	gap := c.dropNext.Sub(now)
	if want := Duration(70710678); gap < want-Millisecond || gap > want+Millisecond {
		t.Fatalf("next drop in %d, want about %d", gap, want)
	}
	if leave(5*Millisecond, false) || c.dropping {
		t.Fatal("still dropping below the target")
	}
	c.idle()
	if c.firstAbove != 0 || c.dropping {
		t.Fatal("idle kept the standing state")
	}
	if (&codel{}).drop(0, Time(10*Second), 0, false) {
		t.Fatal("a zero target dropped")
	}
}

// A stream queue read late drops by sojourn, counted, and keeps the
// datagram that arrived last.
func TestStreamDatagramSojourn(t *testing.T) {
	s := &Session{cfg: Config{DgramTarget: 20 * Millisecond, DgramStreamQ: 1000}}
	st := &Stream{bidi: true}
	s.now = Time(Second)
	for range 100 {
		s.queueDatagram(st, receivedDgram{context: 1, data: []byte{'x'}})
		s.now = s.now.Add(Millisecond)
	}
	s.now = s.now.Add(200 * Millisecond)
	got := 0
	for {
		_, _, ok := s.RecvStreamDatagram(st)
		if !ok {
			break
		}
		got++
		s.now = s.now.Add(10 * Millisecond)
	}
	if got == 0 || got == 100 || st.cnt.dgDropped != uint64(100-got) {
		t.Fatalf("delivered %d, dropped %d", got, st.cnt.dgDropped)
	}
}
