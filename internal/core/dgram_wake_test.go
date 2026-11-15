package core

import "testing"

// TestDiscardWakesDatagramWaiter: discarding a stream whose datagram queue
// is empty (never allocated) still wakes its datagram waiter, which then
// sees the flow's end.
func TestDiscardWakesDatagramWaiter(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	st.idSet, st.local = true, true
	d := NewSignal()
	st.SetSignals(nil, nil, d)
	st.snd.state, st.rcv.state = sendDataRecvd, recvDataRead
	st.releasedS, st.releasedR = true, true
	s.TakeWakes(nil)
	s.maybeDiscard(st)
	if !st.discarded {
		t.Fatal("stream not discarded")
	}
	woke := false
	for _, w := range s.TakeWakes(nil) {
		woke = woke || w == d
	}
	if !woke {
		t.Fatal("datagram waiter not woken on discard")
	}
	if end, err := s.StreamDatagramsEnded(st); !end || err != nil {
		t.Fatalf("StreamDatagramsEnded = %v, %v", end, err)
	}
}

// TestStopWakesDatagramWaiter: stopping the receiving part wakes the
// stream's datagram waiter.
func TestStopWakesDatagramWaiter(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	d := NewSignal()
	st.SetSignals(nil, nil, d)
	s.TakeWakes(nil)
	s.stopRecv(st, 5)
	woke := false
	for _, w := range s.TakeWakes(nil) {
		woke = woke || w == d
	}
	if !woke {
		t.Fatal("datagram waiter not woken on stop")
	}
}
