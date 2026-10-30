package core

import "testing"

func TestCancelUnopenedEndsReceiveAndWakesCapacity(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	read, capacity := NewSignal(), NewSignal()
	s.WaitRead(st, read)
	if s.WaitSendCapacity(st, 1, capacity) {
		t.Fatal("unestablished stream unexpectedly has capacity")
	}
	s.Reset(st, 91)
	wakes := s.TakeWakes(nil)
	seenRead, seenCapacity := false, false
	for _, w := range wakes {
		seenRead = seenRead || w == read
		seenCapacity = seenCapacity || w == capacity
	}
	if !seenRead || !seenCapacity {
		t.Fatalf("cancelled provisional waiter not woken: read=%v capacity=%v", seenRead, seenCapacity)
	}
	_, buf, fin, err := s.ReadChunk(st, 1)
	if buf != nil {
		buf.Unref()
	}
	if err != nil || !fin {
		t.Fatalf("unopened receive did not end: fin=%v err=%v", fin, err)
	}
	if st.idSet || s.ctlHead != nil || s.SendCapacity(st) != 0 {
		t.Fatal("cancelled stream emitted control or retained capacity")
	}
}

func TestCloseWakesProvisionalObservers(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	read, write := NewSignal(), NewSignal()
	st.SetSignals(read, write)
	s.Close(0, nil)
	wakes := s.TakeWakes(nil)
	seenRead, seenWrite := false, false
	for _, w := range wakes {
		seenRead = seenRead || w == read
		seenWrite = seenWrite || w == write
	}
	if !seenRead || !seenWrite {
		t.Fatal("session close missed provisional observers")
	}
}

func TestShutdownRetainsUnemittedStreamsAndRefusesOpeners(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	wait := NewSignal()
	s.WaitOpen(true, wait)
	s.Shutdown(0, nil)
	if s.isClosing() {
		t.Fatal("shutdown discarded an already opened provisional stream")
	}
	if _, err = s.OpenStream(true); err == nil {
		t.Fatal("shutdown allowed a new stream")
	}
	found := false
	for _, w := range s.TakeWakes(nil) {
		found = found || w == wait
	}
	if !found {
		t.Fatal("shutdown failed to wake an already blocked opener")
	}
	s.Release(st)
	s.Settle()
	if !s.isClosing() {
		t.Fatal("releasing the last provisional stream did not finish shutdown")
	}
}
