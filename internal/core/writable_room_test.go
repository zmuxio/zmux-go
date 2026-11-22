package core

import "testing"

// TestSendRoomWithoutCredit: a stream with no credit yet (no peer seen)
// still has send-buffer room, and Write takes it: room, not credit, is
// what admits a write.
func TestSendRoomWithoutCredit(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if s.SendCapacity(st) != 0 {
		t.Fatal("credit before the peer was seen")
	}
	room := s.SendRoom(st)
	if room == 0 {
		t.Fatal("no room")
	}
	n, err := s.Write(st, make([]byte, room+1), false)
	if err != nil || uint64(n) != room {
		t.Fatalf("Write took %d of room %d: %v", n, room, err)
	}
	if s.SendRoom(st) != 0 {
		t.Fatal("room left after a full write")
	}
}

// TestEndWaitWrite: a wait for room that ends without taking the last byte
// (room found, cancelled) no longer counts the stream as a waiting writer,
// and Finish wakes the writer that waits.
func TestEndWaitWrite(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Write(st, make([]byte, 10), false); n != 10 {
		t.Fatal("nothing written")
	}
	sig := NewSignal()
	st.SetSignals(nil, sig, nil)
	s.WaitWrite(st)
	if !st.snd.wwait || !classOf(st).big || s.conn.smallHeld != 0 {
		t.Fatal("WaitWrite did not make the stream a waiting writer")
	}
	s.EndWaitWrite(st)
	if st.snd.wwait || classOf(st).big || s.conn.smallHeld != 10 {
		t.Fatalf("still a waiting writer after EndWaitWrite: smallHeld %d", s.conn.smallHeld)
	}
	s.WaitWrite(st)
	if err := s.Finish(st); err != nil {
		t.Fatal(err)
	}
	if !sig.queued {
		t.Fatal("Finish did not wake the waiting writer")
	}
}

// TestWaitWriteEndsWithPart: a writer that waits for room while the part
// ends (Close, a reset, a STOP_SENDING, the session's end) no longer counts
// as a waiting writer, whether or not its write ran again: the core ends
// the wait, not the caller.
func TestWaitWriteEndsWithPart(t *testing.T) {
	cases := []struct {
		end  func(s *Session, st *Stream)
		name string
	}{
		{name: "finish", end: func(s *Session, st *Stream) {
			if err := s.Finish(st); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "reset", end: func(s *Session, st *Stream) { s.Reset(st, 3) }},
		{name: "stop", end: func(s *Session, st *Stream) { s.onStopSending(st, 5) }},
		{name: "session", end: func(s *Session, _ *Stream) { s.Close(0, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, p := channelSession(t, nil, false)
			s.peer.MaxStreamDataBidiRem = 1 << 20
			s.loc[0].limit = 10
			s.conn.sendMax = 1 << 20
			st, _ := s.OpenStream(true)
			_, _ = s.Write(st, []byte("data"), false)
			pollStream(t, s, p)
			s.WaitWrite(st)
			tc.end(s, st)
			if tc.name == "finish" || tc.name == "reset" {
				if st.snd.wwait {
					t.Fatal("the part ended with the writer still waiting")
				}
			}
			if _, err := s.Write(st, []byte("more"), false); err == nil {
				t.Fatal("Write after the end succeeded")
			}
			if st.snd.wwait {
				t.Fatal("a failed write left the stream a waiting writer")
			}
		})
	}
}

// TestResetWakesWriter: a local reset (CancelWrite, Reject, a release)
// wakes a writer that waits for room at once, not when the RESET leaves.
func TestResetWakesWriter(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	st, _ := s.OpenStream(true)
	_, _ = s.Write(st, []byte("data"), false)
	pollStream(t, s, p)
	s.TakeWakes(nil)
	sig := NewSignal()
	st.SetSignals(nil, sig, nil)
	s.WaitWrite(st)
	s.Reset(st, 3)
	if !sig.queued {
		t.Fatal("a local reset did not wake the waiting writer")
	}
}

// TestOpenWaitersPassTurn: when credit for several streams arrives, the
// first open waiter is woken, and its leaving the queue (opened or given
// up) wakes the next; asking again keeps a waiter's place.
func TestOpenWaitersPassTurn(t *testing.T) {
	s := New(DefaultConfig(), 0)
	a, b := NewSignal(), NewSignal()
	s.WaitOpen(true, a)
	s.WaitOpen(true, b)
	s.WaitOpen(true, a) // again: still first
	if s.openW[0][0] != a || len(s.openW[0]) != 2 {
		t.Fatalf("queue %v", s.openW[0])
	}
	s.loc[0].limit = s.loc[0].reserved + 2
	s.wakeOpeners()
	if !a.queued || b.queued {
		t.Fatal("credit did not wake the first waiter only")
	}
	s.TakeWakes(nil)
	s.CancelOpenWait(true, a) // a opened its stream
	if !b.queued {
		t.Fatal("the next waiter was not woken when the first left")
	}
}
