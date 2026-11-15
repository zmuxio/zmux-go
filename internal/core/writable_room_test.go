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
	s.WaitWrite(st, sig)
	if !st.snd.wwait || !classOf(st).big || s.conn.smallHeld != 0 {
		t.Fatal("WaitWrite did not make the stream a waiting writer")
	}
	s.EndWaitWrite(st)
	if st.snd.wwait || classOf(st).big || s.conn.smallHeld != 10 {
		t.Fatalf("still a waiting writer after EndWaitWrite: smallHeld %d", s.conn.smallHeld)
	}
	s.WaitWrite(st, sig)
	if err := s.Finish(st); err != nil {
		t.Fatal(err)
	}
	if !sig.queued {
		t.Fatal("Finish did not wake the waiting writer")
	}
}
