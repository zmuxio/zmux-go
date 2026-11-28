package core

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// datagramKinds polls one batch and returns the number of DATAGRAM frames.
func datagramFrames(t *testing.T, s *Session, p *Pipe) int {
	t.Helper()
	n := 0
	for _, k := range pollKinds(t, s, p, 1) {
		if k == wire.KindDatagram {
			n++
		}
	}
	return n
}

// TestDatagramMaxPayloadIsSendable: a datagram of exactly
// MaxDatagramPayload is written (without its Length field, ending its
// record), one byte more is refused, and a queued datagram that can never
// fit a record is dropped without holding back the ones after it.
func TestDatagramMaxPayloadIsSendable(t *testing.T) {
	for _, tc := range []struct {
		name string
		tls  bool
		max  int
	}{{"tcp", false, 65535 - 3}, {"tls", true, wire.DefaultMaxRecordSize - 3}} {
		t.Run(tc.name, func(t *testing.T) {
			s, p := channelSession(t, nil, false)
			p.out.lanes[0].info.TLS = tc.tls
			if m := s.MaxDatagramPayload(1); m != tc.max {
				t.Fatalf("MaxDatagramPayload %d, want %d", m, tc.max)
			}
			if err := s.SendDatagram(nil, 0, make([]byte, tc.max+1)); err == nil {
				t.Fatal("one byte above the maximum was accepted")
			}
			if err := s.SendDatagram(nil, 0, make([]byte, tc.max)); err != nil {
				t.Fatal(err)
			}
			if err := s.SendDatagram(nil, 0, []byte("x")); err != nil {
				t.Fatal(err)
			}
			if n := datagramFrames(t, s, p); n != 2 {
				t.Fatalf("%d DATAGRAM frames sent, want 2", n)
			}
			// Queued before the peer's limits were known, too large for
			// any record: dropped, and the next one still goes.
			s.peerSeen = false
			if err := s.SendDatagram(nil, 0, make([]byte, 70000)); !errors.Is(err, ErrNotNegotiated) {
				t.Fatal(err)
			}
			s.peerSeen = true
			if err := s.SendDatagram(nil, 0, []byte("y")); err != nil {
				t.Fatal(err)
			}
			if n := datagramFrames(t, s, p); n != 1 {
				t.Fatalf("%d DATAGRAM frames after a too-large one, want 1", n)
			}
			if st := s.SessionStats(); st.DatagramsDroppedSend != 0 {
				t.Fatalf("DatagramsDroppedSend %d", st.DatagramsDroppedSend)
			}
		})
	}
}

func TestDatagramDataMax(t *testing.T) {
	for capy := range uint64(20000) {
		for _, wl := range []bool{false, true} {
			n := datagramDataMax(capy, wl)
			if n > 0 && uint64(datagramFrameLen(n, wl)) > capy || uint64(datagramFrameLen(n+1, wl)) <= capy {
				t.Fatalf("cap %d withLen %v: %d", capy, wl, n)
			}
		}
	}
}

// TestDatagramAtRecordFloor: on a pipe whose records take the floor of 8192
// bytes, MaxDatagramPayload is what one such record holds; a datagram over
// it is refused when queued, and one queued while the limits were unknown is
// dropped and counted without stalling the ones behind it. A lane applies
// the same cap.
func TestDatagramAtRecordFloor(t *testing.T) {
	s, p := channelSession(t, nil, false)
	p.out.lanes[0].info.MaxRecord = 8192
	m := s.MaxDatagramPayload(1)
	if m != 8192-3 {
		t.Fatalf("MaxDatagramPayload %d, want %d", m, 8192-3)
	}
	if err := s.SendDatagram(nil, 0, make([]byte, m+1)); err == nil {
		t.Fatal("one byte above the maximum was accepted")
	}
	if err := s.SendDatagram(nil, 0, make([]byte, m)); err != nil {
		t.Fatal(err)
	}
	if n := datagramFrames(t, s, p); n != 1 {
		t.Fatalf("%d DATAGRAM frames sent, want 1", n)
	}
	s.peerSeen = false
	if err := s.SendDatagram(nil, 0, make([]byte, 9000)); !errors.Is(err, ErrNotNegotiated) {
		t.Fatal(err)
	}
	s.peerSeen = true
	if err := s.SendDatagram(nil, 0, []byte("y")); err != nil {
		t.Fatal(err)
	}
	if n := datagramFrames(t, s, p); n != 1 {
		t.Fatalf("%d DATAGRAM frames after a too-large one, want 1", n)
	}
	if st := s.SessionStats(); st.DatagramsDroppedSend != 0 {
		t.Fatalf("DatagramsDroppedSend %d", st.DatagramsDroppedSend)
	}
	// Datagrams use lane zero, so a narrower alternate lane does not lower the bound.
	s.peerMaxRecord = 65535
	p.out.lanes[0].info.MaxRecord = 0
	l := &outLane{lane: 1, info: CarrierInfo{MaxRecord: 8192}}
	p.out.lanes = append(p.out.lanes, l)
	if m := s.MaxDatagramPayload(1); m != 65535-3 {
		t.Fatalf("with a capped lane: %d", m)
	}
}

// A frame that no record can hold is flagged (PollTransmit then fails the session) instead of waiting for
// room that never comes.
func TestOversizeFrameIsFlagged(t *testing.T) {
	var w recWriter
	b := GetBatch()
	defer PutBatch(b)
	w.init(b, FramingBytes, 4096, DefaultBatch)
	if !w.frame(4000, true, func(b []byte) []byte { return append(b, make([]byte, 4000)...) }) || w.oversize {
		t.Fatal("a frame within the record was refused")
	}
	if w.frame(4097, true, func(b []byte) []byte { return b }) || !w.oversize {
		t.Fatal("a frame over the record limit was not flagged")
	}
}
