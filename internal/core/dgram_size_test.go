package core

import (
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
	}{{"tcp", false, 65535 - 2}, {"tls", true, wire.DefaultMaxRecordSize - 2}} {
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
			if err := s.SendDatagram(nil, 0, make([]byte, 70000)); err != nil {
				t.Fatal(err)
			}
			s.peerSeen = true
			if err := s.SendDatagram(nil, 0, []byte("y")); err != nil {
				t.Fatal(err)
			}
			if n := datagramFrames(t, s, p); n != 1 {
				t.Fatalf("%d DATAGRAM frames after a too-large one, want 1", n)
			}
			if st := s.SessionStats(); st.DatagramsDroppedSend != 1 {
				t.Fatalf("DatagramsDroppedSend %d", st.DatagramsDroppedSend)
			}
		})
	}
}

func TestDatagramDataMax(t *testing.T) {
	for capy := uint64(0); capy < 20000; capy++ {
		for _, wl := range []bool{false, true} {
			n := datagramDataMax(capy, wl)
			if n > 0 && uint64(datagramFrameLen(n, wl)) > capy || uint64(datagramFrameLen(n+1, wl)) <= capy {
				t.Fatalf("cap %d withLen %v: %d", capy, wl, n)
			}
		}
	}
}
