package core

import (
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestDatagramReportDeadlineFollowsRTT(t *testing.T) {
	s, p, _ := quietWriterPipes()
	s.SetNow(Time(Second))
	s.feat.DatagramFamily = true
	s.gates.MaxDatagram = 65535
	p.mp.rtt.has, p.mp.rtt.smoothed = true, 400*Millisecond
	s.pathRTTChanged(p, true)
	s.datagramArrival(p, false, 1)
	route, _ := s.primary()
	if frames := drainRequested(t, s, route); !slices.Contains(frames, wire.KindDatagramAck) {
		t.Fatalf("initial arrival sent %v", frames)
	}
	s.TakePollRequests()
	s.datagramArrival(p, false, 2)
	if got := s.datagramReportWake(); got != s.now.Add(100*Millisecond) {
		t.Fatalf("initial report deadline %v", got)
	}
	p.mp.rtt.smoothed = 200 * Millisecond
	s.pathRTTChanged(p, false)
	due := s.now.Add(50 * Millisecond)
	if got := s.datagramReportWake(); got != due {
		t.Fatalf("new report deadline %v, want %v", got, due)
	}
	s.HandleTimeout(due - 1)
	if len(s.TakePollRequests()) != 0 {
		t.Fatal("reported before the earlier RTT deadline")
	}
	s.HandleTimeout(due)
	if got := s.TakePollRequests(); len(got) != 1 || got[0].Pipe != route.id {
		t.Fatalf("report deadline requests %v", got)
	}
	if s.datagramReportWake() != Never {
		t.Fatal("a registered writer also left a host timer armed")
	}
	if frames := drainRequested(t, s, route); !slices.Contains(frames, wire.KindDatagramAck) {
		t.Fatalf("new deadline sent %v", frames)
	}
}

func TestDatagramReroutesAfterTopologyChange(t *testing.T) {
	s, first, second := quietWriterPipes()
	s.peerMaxDgram = 65535
	s.gates.MaxDatagram = 65535
	s.SetTopologyExclusion(second.id, true, false)
	if err := s.SendDatagram(nil, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if first.datagramRecords != 1 {
		t.Fatal("first pipe did not own the queued datagram")
	}

	s.SetTopologyExclusion(first.id, true, false)
	if first.datagramRecords != 0 {
		t.Fatal("excluded pipe still owns the queued datagram")
	}
	s.SetTopologyExclusion(second.id, false, false)
	if frames := drainRequested(t, s, second); !slices.Contains(frames, wire.KindDatagram) {
		t.Fatalf("alternate pipe sent %v", frames)
	}
}

func TestDatagramCopyWaitsForInFlightWrite(t *testing.T) {
	s, first, second := quietWriterPipes()
	s.peerMaxDgram = 65535
	s.feat.DatagramFamily = true
	lane := second.out.lanes[0]
	lane.writing = true
	options := DatagramOptions{Copy: DatagramDuplicate, Copies: 2}
	if err := s.SendDatagramWith(nil, 0, []byte("payload"), options, s.now); err != nil {
		t.Fatal(err)
	}
	if frames := drainRequested(t, s, first); !slices.Contains(frames, wire.KindDatagram) {
		t.Fatalf("first pipe sent %v", frames)
	}
	if len(second.datagramCopies) != 1 {
		t.Fatalf("the busy pipe retained %d copies, want 1", len(second.datagramCopies))
	}
	lane.writing = false
	if frames := drainRequested(t, s, second); !slices.Contains(frames, wire.KindDatagram) {
		t.Fatalf("second pipe sent %v after write completion", frames)
	}
	if len(second.datagramCopies) != 0 {
		t.Fatal("the copy did not leave after completion")
	}
}
