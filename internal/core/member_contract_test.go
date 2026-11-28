package core

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestPairedEvidenceRetainsReceiveProbeUntilLocalACK(t *testing.T) {
	s, p := probeSession(DefaultConfig())
	s.ack = newAckState(s)
	s.syncPipeHealth(p)
	p.out.snd.so = 16
	p.out.lanes[0].pos = 16
	p.mp.pingEnds = []uint64{16}
	p.mp.health.receive.probeEnd = 16
	e := wire.AckEntry{PipeID: p.id, Position: 16}
	if err := s.mpAckEntry(e, s.now, nil); err != nil {
		t.Fatal(err)
	}
	h := s.pipeHealth(p)
	if h.Send.Proof == HealthProofNone || h.Receive.Proof != HealthProofNone {
		t.Fatalf("alternate ACK evidence = %+v", h)
	}
	if err := s.mpAckEntry(e, s.now, p); err != nil {
		t.Fatal(err)
	}
	h = s.pipeHealth(p)
	if h.Receive.Proof == HealthProofNone {
		t.Fatalf("same-path ACK did not prove receive: %+v", h)
	}
}

func TestPhysicalCapacityDeduplicatesSharedAttachment(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SessionPolicy.Limits.LocalCarriers = 1
	s := New(cfg, 0)
	shared := CarrierKeys{"physical"}
	distinct := CarrierKeys{"another"}
	s.AcquireCarriers(shared)
	if !s.CanAcquireCarriers(shared) || s.CanAcquireCarriers(distinct) {
		t.Fatal("a second attachment may share the existing connection only")
	}
	s.AcquireCarriers(shared)
	s.ReleaseCarriers(shared)
	if s.CarrierCount() != 1 || s.CanAcquireCarriers(distinct) {
		t.Fatal("first I/O tail released the second attachment's connection")
	}
	s.ReleaseCarriers(shared)
	if s.CarrierCount() != 0 || !s.CanAcquireCarriers(distinct) {
		t.Fatal("the final I/O tail did not release the physical slot")
	}
}

func TestMemberRotationCountsCompletedRecordPrefix(t *testing.T) {
	s, p := channelSession(t, nil, false)
	p.out.lane(0).info.Framing = FramingRecords
	s.peerMaxRecord = 256
	s.SetMemberRotation(p.member.ID, 1, 0)
	for range 2 {
		if err := s.SendDatagram(nil, 0, make([]byte, 192)); err != nil {
			t.Fatal(err)
		}
	}
	var b Batch
	if !s.PollTransmit(p.id, 0, &b, DefaultBatch, s.now) || len(b.recs) != 2 {
		t.Fatalf("expected a batch of two records, got %d", len(b.recs))
	}
	if !b.NeedsProgress() {
		t.Fatal("member rotation did not request record write progress")
	}
	first := b.recs[0].size
	s.WriteDone(&b, first, errors.New("second record refused"), s.now)
	if got, want := p.member.rotation.sent, uint64(recordPositionBytes(first)); got != want {
		t.Fatalf("rotation counted %d bytes, want %d", got, want)
	}
}
