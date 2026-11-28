package core

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func reportDatagramLoss(t *testing.T, s *Session, p *Pipe, channel bool, generation, lost uint64) {
	t.Helper()
	if err := s.ReportDatagramLoss(p.id, generation, channel, s.now, 100, lost); err != nil {
		t.Fatal(err)
	}
}

func TestDatagramEvidenceGenerationsByRepresentation(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.gates.DatagramAck = true
	s.datagramDispatch(p, false, 1)
	s.datagramDispatch(p, true, 2)
	s.SetNow(4 * Time(Millisecond))

	if err := s.ReportDatagramLoss(p.id, 0, false, s.now, 100, 6); !errors.Is(err, ErrDatagramEvidence) {
		t.Fatalf("generation 0: got %v", err)
	}
	if err := s.ReportDatagramLoss(p.id, 5, false, Never, 0, 0); err != nil {
		t.Fatal(err)
	}
	s.onDatagramAck(&wire.DatagramAck{PipeID: p.id, Largest: 1})
	record := &p.datagramFeedback.sent[0]
	native := &p.datagramFeedback.sent[1]
	if record.samples != 1 || record.source.revision != 0 || record.loss.set || native.source.generation != 0 {
		t.Fatal("first declaration changed record evidence or native source")
	}

	reportDatagramLoss(t, s, p, false, 5, 6)
	reportDatagramLoss(t, s, p, true, 1, 2)
	if native.source.generation != 1 || len(native.entries) != 0 || !record.loss.fresh(s.now) {
		t.Fatal("first native observation missed its generation or reset record evidence")
	}

	earlier := s.now.Add(-Millisecond)
	if err := s.ReportDatagramLoss(p.id, 5, false, earlier, 100, 1); !errors.Is(err, ErrDatagramEvidence) {
		t.Fatalf("regressed observation in generation 5: %v", err)
	}
	if err := s.ReportDatagramLoss(p.id, 6, false, earlier, 100, 1); err != nil {
		t.Fatalf("first observation in generation 6: %v", err)
	}
	if record.samples != 0 || record.loss.at != earlier || native.loss.lost != 2 {
		t.Fatal("record generation change did not reset exactly its own evidence")
	}
	s.onDatagramAck(&wire.DatagramAck{PipeID: p.id, Largest: 1})
	if record.samples != 0 {
		t.Fatal("ACK for previous generation replenished timing evidence")
	}
}

func TestDatagramEvidenceExplicitFirstReplacementClearsOnlyItsRepresentation(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.gates.DatagramAck = true
	s.datagramDispatch(p, false, 1)
	s.datagramDispatch(p, true, 2)
	if err := s.SetDatagramSourceGeneration(p.id, false, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDatagramSourceGeneration(p.id, false, 1); !errors.Is(err, ErrDatagramEvidence) {
		t.Fatalf("unchanged generation: %v", err)
	}
	s.SetNow(Time(Millisecond))
	s.onDatagramAck(&wire.DatagramAck{PipeID: p.id, Largest: 1})
	s.onDatagramAck(&wire.DatagramAck{PipeID: p.id, Channel: 1, Largest: 2})
	if p.datagramFeedback.sent[0].samples != 0 || p.datagramFeedback.sent[1].samples != 1 {
		t.Fatal("explicit generation 1 replacement did not retire just the record evidence")
	}
}

func TestDatagramEvidenceExpiresWhenChannelChanges(t *testing.T) {
	s, p := channelSession(t, &testChannel{id: 4, hasID: true, max: 1200}, true)
	s.PollChannel(p.id, p.chUse, nil, s.now)
	reportDatagramLoss(t, s, p, false, 1, 6)
	reportDatagramLoss(t, s, p, true, 1, 6)

	use := p.chUse
	use.Room /= 2
	s.PollChannel(p.id, use, nil, s.now)
	s.SetNow(Time(Millisecond))
	reportDatagramLoss(t, s, p, true, 1, 6)
	s.PollChannel(p.id, ChannelUse{}, nil, s.now)
	use.ID += 4
	s.PollChannel(p.id, use, nil, s.now)

	native := &p.datagramFeedback.sent[1]
	if native.loss.set || native.samples != 0 || !p.datagramFeedback.sent[0].loss.fresh(s.now) {
		t.Fatal("native replacement did not clear exactly native evidence")
	}
	if err := s.ReportDatagramLoss(p.id, 1, true, s.now, 100, 0); !errors.Is(err, ErrDatagramStaleSource) {
		t.Fatalf("replaced native generation: %v", err)
	}
	reportDatagramLoss(t, s, p, false, 1, 6)
	reportDatagramLoss(t, s, p, true, 2, 6)
}

func TestDatagramEvidenceExpiresWhenAttachmentChanges(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.peerSeen = false
	reportDatagramLoss(t, s, p, false, 1, 6)
	reportDatagramLoss(t, s, p, true, 2, 6)

	if !s.RestartEarly(CarrierInfo{Channel: true}, nil, false, Time(Millisecond)) {
		t.Fatal("same attachment restart refused")
	}
	reportDatagramLoss(t, s, p, false, 1, 6)
	reportDatagramLoss(t, s, p, true, 2, 6)
	if !s.RestartEarly(CarrierInfo{}, nil, true, 2*Time(Millisecond)) {
		t.Fatal("replacement attachment refused")
	}
	if err := s.ReportDatagramLoss(p.id, 1, false, s.now, 100, 6); !errors.Is(err, ErrDatagramStaleSource) {
		t.Fatalf("replaced record source: %v", err)
	}
	if err := s.ReportDatagramLoss(p.id, 2, true, s.now, 100, 6); !errors.Is(err, ErrDatagramStaleSource) {
		t.Fatalf("removed native source: %v", err)
	}
	reportDatagramLoss(t, s, p, false, 2, 6)
}

func TestDatagramEvidenceExpiresWhenPhysicalConnectionChanges(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.SetCarrierConn(p.id, "first")
	reportDatagramLoss(t, s, p, false, 1, 6)
	reportDatagramLoss(t, s, p, true, 2, 6)
	s.SetCarrierConn(p.id, "first")
	s.SetNow(Time(Millisecond))
	reportDatagramLoss(t, s, p, true, 2, 6)

	s.SetCarrierConn(p.id, "second")
	if err := s.ReportDatagramLoss(p.id, 1, false, s.now, 100, 6); !errors.Is(err, ErrDatagramStaleSource) {
		t.Fatalf("old record source on the replaced connection: %v", err)
	}
	if err := s.ReportDatagramLoss(p.id, 2, true, s.now, 100, 6); !errors.Is(err, ErrDatagramStaleSource) {
		t.Fatalf("old native source on the replaced connection: %v", err)
	}
	reportDatagramLoss(t, s, p, false, 2, 6)
	reportDatagramLoss(t, s, p, true, 3, 6)
}

func adaptiveDatagramPaths(t *testing.T) (*Session, *Pipe, *Pipe, *Pipe) {
	t.Helper()
	s, a := channelSession(t, &testChannel{max: 1200}, true)
	s.mp = &multiPath{}
	a.mp = &attach{out: OutActive, in: InOpen, weight: 1}
	b, c := capacityExtra(s), capacityExtra(s)
	for i, p := range []*Pipe{a, b, c} {
		lane := p.out.lanes[0]
		lane.info.Channel, lane.confirmed = true, true
		lane.info.FateDomain = string(rune('a' + i))
		p.chUse = ChannelUse{Limit: 1200, Room: 1200}
	}
	return s, a, b, c
}

func TestDatagramAdaptiveKeepsIndependentPairLatches(t *testing.T) {
	s, a, b, c := adaptiveDatagramPaths(t)
	for _, native := range []bool{false, true} {
		reportDatagramLoss(t, s, a, native, 1, 6)
		reportDatagramLoss(t, s, b, native, 1, 0)
		reportDatagramLoss(t, s, c, native, 1, 0)
	}
	for _, target := range []*Pipe{b, c} {
		if !s.adaptiveDatagramPair(a, target, true, true) {
			t.Fatal("qualifying native pair did not latch")
		}
	}
	if !s.adaptiveDatagramPair(a, b, true, false) {
		t.Fatal("record alternative did not latch")
	}
	s.SetNow(Time(adaptiveHold))
	reportDatagramLoss(t, s, a, true, 1, 3) // retention threshold only
	if !s.adaptiveDatagramPair(a, b, true, true) || !s.adaptiveDatagramPair(a, c, true, true) ||
		!s.adaptiveDatagramPair(a, b, true, false) {
		t.Fatal("evaluating another pair erased a live latch")
	}
	reportDatagramLoss(t, s, b, true, 2, 0)
	if s.adaptiveDatagramPair(a, b, true, true) || !s.adaptiveDatagramPair(a, b, true, false) ||
		!s.adaptiveDatagramPair(a, c, true, true) {
		t.Fatal("source switch did not clear exactly its dependent latch")
	}
}

func TestDatagramAdaptiveFirstDeclarationKeepsTimingLatch(t *testing.T) {
	s, a, b, _ := adaptiveDatagramPaths(t)
	s.gates.DatagramAck = true
	for seq := uint64(1); seq <= 3; seq++ {
		for _, p := range []*Pipe{a, b} {
			s.datagramDispatch(p, true, seq)
		}
		s.SetNow(s.now.Add(10 * Millisecond))
		s.onDatagramAck(&wire.DatagramAck{PipeID: a.id, Channel: 1, Largest: seq})
		s.onDatagramAck(&wire.DatagramAck{PipeID: b.id, Channel: 1, Largest: seq, Delay: 8000})
	}
	if !s.adaptiveDatagramPair(a, b, true, true) {
		t.Fatal("timing evidence did not qualify")
	}
	key := datagramPairKey{b, true, true}
	before := a.datagramFeedback.pairs[key]
	if err := s.ReportDatagramLoss(a.id, 1, true, Never, 0, 0); err != nil {
		t.Fatal(err)
	}
	if a.datagramFeedback.pairs[key] != before || !s.adaptiveDatagramPair(a, b, true, true) ||
		a.datagramFeedback.sent[1].samples != 3 {
		t.Fatal("first source declaration changed timing evidence or its latch")
	}
}

func TestDatagramAdaptiveDoesNotBridgeEvidenceGaps(t *testing.T) {
	s, a, b, _ := adaptiveDatagramPaths(t)
	reportDatagramLoss(t, s, a, true, 1, 6)
	reportDatagramLoss(t, s, b, true, 1, 0)
	if !s.adaptiveDatagramPair(a, b, true, true) {
		t.Fatal("loss evidence did not qualify")
	}
	s.SetNow(Time(datagramObservationWindow))
	reportDatagramLoss(t, s, a, true, 1, 3)
	reportDatagramLoss(t, s, b, true, 1, 0)
	if s.adaptiveDatagramPair(a, b, true, true) {
		t.Fatal("fresh samples restored a latch across an evidence gap with retention-only loss")
	}
}

func TestDatagramAdaptiveBudgetAndUrgentLeaveLatchesUntouched(t *testing.T) {
	s, a, b, c := adaptiveDatagramPaths(t)
	reportDatagramLoss(t, s, a, true, 1, 6)
	reportDatagramLoss(t, s, b, true, 1, 0)
	reportDatagramLoss(t, s, c, true, 1, 0)
	send := func(urgent bool) {
		t.Helper()
		o := DatagramOptions{Copy: DatagramAdaptive, Urgent: urgent, HasUrgent: true}
		if err := s.SendDatagramWith(nil, 0, []byte("x"), o, s.now); err != nil {
			t.Fatal(err)
		}
		out, _ := s.PollChannel(a.id, a.chUse, nil, s.now)
		if len(out) != 1 {
			t.Fatalf("original dispatches: %d", len(out))
		}
		s.ChannelDone(out, s.now)
	}
	send(false)
	if len(b.datagramCopies) != 1 || len(c.datagramCopies) != 0 {
		t.Fatal("queued replica did not use the first qualifying alternative")
	}
	key := datagramPairKey{b, true, true}
	prior := a.datagramFeedback.pairs[key]
	if !prior.loss.active || len(a.datagramFeedback.pairs) != 1 {
		t.Fatal("selection updated a pair beyond the chosen alternative")
	}

	s.SetNow(Time(adaptiveHold))
	reportDatagramLoss(t, s, a, true, 1, 0) // comparing now would release the latch
	s.replicaBytes(s.replicaRoom(), true)
	if out, _ := s.PollChannel(b.id, b.chUse, nil, s.now); len(out) != 0 || len(b.datagramCopies) != 0 {
		t.Fatal("replica dispatched without budget")
	}
	send(false)
	if a.datagramFeedback.pairs[key] != prior || len(a.datagramFeedback.pairs) != 1 ||
		len(b.datagramCopies) != 0 || s.SessionStats().DatagramCopyBudgetDropped != 2 {
		t.Fatal("a budget refusal evaluated an existing latch")
	}
	s.replicaBytes(100, false)
	send(true)
	out, _ := s.PollChannel(b.id, b.chUse, nil, s.now)
	if len(out) != 1 || a.datagramFeedback.pairs[key] != prior {
		t.Fatal("Urgent dispatch failed to bypass comparison while preserving its latch")
	}
	s.ChannelDone(out, s.now)
}

func TestSessionRejectsUnresolvedDatagramCopyLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DatagramCopyLimit = 0
	defer func() {
		if recover() == nil {
			t.Fatal("core accepted a zero resolved copy limit")
		}
	}()
	New(cfg, 0)
}

func TestDatagramAdaptiveDispatchClearsIneligibleTargetPair(t *testing.T) {
	s, a, b, c := adaptiveDatagramPaths(t)
	reportDatagramLoss(t, s, a, true, 1, 6)
	reportDatagramLoss(t, s, b, true, 1, 0)
	reportDatagramLoss(t, s, c, true, 1, 0)
	if !s.adaptiveDatagramPair(a, c, true, true) {
		t.Fatal("separate alternative did not qualify")
	}
	err := s.SendDatagramWith(nil, 0, []byte("x"), DatagramOptions{Copy: DatagramAdaptive}, s.now)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := s.PollChannel(a.id, a.chUse, nil, s.now)
	if len(out) != 1 || len(b.datagramCopies) != 1 {
		t.Fatal("preferred copy was not queued")
	}
	s.ChannelDone(out, s.now)
	b.out.lanes[0].info.FateDomain = ""
	out, _ = s.PollChannel(b.id, b.chUse, nil, s.now)
	if len(out) != 0 || len(b.datagramCopies) != 0 {
		t.Fatal("ineligible alternative dispatched")
	}
	if len(a.datagramFeedback.pairs) != 1 || !a.datagramFeedback.pairs[datagramPairKey{c, true, true}].loss.active {
		t.Fatal("dispatch invalidated more than its ineligible pair")
	}
}
