package core

import (
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

type fallbackDelayShaper struct{ delayShaper }

func (*fallbackDelayShaper) Capabilities() shaping.Caps {
	return shaping.CapDatagramPlan | shaping.CapPlan
}

func (sh *fallbackDelayShaper) PlanDatagram(_ *shaping.DatagramView, plan *shaping.DatagramPlan) {
	plan.Delay = sh.delay
}

func TestReliableDatagramFlowDelay(t *testing.T) {
	s, p := channelSession(t, nil, false)
	p.out.lane(0).info.MaxRecord = 1200
	profile := shaping.Lightweight()
	s.SetShapeLimits(shaping.Limits{Profile: &profile})
	s.SetPipeShaper(p.id, &delayShaper{delay: 2 * time.Millisecond})
	for range 2 {
		if err := s.SendDatagram(nil, 0, make([]byte, 900)); err != nil {
			t.Fatal(err)
		}
	}
	b := GetBatch()
	defer PutBatch(b)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, 0) {
		t.Fatal("record escaped its initial delay")
	}
	due := Time(2 * Millisecond)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, due) || len(b.recs) != 2 {
		t.Fatalf("at the shared deadline dispatched %d records", len(b.recs))
	}
	s.WriteDone(b, b.Len(), nil, due)
	if got := s.ShapingStats(); got.MaxAddedDelay != 2*time.Millisecond || got.StallSpent != 4*time.Millisecond {
		t.Fatalf("delay meters: %+v", got)
	}
}

func TestDatagramFlowDelaySurvivesNativeFallback(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	profile := shaping.Lightweight()
	s.SetShapeLimits(shaping.Limits{Profile: &profile})
	s.SetPipeShaper(p.id, &fallbackDelayShaper{delayShaper{delay: 2 * time.Millisecond}})
	if err := s.SendDatagram(nil, 0, []byte("fallback")); err != nil {
		t.Fatal(err)
	}
	if out, _ := s.PollChannel(p.id, p.chUse, nil, 0); len(out) != 0 {
		t.Fatal("native send escaped its delay")
	}
	fallback := Time(1500 * Microsecond)
	s.PollChannel(p.id, ChannelUse{}, nil, fallback)
	b := GetBatch()
	defer PutBatch(b)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, fallback) {
		t.Fatal("fallback escaped its remaining delay")
	}
	due := Time(2 * Millisecond)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, due) {
		t.Fatal("fallback exceeded its original deadline")
	}
	s.WriteDone(b, b.Len(), nil, due)
	if got := s.ShapingStats(); got.MaxAddedDelay != 2*time.Millisecond || got.StallSpent != 2*time.Millisecond {
		t.Fatalf("delay meters: %+v", got)
	}
}

func TestNativeInFlightKeepsDatagramQueueSlot(t *testing.T) {
	for _, name := range []string{"session", "flow"} {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			switch name {
			case "session":
				cfg.DgramSendQ = 1
			case "flow":
				cfg.DatagramFlowQ = 1
			}
			s, p := channelSessionWithConfig(t, cfg, &testChannel{max: 1200}, true)
			if err := s.SendDatagram(nil, 0, []byte("first")); err != nil {
				t.Fatal(err)
			}
			out, _ := s.PollChannel(p.id, p.chUse, nil, 0)
			if len(out) != 1 {
				t.Fatalf("dispatched %d datagrams", len(out))
			}
			s.SetPipeShaper(p.id, &delayShaper{})
			if err := s.SendDatagram(nil, 0, []byte("second")); !errors.Is(err, DatagramDroppedError{Reason: DatagramDropQueueOverflow}) {
				t.Fatalf("admission with a full queue: %v", err)
			}
			s.ChannelDone(out, s.now)
			if err := s.SendDatagram(nil, 0, []byte("third")); err != nil {
				t.Fatal(err)
			}
			s.ChannelDone(out, s.now)
			if s.dg.nativeCount != 0 || s.dg.nativeBytes != 0 || s.dg.sendCount != 1 || s.dg.flows[nil].nativeCount != 0 {
				t.Fatal("duplicate completion changed queue accounting")
			}
		})
	}
}

func TestShapingGateExcludesUnwritableTime(t *testing.T) {
	profile := shaping.Lightweight()
	s, p, st := budgetSession(t, shaping.Limits{}, shaping.Budget{Profile: &profile}, &fixedPlan{})
	if _, err := s.Write(st, []byte("pending"), false); err != nil {
		t.Fatal(err)
	}
	start := s.now
	ps := p.sh
	ps.applyGate(&shaping.Effects{PauseUntil: time.Duration(start.Add(2 * Millisecond))})
	b := GetBatch()
	defer PutBatch(b)
	s.PollTransmit(p.id, 0, b, DefaultBatch, start)
	if b.Len() > 0 {
		s.WriteDone(b, b.Len(), nil, start)
	}
	blocked := start.Add(500 * Microsecond)
	s.SetWritable(p.id, 0, false, blocked)
	s.HandleTimeout(start.Add(2 * Millisecond))
	m := s.ShapingStats()
	if m.MaxAddedDelay != 500*time.Microsecond || m.StallSpent != 500*time.Microsecond {
		t.Fatalf("unwritable interval charged to shaping: %+v", m)
	}
}

func TestPipeAckCreditsOnlyProfileBaseline(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.ack = newAckState(s)
	p.in.lanes[0].pos = 1
	profile := shaping.Lightweight()
	if !s.SetShapeBudget(shaping.Budget{Profile: &profile, Wire: &shaping.Account{Ceiling: 100, PerMB: 1_000_000}}) {
		t.Fatal("install budget")
	}
	b := GetBatch()
	defer PutBatch(b)
	var baseline uint64
	for range 2 {
		s.ack.onRecord(p, 1, recordFacts{elicit: true}, s.now)
		if !s.PollTransmit(p.id, 0, b, DefaultBatch, s.ack.deadline()) {
			t.Fatal("missing ACK")
		}
		baseline += uint64(b.Len())
		s.WriteDone(b, b.Len(), nil, s.now)
	}
	m := s.ShapeMeters()
	if m.EarnedBytes != 0 || m.WireLeft != 0 || m.Profile.BaselineBytes != baseline {
		t.Fatalf("ACK accounting: %+v, want baseline %d", m, baseline)
	}
}
