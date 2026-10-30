package core

import "testing"

func capacitySession() *Session {
	s := New(DefaultConfig(), 0)
	s.AttachBootstrap(CarrierInfo{}, CarrierInfo{}, nil)
	s.peerSeen, s.state = true, StateActive
	return s
}
func capacityExtra(s *Session) *Pipe {
	p := &Pipe{id: uint64(len(s.pipes) * 2), mp: &attach{out: outActive, in: inOpen, probOpt: -1, weight: 1}, out: &outDir{lanes: []*outLane{{}}}}
	s.pipes = append(s.pipes, p)
	return p
}
func capacityMP() *Session {
	s := capacitySession()
	s.mp = &multiPath{}
	s.pipes[0].mp = &attach{out: outActive, in: inOpen, probOpt: -1, weight: 1}
	return s
}
func measuredRate(p *Pipe, rate float64) {
	e := &p.out.snd.re
	e.blockedNow = true
	e.sample(rate, false, 0, 0)
	e.blockedNow = false
}
func takeCapacity(s *Session) (Event, bool) {
	for {
		e, ok := s.PollEvent()
		if !ok {
			return Event{}, false
		}
		if e.Kind == EvCapacityBelow || e.Kind == EvCapacityRestored {
			return e, true
		}
	}
}
func wantCapacity(t *testing.T, s *Session, kind EventKind, rate float64) {
	t.Helper()
	e, ok := takeCapacity(s)
	if !ok || e.Kind != kind || e.Rate != rate {
		t.Fatalf("capacity event %+v, present %v; want kind %v rate %v", e, ok, kind, rate)
	}
}
func TestCapacityCountsAcceptedApplicationLimitedSampleWithoutEndingPlacement(t *testing.T) {
	s := capacitySession()
	p := s.pipes[0]
	s.SetCapacityFloor(CapacityFloor{Rate: 1500000, RateRatio: 1.01})
	wantCapacity(t, s, EvCapacityBelow, 0)
	p.out.snd.re.sample(2000000, true, 0, 1)
	if p.out.snd.re.measured {
		t.Fatal("application-limited sample ended placement")
	}
	s.qualitySample(p)
	wantCapacity(t, s, EvCapacityRestored, 2000000)
}
func TestCapacityRejectedSampleDoesNotCountTheInitialEstimate(t *testing.T) {
	s := capacitySession()
	s.pipes[0].out.snd.re.sample(500000, true, 0, 1)
	s.SetCapacityFloor(CapacityFloor{Rate: 100000})
	wantCapacity(t, s, EvCapacityBelow, 0)
}
func TestCapacityFollowsProbationExpiryAndExplicitDisable(t *testing.T) {
	s := capacityMP()
	p := capacityExtra(s)
	measuredRate(s.pipes[0], 2000000)
	measuredRate(p, 3000000)
	p.mp.prob.on = true
	p.mp.probOpt = 10 * Millisecond
	s.SetCapacityFloor(CapacityFloor{Rate: 4000000, RateRatio: 1.01})
	wantCapacity(t, s, EvCapacityBelow, 2000000)
	s.now = Time(10 * Millisecond)
	s.probationTimers(s.now, true)
	wantCapacity(t, s, EvCapacityRestored, 5000000)
	p.mp.prob.on = true
	s.checkCapacityFloor()
	wantCapacity(t, s, EvCapacityBelow, 2000000)
	zero := Duration(0)
	if err := s.SetPipe(p.id, PipeSettings{Probation: &zero}); err != nil {
		t.Fatal(err)
	}
	wantCapacity(t, s, EvCapacityRestored, 5000000)
}
func TestCapacityRevalidationExcludesTransientPathsButKeepsLastPathFallback(t *testing.T) {
	s := capacityMP()
	p := capacityExtra(s)
	measuredRate(s.pipes[0], 2000000)
	measuredRate(p, 3000000)
	s.SetCapacityFloor(CapacityFloor{Rate: 4000000, RateRatio: 1.01})
	if err := s.RevalidatePipes(p.id, false, s.now); err != nil {
		t.Fatal(err)
	}
	wantCapacity(t, s, EvCapacityBelow, 2000000)
	s.endRevalidation(p)
	wantCapacity(t, s, EvCapacityRestored, 5000000)
	standby := true
	s.pipes[0].mp.sendStandby = &standby
	p.mp.reval = true
	s.checkCapacityFloor()
	wantCapacity(t, s, EvCapacityBelow, 0)
	s.pipes[0].mp.reval = true
	s.SetCapacityFloor(CapacityFloor{Rate: 2500000, RateRatio: 1.01})
	if e, ok := takeCapacity(s); ok {
		t.Fatalf("fallback should count the non-standby path: %+v", e)
	}
}
func TestCapacityDrainCannotTemporarilyRestoreARevalidatingPath(t *testing.T) {
	s := capacityMP()
	p := capacityExtra(s)
	measuredRate(s.pipes[0], 2000000)
	measuredRate(p, 3000000)
	p.mp.reval = true
	s.SetCapacityFloor(CapacityFloor{Rate: 4000000, RateRatio: 1.01})
	wantCapacity(t, s, EvCapacityBelow, 2000000)
	s.startDrain(p)
	if e, ok := takeCapacity(s); ok {
		t.Fatalf("draining produced transient capacity event %+v", e)
	}
}
func TestCapacityPenaltyEntryAndLastAcknowledgementNeedNoLaterTraffic(t *testing.T) {
	s := capacityMP()
	p := capacityExtra(s)
	measuredRate(s.pipes[0], 1000000)
	measuredRate(p, 4000000)
	s.SetCapacityFloor(CapacityFloor{Rate: 2500000, RateRatio: 1.01})
	p.out.snd.so = 100
	s.penalize(p)
	wantCapacity(t, s, EvCapacityBelow, 1000000)
	p.out.snd.cum = 99
	s.penaltyAcked(p)
	if e, ok := takeCapacity(s); ok {
		t.Fatalf("early restoration %+v", e)
	}
	p.out.snd.cum = 100
	s.penaltyAcked(p)
	wantCapacity(t, s, EvCapacityRestored, 3000000)
}

func TestCapacityStateAndPriorityChangesReportWithoutAnotherAcknowledgement(t *testing.T) {
	s := capacityMP()
	p := s.pipes[0]
	measuredRate(p, 2000000)
	s.SetCapacityFloor(CapacityFloor{Rate: 1500000, RateRatio: 1.01})
	p.mp.out = outFailed
	s.emitPipeState(p)
	wantCapacity(t, s, EvCapacityBelow, 0)
	p.mp.out = outActive
	s.emitPipeState(p)
	wantCapacity(t, s, EvCapacityRestored, 2000000)
	standby := true
	p.mp.sendStandby = &standby
	s.emitPriority(p)
	wantCapacity(t, s, EvCapacityBelow, 0)
	standby = false
	s.emitPriority(p)
	wantCapacity(t, s, EvCapacityRestored, 2000000)
	s.SetCapacityFloor(CapacityFloor{})
	if s.q.floorSet || s.q.floor != nil {
		t.Fatal("zero floor retained monitoring state")
	}
	p.mp.out = outFailed
	s.emitPipeState(p)
	if e, ok := takeCapacity(s); ok {
		t.Fatalf("disabled floor reported %+v", e)
	}
}

func TestExpiredProbationRemainsAnImmediateDeadline(t *testing.T) {
	s := capacityMP()
	p := capacityExtra(s)
	p.mp.prob.on = true
	p.mp.probOpt = 10 * Millisecond
	s.now = Time(20 * Millisecond)
	next := s.probationTimers(s.now, false)
	if next == Never || next > s.now {
		t.Fatalf("expired probation omitted from next deadline: %v", next)
	}
	if !p.mp.prob.on {
		t.Fatal("deadline query mutated probation")
	}
	s.probationTimers(s.now, true)
	if p.mp.prob.on {
		t.Fatal("due probation was not cleared")
	}
}
