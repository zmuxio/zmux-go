package core

import "testing"

func observePath(s *Session, p *Pipe, rtt Duration, rate float64) {
	s.enablePathQuality(p)
	p.mp.rtt.update(rtt, 0, 0, s.now, false)
	p.mp.quality.sampleRTT(rtt, 0, s.now, false)
	p.out.snd.re.sample(rate, false, true, 0, s.now, p.mp.quality)
}

func setPathTime(s *Session, at Duration) { s.SetNow(Time(at)) }

func takePathQuality(s *Session, id uint64) (Event, bool) {
	for ev, ok := s.PollEvent(); ok; ev, ok = s.PollEvent() {
		if ev.Kind == EvPipePathQuality && ev.Pipe == id {
			return ev, true
		}
	}
	return Event{}, false
}

func qualityTestSession() (*Session, *Pipe, *Pipe) {
	s, sibling := statsMP()
	s.feat.Multipath, s.ack = true, newAckState(s)
	s.cfg.Probation = 2 * Second
	s.cfg.Policy = PolicySpreadETA
	s.EnablePathAdvice()
	p := s.pipes[0]
	p.mp.fate, sibling.mp.fate = 7, 7
	setPathTime(s, Second)
	observePath(s, p, 100*Millisecond, 1e6)
	observePath(s, sibling, 160*Millisecond, 1e6)
	s.mpHandleTimeout(s.now)
	return s, p, sibling
}

func bestQualityPipe(s *Session) *Pipe {
	g := s.buildGate(&s.mp.gateBuf)
	return s.fastestIn(g, 0, 256, nil)
}

func TestPathQualityDegradesAndRecovers(t *testing.T) {
	s, p, sibling := qualityTestSession()
	if p.mp.out != OutActive || bestQualityPipe(s) != p {
		t.Fatal("healthy pipe did not win placement")
	}

	p.out.snd.so = 1024
	p.out.lanes[0].log = append(p.out.lanes[0].log, retained{d: frameDesc{kind: dStream, n: 1024}})
	for _, at := range []Duration{2 * Second, 3 * Second} {
		setPathTime(s, at)
		observePath(s, p, Second, 12)
		p.out.snd.cum++ // the slow pipe continues to make progress
		observePath(s, sibling, 160*Millisecond, 1e6)
		s.mpHandleTimeout(s.now)
		if p.mp.out != OutActive || bestQualityPipe(s) != sibling || s.stats.Reinjections != 0 {
			t.Fatal("performance deterioration entered native suspicion or retained placement preference")
		}
	}
	e, ok := takePathQuality(s, p.id)
	if !ok || e.PathQuality != PathQualityHealthySibling || e.Wait < Second {
		t.Fatalf("connection-specific advice missing: %+v", e)
	}
	episode, _, ok := s.ReplacementAdvice(p.id)
	if !ok {
		t.Fatal("current loaded episode was not revalidated")
	}
	setPathTime(s, 4*Second)
	for range 16 {
		observePath(s, p, 100*Millisecond, 1e6)
	}
	s.mpHandleTimeout(s.now)
	if _, _, ok := s.ReplacementAdvice(p.id); ok {
		t.Fatal("recovered path authorized another dial")
	}
	if e, ok := takePathQuality(s, p.id); ok {
		t.Fatalf("one healthy interval published recovery: %+v", e)
	}
	setPathTime(s, 5*Second)
	observePath(s, p, 100*Millisecond, 1e6)
	s.mpHandleTimeout(s.now)
	if e, ok := takePathQuality(s, p.id); !ok || e.PathQuality != PathQualityRestored {
		t.Fatalf("sustained fresh measurements did not restore quality: %+v", e)
	}
	for _, at := range []Duration{6 * Second, 7 * Second} {
		setPathTime(s, at)
		observePath(s, p, Second, 12)
		observePath(s, sibling, 160*Millisecond, 1e6)
		s.mpHandleTimeout(s.now)
	}
	if next, _, ok := s.ReplacementAdvice(p.id); !ok || next == episode {
		t.Fatal("later deterioration did not require a new episode")
	}
}

func TestPathQualitySharedDeteriorationOnlyAdvises(t *testing.T) {
	s, p, sibling := qualityTestSession()
	alternate := capacityExtra(s)
	alternate.mp.fate = 8
	for _, at := range []Duration{2 * Second, 3 * Second} {
		setPathTime(s, at)
		observePath(s, p, Second, 12)
		observePath(s, sibling, Second, 12)
		observePath(s, alternate, 250*Millisecond, 1e6)
		s.mpHandleTimeout(s.now)
	}
	for _, q := range []*Pipe{p, sibling} {
		e, ok := takePathQuality(s, q.id)
		if !ok || e.PathQuality != PathQualityShared || e.Wait != 0 || q.mp.out != OutActive {
			t.Fatalf("shared path triggered replacement or suspicion: %+v", e)
		}
	}
	if bestQualityPipe(s) != alternate {
		t.Fatal("ETA did not downweight the shared path")
	}
}

func TestPathQualityIsolatedCarrierQueue(t *testing.T) {
	s, p, sibling := qualityTestSession()
	p.mp.fate = 0
	s.SetCarrierUnsent(p.id, 0, 2<<20, s.now)
	for _, at := range []Duration{2 * Second, 3 * Second} {
		setPathTime(s, at)
		observePath(s, p, 100*Millisecond, 1e6)
		observePath(s, sibling, 160*Millisecond, 1e6)
		s.mpHandleTimeout(s.now)
	}
	e, ok := takePathQuality(s, p.id)
	if !ok || e.PathQuality != PathQualityLocalStall || e.Wait <= 0 || e.Count != 2<<20 || p.mp.out != OutActive {
		t.Fatalf("isolated queue did not produce connection advice: %+v", e)
	}
}

func TestPathQualityMeasurementExpiry(t *testing.T) {
	s, p, sibling := qualityTestSession()
	bornSlow := capacityExtra(s)
	observePath(s, bornSlow, Second, 12)
	observePath(s, p, 100*Millisecond, 20e6)
	s.SetCarrierUnsent(p.id, 0, 1024, s.now)
	if bestQualityPipe(s) != p {
		t.Fatal("a pipe born slow won placement")
	}
	setPathTime(s, 32*Second)
	observePath(s, sibling, 160*Millisecond, 1e6)
	if bestQualityPipe(s) != sibling || s.recentRate(p) != initialRate {
		t.Fatal("expired low RTT or high delivery rate still wins placement")
	}
	if rtt := bornSlow.mp.currentRTT(s.now); rtt != unobservedRTT {
		t.Fatalf("expired slow RTT was retained: %v", rtt)
	}
	if _, fresh := p.mp.quality.floor(s.now); fresh {
		t.Fatal("expired RTT minimum remained a placement floor")
	}
	if queue, known := s.carrierUnsent(p); known || queue != 0 {
		t.Fatal("expired carrier queue remained evidence of local backlog")
	}
}

func TestPathQualityFloorSurvivesSustainedCongestion(t *testing.T) {
	s, p, _ := qualityTestSession()
	q := p.mp.quality
	*q = pathQuality{}
	q.sampleRTT(200*Millisecond, 25*Millisecond, s.now, false)
	// Establish the floor just before the first bucket rotates.
	setPathTime(s, 45*Second)
	q.sampleRTT(125*Millisecond, 25*Millisecond, s.now, false)
	if floor, ok := q.floor(s.now); !ok || floor != 100*Millisecond {
		t.Fatalf("floor = %v, %v, want the sample net of peer ACK hold", floor, ok)
	}
	for i := 46; i <= 85; i++ {
		setPathTime(s, Duration(i)*Second)
		q.sampleRTT(825*Millisecond, 25*Millisecond, s.now, false)
	}
	if floor, ok := q.floor(s.now); !ok || floor != 100*Millisecond {
		t.Fatalf("40 s of congestion raised the propagation floor to %v, %v", floor, ok)
	}
	if !s.qualityElevated(p) {
		t.Fatal("persistent RTT inflation lost its baseline")
	}
	setPathTime(s, 91*Second)
	q.sampleRTT(825*Millisecond, 25*Millisecond, s.now, false)
	if floor, ok := q.floor(s.now); !ok || floor != 800*Millisecond {
		t.Fatalf("rotating both buckets did not age out the old floor: %v, %v", floor, ok)
	}
}

func TestExpiredCarrierUnsentStillBoundsPlacementQueue(t *testing.T) {
	s, p, _ := qualityTestSession()
	const backlog = 2 << 20
	s.SetCarrierUnsent(p.id, 0, backlog, s.now)
	setPathTime(s, 12*Second)
	if got := s.queued(p); got < backlog {
		t.Fatalf("placement queue = %.0f after 11 s, want at least %d", got, backlog)
	}
	if _, fresh := s.carrierUnsent(p); fresh {
		t.Fatal("expired telemetry still counted as fresh evidence for advice")
	}
	s.SetCarrierUnsent(p.id, 0, 0, s.now)
	if got := s.queued(p); got != 0 {
		t.Fatalf("new zero backlog did not replace the previous estimate: %.0f", got)
	}
}

func TestPathQualityReasonRequiresConsecutiveObservations(t *testing.T) {
	s, p, sibling := qualityTestSession()
	p.out.snd.so = 1024
	p.out.lanes[0].log = append(p.out.lanes[0].log, retained{d: frameDesc{kind: dStream, n: 1024}})
	tick := func(at Duration, freshSibling bool) {
		t.Helper()
		setPathTime(s, at)
		observePath(s, p, Second, 12)
		if freshSibling {
			observePath(s, sibling, 160*Millisecond, 1e6)
		} else {
			sibling.mp.quality.rttAt = 0
		}
		s.pathQualityTimers(s.now, true)
	}
	tick(2*Second, true)
	tick(3*Second, true)
	if e, ok := takePathQuality(s, p.id); !ok || e.PathQuality != PathQualityHealthySibling {
		t.Fatalf("missing initial confirmed advice: %+v", e)
	}
	episode, _, ok := s.ReplacementAdvice(p.id)
	if !ok {
		t.Fatal("confirmed advice did not authorize replacement")
	}

	for i := 4; i <= 7; i++ {
		tick(Duration(i)*Second, i%2 != 0)
		if e, ok := takePathQuality(s, p.id); ok {
			t.Fatalf("one inconclusive sample emitted advice at tick %d: %+v", i, e)
		}
		if got, _, ok := s.ReplacementAdvice(p.id); !ok || got != episode {
			t.Fatalf("one inconclusive sample revoked current advice at tick %d", i)
		}
	}

	tick(8*Second, false)
	tick(9*Second, false)
	if e, ok := takePathQuality(s, p.id); !ok || e.PathQuality != PathQualityUnknown {
		t.Fatalf("two consecutive inconclusive samples did not update advice: %+v", e)
	}
	if _, _, ok := s.ReplacementAdvice(p.id); ok {
		t.Fatal("confirmed inconclusive evidence still authorizes a new replacement")
	}
	if !s.ReplacementEpisode(p.id, episode) {
		t.Fatal("reason change ended an in-progress replacement episode")
	}
}

func TestPathQualityStaleSiblingCannotJustifyRedial(t *testing.T) {
	s, p, _ := qualityTestSession()
	p.out.snd.so = 1024
	p.out.lanes[0].log = append(p.out.lanes[0].log, retained{d: frameDesc{kind: dStream, n: 1024}})
	for _, at := range []Duration{2 * Second, 3 * Second} {
		setPathTime(s, at)
		observePath(s, p, Second, 12)
		s.mpHandleTimeout(s.now)
	}
	e, ok := takePathQuality(s, p.id)
	if !ok || e.PathQuality != PathQualityUnknown || e.Wait != 0 || p.mp.out != OutActive {
		t.Fatalf("old sibling sample justified reconnection: %+v", e)
	}
}

func TestPathQualityWithoutLoadCannotAuthorizeReplacement(t *testing.T) {
	s, p, sibling := qualityTestSession()
	for _, at := range []Duration{2 * Second, 3 * Second} {
		setPathTime(s, at)
		observePath(s, p, Second, 12)
		observePath(s, sibling, 160*Millisecond, 1e6)
		s.mpHandleTimeout(s.now)
	}
	if _, _, ok := s.ReplacementAdvice(p.id); ok {
		t.Fatal("idle RTT change authorized a replacement")
	}
	if p.mp.out != OutActive || s.stats.Reinjections != 0 {
		t.Fatal("idle RTT change triggered native liveness")
	}
}

func TestPathQualityStorageFollowsConsumersAndAttachments(t *testing.T) {
	cfg := cwMPConfigs()
	m := newCWSim(t, cfg, CarrierInfo{Framing: FramingRecords}, Millisecond)
	m.established()
	s := m.end[0]
	p := s.pipe(0)
	if p == nil {
		t.Fatal("no bootstrap pipe")
	}
	if p.mp.quality != nil || s.pathQualityTimers(s.now, false) != Never {
		t.Fatal("bootstrap without a consumer started quality measurement")
	}

	id := m.join()
	joined := s.pipe(id)
	if joined == nil {
		t.Fatal("the joined pipe is missing")
	}
	if s.mp.additionalPipes == nil || p.mp.quality == nil || joined.mp.quality == nil {
		t.Fatal("an additional pipe did not enable placement measurements")
	}
	s.retire(joined, false)
	if p.mp.quality != nil || s.pathQualityTimers(s.now, false) != Never {
		t.Fatal("retiring the additional pipe kept placement-only measurements")
	}

	s.EnablePathAdvice()
	q := p.mp.quality
	if q == nil || s.pathQualityTimers(s.now, false) == Never {
		t.Fatal("registering a consumer did not enable bootstrap advice")
	}
	s.EnablePathAdvice()
	if p.mp.quality != q {
		t.Fatal("enabling advice again discarded measurements")
	}
}

func TestPathAdviceUnsubscribeReleasesOnlyAdvisoryState(t *testing.T) {
	s := capacityMP()
	s.EnablePathAdvice()
	bootstrap := s.pipes[0]
	if bootstrap.mp.quality == nil {
		t.Fatal("subscription did not allocate bootstrap measurement")
	}
	s.DisablePathAdvice()
	if bootstrap.mp.quality != nil {
		t.Fatal("unsubscribed bootstrap retained measurement")
	}
	extra := capacityExtra(s)
	s.EnablePathAdvice()
	q := bootstrap.mp.quality
	q.rate, q.since, q.advice = 1000, 1, PathQualityLocalStall
	s.DisablePathAdvice()
	if bootstrap.mp.quality != q || q.rate != 1000 || q.since != 0 || q.advice != 0 || extra.mp.quality == nil {
		t.Fatal("unsubscription changed placement measurements or kept the advisory episode")
	}
}
