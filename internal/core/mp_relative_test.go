package core

import (
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

type replayEstimate struct {
	SRTT      Duration `json:"srtt_us"`
	RTTVar    Duration `json:"rttvar_us"`
	ETA       Duration `json:"eta_us"`
	Deviation Duration `json:"deviation_us"`
}

type replayPath struct {
	ID     string
	PipeID uint64 `json:"pipe_id"`
	replayEstimate
}

type relativeFixture struct {
	ID    string
	Setup struct {
		Profile    string
		PeerDelay  Duration `json:"peer_max_ack_delay_us"`
		Resume     *bool
		Idle       Duration `json:"idle_timeout_us"`
		Controller string
		Pipes      []replayPath
	}
	Steps []struct {
		Op, Target, Action string
		At                 Duration `json:"at_us"`
		Args               struct {
			replayEstimate
			Record, Stream, Source string
			Ready                  bool
			Prefix                 uint64   `json:"prefix_bytes"`
			QueueDelay             Duration `json:"queue_delay_us"`
		}
		Values map[string]json.RawMessage
	}
}

type relativeEvent struct {
	Name   string   `json:"name"`
	Pipe   string   `json:"pipe"`
	Stream string   `json:"stream"`
	Mode   string   `json:"mode"`
	Reason string   `json:"reason"`
	At     Duration `json:"at_us"`
	Length uint64   `json:"length"`
}

type relativeReplay struct {
	t          *testing.T
	s          *Session
	base       Time
	pipe       map[string]*Pipe
	estimate   map[*Pipe]replayEstimate
	records    map[string]uint64
	events     []relativeEvent
	faults     map[MemberID]int
	probes     map[*Pipe]int
	probeEnd   map[*Pipe]uint64
	stream     *Stream
	streamName string
	source     *Pipe
}

func newRelativeReplay(t *testing.T, c relativeFixture) *relativeReplay {
	t.Helper()
	s, _, q := quietWriterPipes()
	s.feat.Multipath, s.feat.Preamble = true, true
	s.feat.Resume = c.Setup.Resume == nil || *c.Setup.Resume
	s.cfg.Test.Seed = 13
	s.cfg.ProbeInterval, s.cfg.StandbyProbe, s.cfg.KeepAlive = 0, 0, 0
	s.cfg.Probation, s.cfg.StabilityHold = 5*Second, 5*Second
	s.cfg.SuspectFloor = 250 * Millisecond
	s.cfg.IdleTimeout = c.Setup.Idle * Microsecond
	s.ack.peerDelay = c.Setup.PeerDelay * Microsecond
	s.conn.sendMax, s.loc[0].limit, s.peer.MaxStreamDataBidiRem = 1<<20, 32, 1<<20
	s.cfg.Redundancy = false
	s.cfg.Policy = PolicyAffinity
	s.EnableMemberFeedback()
	if len(c.Setup.Pipes) == 1 {
		s.pipes = s.pipes[:1]
	}
	for i := 1; i < len(c.Setup.Pipes); i++ {
		path := q
		if i > 1 {
			path = capacityExtra(s)
			path.out.sig = &WriteSignal{}
			path.out.lanes[0].confirmed, path.out.lanes[0].tpSent = true, true
		}
		path.id = c.Setup.Pipes[i].PipeID
		path.in = &inDir{lanes: []*inLane{{}}}
		s.addMemberPipe(nil, path, DirectionBoth)
	}
	r := &relativeReplay{t: t, s: s, base: Time(10 * Second), pipe: map[string]*Pipe{},
		estimate: map[*Pipe]replayEstimate{}, records: map[string]uint64{},
		faults: map[MemberID]int{}, probes: map[*Pipe]int{}, probeEnd: map[*Pipe]uint64{}}
	s.SetNow(Time(Second))
	s.noteActivity(s.now)
	for i, setup := range c.Setup.Pipes {
		path := s.pipes[i]
		if path.id != setup.PipeID {
			t.Fatalf("unexpected fixture pipe id %d", setup.PipeID)
		}
		r.pipe[setup.ID] = path
		r.estimate[path] = setup.replayEstimate
		path.mp.peer, path.mp.liveAt = peerObserved, s.now
		s.syncPipeHealth(path)
		s.ObserveStability(path.id)
		if !r.poll(path) {
			t.Fatalf("setup did not hand off probe on pipe %d", path.id)
		}
	}
	s.SetNow(Time(Second + 50*Millisecond))
	for _, path := range s.pipes {
		r.ack(path, path.out.lanes[0].pos, false)
	}
	s.refreshPathTopology()
	s.HandleTimeout(r.base)
	for _, path := range s.pipes {
		if !path.mp.health.snapshot.Send.Settled {
			t.Fatalf("setup did not settle pipe %d", path.id)
		}
		r.inputs(path, r.estimate[path])
	}
	r.drain()
	r.events = nil
	return r
}

// Feed the scripted PTO and one-way placement observations into their actual
// estimator outputs. A rate of 16 GB/s makes the 16-byte serialization 1 ns.
func (r *relativeReplay) inputs(p *Pipe, e replayEstimate) {
	s := r.s
	r.estimate[p] = e
	p.mp.rtt = rttEst{has: true, smoothed: e.SRTT * Microsecond, rttvar: e.RTTVar * Microsecond, min: e.SRTT * Microsecond}
	p.mp.quality.rtt = 2 * (e.ETA*Microsecond - 1)
	p.mp.quality.rttAt = s.now
	p.mp.quality.minRTT = 0
	p.mp.quality.rate, p.mp.quality.rateAt = 16e9, s.now
	s.pathRTTChanged(p, true)
	s.armPathDeadline(p, pathStall)
}

func (r *relativeReplay) ack(p *Pipe, pos uint64, scripted bool) {
	r.t.Helper()
	if scripted {
		// Recorded RTT inputs remain fixed across the replay's proof ACKs.
		p.mp.noSample, p.mp.noSampleAt = true, r.s.now
	}
	if err := r.s.mpAckEntry(wire.AckEntry{PipeID: p.id, Position: pos}, r.s.now, p); err != nil {
		r.t.Fatal(err)
	}
}

// handoffControl uses the regular record writer and send-order accounting.
// Scripted controls are separate from the session's rate-limited probes.
func (r *relativeReplay) handoffControl(p *Pipe, lane uint64) uint64 {
	r.t.Helper()
	s := r.s
	b := GetBatch()
	defer PutBatch(b)
	ol := p.out.lane(lane)
	b.Pipe, b.Lane, b.owner = p.id, lane, ol
	var w recWriter
	w.init(b, ol.info.Framing, s.recMax(ol), DefaultBatch)
	frame := wire.AppendPing(nil, false, 1)
	if !w.frame(len(frame), true, func(b []byte) []byte { return append(b, frame...) }) {
		r.t.Fatal("could not encode scripted control")
	}
	w.end()
	s.afterBatch(&w, p, ol)
	s.armPathDeadline(p, pathStall)
	s.armPathDeadline(p, pathProbe)
	s.WriteDone(b, b.Len(), nil, s.now)
	return ol.pos
}

func (r *relativeReplay) poll(p *Pipe) bool {
	r.t.Helper()
	s := r.s
	if p.out == nil || !s.open(p) {
		return false
	}
	b := GetBatch()
	defer PutBatch(b)
	ol := p.out.lanes[0]
	before := ol.pos
	probe := p.mp.suspPing
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, s.now) {
		return false
	}
	if probe && !p.mp.suspPing {
		r.probes[p]++
		r.probeEnd[p] = ol.pos
	}
	if r.stream != nil && r.source != nil {
		for _, e := range ol.log {
			if e.end <= before || e.d.kind != dStream || e.d.id != r.stream.id {
				continue
			}
			mode := "suspect_reinjection"
			switch {
			case e.d.fresh:
				continue
			case e.d.repair:
				mode = "opportunistic"
			case r.source.mp.out == OutFailed:
				mode = "failure_reinjection"
			}
			r.events = append(r.events, relativeEvent{Name: "PrefixDispatched", Pipe: r.name(p),
				Stream: r.streamName, Mode: mode, At: s.now.Sub(r.base) / Microsecond, Length: e.d.n})
		}
	}
	s.WriteDone(b, b.Len(), nil, s.now)
	return true
}

func (r *relativeReplay) name(p *Pipe) string {
	for name, q := range r.pipe {
		if p == q {
			return name
		}
	}
	return ""
}

func (r *relativeReplay) drain() {
	for ev, ok := r.s.PollEvent(); ok; ev, ok = r.s.PollEvent() {
		if ev.Kind != EvPipeHealth || ev.Dir != DirOut {
			continue
		}
		e := relativeEvent{Pipe: r.name(r.s.pipe(ev.Pipe)), At: ev.At.Sub(r.base) / Microsecond}
		switch {
		case ev.HealthAfter.State == HealthUnavailable && faultReason(ev.HealthAfter.Reason):
			e.Name = "PipeSuspect"
		case ev.HealthAfter.State == HealthEnded && faultReason(ev.HealthAfter.Reason):
			e.Name = "PipeFailed"
		case ev.HealthBefore.State == HealthUnavailable && faultReason(ev.HealthBefore.Reason) && ev.HealthAfter.Stable:
			e.Name = "PipeRecovered"
		default:
			continue
		}
		if e.Name != "PipeRecovered" {
			switch {
			case errors.Is(ev.Cause, io.EOF):
				e.Reason = "unexpected_eof"
			case ev.HealthAfter.Reason == HealthAckStall:
				e.Reason = "ack_stalled"
			case ev.HealthAfter.Reason == HealthWriteStall:
				e.Reason = "write_stalled"
			}
		}
		r.events = append(r.events, e)
	}
	for _, m := range r.s.TakeMemberChanges() {
		if m.Failed {
			r.faults[m.ID]++
		}
	}
}

func (r *relativeReplay) quiesce() {
	r.t.Helper()
	s := r.s
	for range 32 {
		s.HandleTimeout(s.now)
		r.drain()
		work := false
		for _, p := range s.pipes {
			// The replay profile suppresses periodic probes, including
			// probation probes, but sends probes requested by suspicion.
			if p.mp.prob.on && p.mp.needPing && !p.mp.suspPing {
				p.mp.needPing = false
				s.armPathDeadline(p, pathProbe)
			}
			work = r.poll(p) || work
		}
		if !work && s.NextTimeout() > s.now {
			return
		}
	}
	r.t.Fatal("owner did not quiesce at", s.now.Sub(r.base))
}

func (r *relativeReplay) advance(at Time) {
	r.t.Helper()
	for i := 0; r.s.NextTimeout() < at; i++ {
		if i == 64 {
			r.t.Fatal("owner kept scheduling earlier deadlines")
		}
		r.s.SetNow(r.s.NextTimeout())
		r.quiesce()
	}
	r.s.SetNow(at)
}

type replayPrefix struct {
	name       string
	bytes      uint64
	queueDelay Duration
}

func (r *relativeReplay) open(source *Pipe, prefix replayPrefix) {
	n, delay := prefix.bytes, prefix.queueDelay
	r.t.Helper()
	s := r.s
	st, err := s.OpenStream(true)
	if err != nil {
		r.t.Fatal(err)
	}
	r.stream, r.streamName = st, prefix.name
	s.SetRedundant(st, new(bool))
	if _, err := s.PinID(st); err != nil {
		r.t.Fatal(err)
	}
	if st.snd.preLen == 0 || st.snd.preLen >= n {
		r.t.Fatal("invalid opening preamble size", st.snd.preLen)
	}
	if _, err := s.Write(st, make([]byte, n-st.snd.preLen), false); err != nil {
		r.t.Fatal(err)
	}
	// The fixture directs only this initial scheduler decision. Later
	// writes and all re-injection use ordinary topology and placement.
	for _, p := range s.pipes {
		p.mp.topologyOut = p != source
	}
	s.mp.invalidateCandidates()
	s.SetCarrierUnsent(source.id, 0, uint64(float64(delay*Microsecond)*s.rateOf(source)/1e9), s.now)
	wrote := r.poll(source)
	for _, p := range s.pipes {
		p.mp.topologyOut = false
	}
	s.mp.invalidateCandidates()
	if !wrote || st.snd.sent != n {
		r.t.Fatalf("opening prefix did not hand off %d bytes on %d (sent %d)", n, source.id, st.snd.sent)
	}
	r.source = source
	r.records[prefix.name] = source.out.lanes[0].pos
	s.SetCarrierUnsent(source.id, 0, 0, s.now)
	if actual := s.queueDelay(source.mp); actual != delay*Microsecond {
		r.t.Fatalf("handoff queue snapshot %d ns, want %d ns", actual, delay*Microsecond)
	}
}

func stallReplay(t *testing.T, extra ...replayPath) *relativeReplay {
	t.Helper()
	var c relativeFixture
	c.Setup.PeerDelay, c.Setup.Idle = 25_000, 120_000_000
	slow := replayEstimate{SRTT: 6_000_000, RTTVar: 118_750, ETA: 3_100_000}
	fast := replayEstimate{SRTT: 80_000, RTTVar: 3750, ETA: 55_000}
	c.Setup.Pipes = append([]replayPath{
		{ID: "p", PipeID: 0, replayEstimate: slow},
		{ID: "q", PipeID: 1, replayEstimate: fast},
	}, extra...)
	return newRelativeReplay(t, c)
}

func TestRelativeStallProofTracksPipeProgressAndGeneration(t *testing.T) {
	r := stallReplay(t)
	s, q := r.s, r.pipe["q"]
	q.out.lanes = append(q.out.lanes, &outLane{lane: 1, confirmed: true})
	first := r.handoffControl(q, 0)
	later := r.handoffControl(q, 1)
	r.advance(r.base.Add(80 * Millisecond))
	previous := q.mp.deliveryAt
	if e := s.mpAckEntry(wire.AckEntry{PipeID: q.id, Lane: 1, Position: later}, s.now, q); e != nil {
		t.Fatal(e)
	}
	if q.mp.deliveryAt != previous {
		t.Fatal("an ACK on a later lane advanced proof across a hole")
	}
	r.ack(q, first, true)
	if q.mp.deliveryAt != s.now || !s.hasDelivery(q, r.base) || s.hasDelivery(q, s.now) {
		t.Fatal("cumulative progress did not establish strictly newer proof")
	}
	r.ack(q, first, true)
	if q.mp.deliveryAt != s.now {
		t.Fatal("duplicate ACK changed the proof")
	}

	// An inflated PTO must leave the original evidence window unchanged.
	e := r.estimate[q]
	e.RTTVar += 50_000
	r.inputs(q, e)
	if until := s.deliveryUntil(q); until != r.base.Add(320*Millisecond) {
		t.Fatalf("PTO growth moved the proof expiry to %d", until.Sub(r.base))
	}
	r.advance(r.base.Add(320 * Millisecond))
	if s.hasDelivery(q, r.base) {
		t.Fatal("the half-open evidence window includes its end")
	}

	old := r.handoffControl(q, 0)
	s.SetTopologyExclusion(q.id, true, false)
	s.SetTopologyExclusion(q.id, false, false)
	s.healthProof(q, DirOut)
	r.ack(q, old, true)
	if q.mp.deliveryGen == q.mp.health.send.generation || s.hasDelivery(q, r.base) {
		t.Fatal("ACK of a pre-generation handoff proved the new generation")
	}
}

func TestRelativeStallSameCutWithdrawal(t *testing.T) {
	for _, by := range []string{"carrier error", "write stall"} {
		t.Run(by, func(t *testing.T) {
			r := stallReplay(t)
			s, p, q := r.s, r.pipe["p"], r.pipe["q"]
			s.cfg.Test.NoPrefixRepair = true
			r.open(p, replayPrefix{name: "s", bytes: 16})
			pos := r.handoffControl(q, 0)
			r.advance(r.base.Add(110 * Millisecond))
			if by == "write stall" {
				ol := q.out.lanes[0]
				ol.writing, ol.stallAt = true, s.now
				s.armPathDeadline(q, pathStall)
			}
			r.advance(r.base.Add(200 * Millisecond))
			r.ack(q, pos, true)
			r.quiesce()
			cut := r.base.Add(360 * Millisecond)
			if s.ackStallDeadline(p) != cut {
				t.Fatal("fixture did not arm a relative deadline")
			}
			r.advance(cut)
			if by == "carrier error" {
				s.PipeEnded(q.id, 0, true, false, io.EOF, nil, s.now)
			}
			r.quiesce()
			if p.mp.out != OutActive || q.mp.out == OutActive || s.ackStallDeadline(p) != r.base.Add(13*Second) {
				t.Fatalf("same-cut reference withdrawal: source=%v reference=%v deadline=%d", p.mp.out, q.mp.out, s.ackStallDeadline(p).Sub(r.base))
			}
		})
	}
}

func TestRelativeStallAndPrefixChooseDifferentPaths(t *testing.T) {
	estimate := replayEstimate{SRTT: 100_000, RTTVar: 3750, ETA: 90_000}
	r := stallReplay(t, replayPath{ID: "r", PipeID: 3, replayEstimate: estimate})
	s, p, q, target := r.s, r.pipe["p"], r.pipe["q"], r.pipe["r"]
	r.open(p, replayPrefix{name: "s", bytes: 16})
	for _, path := range []*Pipe{q, target} {
		r.records[r.name(path)] = r.handoffControl(path, 0)
	}
	r.advance(r.base.Add(250 * Millisecond))
	for _, path := range []*Pipe{q, target} {
		r.ack(path, r.records[r.name(path)], true)
	}
	// q has the lower one-way ETA. Its outstanding control was handed off
	// after a 100 ms queue, so r has the lower current PTO plus queue cost.
	s.SetCarrierUnsent(q.id, 0, uint64(s.rateOf(q)/10), s.now)
	r.handoffControl(q, 0)
	s.SetCarrierUnsent(q.id, 0, 0, s.now)
	r.quiesce()
	if len(r.events) != 1 || r.events[0].Pipe != "q" || r.events[0].Mode != "opportunistic" {
		t.Fatalf("repair did not pick the lowest ETA: %+v", r.events)
	}
	cut := r.base.Add(420 * Millisecond)
	if s.ackStallDeadline(p) != cut {
		t.Fatalf("stall did not pick the lowest PTO plus queue: %d", s.ackStallDeadline(p).Sub(r.base))
	}
	r.advance(cut)
	r.quiesce()
	if p.mp.out != OutSuspect || !s.hasDelivery(q, r.base) || !s.hasDelivery(target, r.base) {
		t.Fatal("the two live alternatives were not available at suspicion")
	}
}

func TestRelativeStallNeedsACKReturnButWriteUsesOwnClock(t *testing.T) {
	r := stallReplay(t)
	s, p, q := r.s, r.pipe["p"], r.pipe["q"]
	r.open(p, replayPrefix{name: "s", bytes: 16})
	pos := r.handoffControl(q, 0)
	r.advance(r.base.Add(80 * Millisecond))
	r.ack(q, pos, true)
	p.in, q.in = nil, nil
	s.refreshPathTopology()
	ol := p.out.lanes[0]
	ol.writing, ol.stallAt = true, s.now
	s.armPathDeadline(p, pathStall)
	r.advance(r.base.Add(360 * Millisecond))
	r.quiesce()
	if p.mp.out != OutActive || s.hasDelivery(q, r.base) || s.ackStallDeadline(p) != Never {
		t.Fatal("no live ACK return, but an ACK arm or proof remained")
	}
	r.advance(r.base.Add(80*Millisecond + 13*Second))
	r.quiesce()
	if p.mp.out != OutSuspect || p.mp.health.snapshot.Send.Reason != HealthWriteStall {
		t.Fatal("local write stall did not use its own absolute clock")
	}
}

func TestRelativeStallCarrierOwnsFailure(t *testing.T) {
	r := stallReplay(t)
	s, p, q := r.s, r.pipe["p"], r.pipe["q"]
	p.mp.ownFail = true
	s.cfg.Test.NoPrefixRepair = true
	r.open(p, replayPrefix{name: "s", bytes: 16})
	pos := r.handoffControl(q, 0)
	r.advance(r.base.Add(200 * Millisecond))
	r.ack(q, pos, true)
	r.advance(r.base.Add(360 * Millisecond))
	r.quiesce()
	if p.mp.out != OutSuspect {
		t.Fatal("carrier-owned death disabled suspicion")
	}
	r.ack(q, q.out.lanes[0].pos, true)
	r.advance(r.base.Add(53 * Second))
	r.quiesce()
	if p.mp.out != OutSuspect {
		t.Fatal("a stall overrode carrier-owned failure")
	}
	s.PipeEnded(p.id, 0, true, false, io.EOF, nil, s.now)
	r.quiesce()
	if p.mp.out != OutFailed || r.faults[p.member.ID] != 1 {
		t.Fatal("carrier error failed to finish the original episode")
	}
}

func TestPrefixRepairFlowCreditUsesSameCap(t *testing.T) {
	r := stallReplay(t)
	s, p, q := r.s, r.pipe["p"], r.pipe["q"]
	r.open(p, replayPrefix{name: "s", bytes: 16})
	r.stream.snd.maxData = 16
	if _, err := s.Write(r.stream, []byte{1}, false); err != nil {
		t.Fatal(err)
	}
	pos := r.handoffControl(q, 0)
	r.advance(r.base.Add(80 * Millisecond))
	r.ack(q, pos, true)
	r.advance(r.base.Add(120 * Millisecond))
	r.quiesce()
	if len(r.events) != 1 || r.events[0].Mode != "opportunistic" || r.events[0].Length != 16 ||
		s.stats.Penalties != 1 {
		t.Fatalf("flow-credit repair events %+v, penalties %d", r.events, s.stats.Penalties)
	}
	s.syncPipeHealth(p)
	for _, edge := range s.TakeMemberChanges() {
		if edge.Failed {
			t.Fatal("a scheduling penalty charged a member fault")
		}
	}
	h := p.mp.health.snapshot.Send
	if h.State != HealthUsable || !h.Settled || h.CanCarry || r.faults[p.member.ID] != 0 {
		t.Fatalf("penalty changed service or health: %+v", h)
	}
}

func TestRelativeStallDoesNotRebaseFailureClock(t *testing.T) {
	r := stallReplay(t)
	s, p, q := r.s, r.pipe["p"], r.pipe["q"]
	s.cfg.Test.NoPrefixRepair = true
	r.open(p, replayPrefix{name: "s", bytes: 16})
	pos := r.handoffControl(q, 0)
	r.advance(r.base.Add(200 * Millisecond))
	r.ack(q, pos, true)
	r.advance(r.base.Add(360 * Millisecond))
	r.quiesce()
	if p.mp.out != OutSuspect {
		t.Fatal("did not enter suspicion at the relative deadline")
	}
	r.ack(q, q.out.lanes[0].pos, true)
	failure := r.base.Add(52 * Second)
	r.advance(failure - 1)
	r.quiesce()
	if p.mp.out != OutSuspect {
		t.Fatal("the reference shortened the final failure timeout")
	}
	r.advance(failure)
	r.quiesce()
	if p.mp.out != OutFailed || r.faults[p.member.ID] != 1 {
		t.Fatal("failure did not use the original source stall anchor")
	}
}

func TestPrefixRepairFreezesSourceDueAndHasMinimumMargin(t *testing.T) {
	r := stallReplay(t)
	s, p, q := r.s, r.pipe["p"], r.pipe["q"]
	fast := replayEstimate{SRTT: 10_000, RTTVar: 500, ETA: 10_000}
	r.inputs(q, fast)
	r.open(p, replayPrefix{name: "s", bytes: 16})
	_, original := s.prefixHead(r.stream, 0, nil)
	wantSource := r.base.Add(3100*Millisecond + 3*Second + 118750*Microsecond + 25*Millisecond)
	if original.confirm != wantSource {
		t.Fatalf("source deadline %d, want %d", original.confirm.Sub(r.base), wantSource.Sub(r.base))
	}
	pos := r.handoffControl(q, 0)
	r.advance(r.base.Add(20 * Millisecond))
	slow := r.estimate[p]
	slow.SRTT *= 2
	r.inputs(p, slow)
	if original.confirm != wantSource {
		t.Fatal("source RTT after handoff moved the frozen deadline")
	}
	r.ack(q, pos, true)
	r.advance(r.base.Add(60*Millisecond - 1))
	r.quiesce()
	if len(r.events) != 0 {
		t.Fatal("copy used a margin below 50 ms")
	}
	r.advance(r.base.Add(60 * Millisecond))
	r.quiesce()
	if len(r.events) != 1 || r.events[0].Mode != "opportunistic" {
		t.Fatalf("copy did not use the 50 ms minimum margin: %+v", r.events)
	}
}
