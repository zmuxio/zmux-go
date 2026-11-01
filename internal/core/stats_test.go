package core

import (
	"errors"
	"math"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// statsMP is a multipath session with pipe 0 and one more attachment.
func statsMP() (*Session, *Pipe) {
	s := capacityMP()
	s.mp.bits, s.mp.pendAbandon = map[uint64]bool{}, map[uint64]bool{}
	p := capacityExtra(s)
	return s, p
}

func TestStatsPenaltiesOncePerEpisode(t *testing.T) {
	s, p := statsMP()
	s.penalize(p)
	s.penalize(p) // the same episode
	if got := s.SessionStats().Penalties; got != 1 {
		t.Fatalf("Penalties = %d after one episode, want 1", got)
	}
	p.mp.penalized = false // the episode ended
	s.penalize(p)
	if got := s.SessionStats().Penalties; got != 2 {
		t.Fatalf("Penalties = %d after two episodes, want 2", got)
	}
}

func TestStatsReinjectionEpisodes(t *testing.T) {
	s, p := statsMP()
	ol := p.out.lanes[0]
	ol.log = append(ol.log, retained{d: frameDesc{kind: dMaxData}})
	s.reinjectPipe(p)
	s.reinjectPipe(p) // nothing new to reinject: no episode
	if got := s.SessionStats().Reinjections; got != 1 {
		t.Fatalf("Reinjections = %d, want 1", got)
	}
	ol.log = append(ol.log, retained{d: frameDesc{kind: dMaxData}})
	s.reinjectPipe(p)
	if got := s.SessionStats().Reinjections; got != 2 {
		t.Fatalf("Reinjections = %d, want 2", got)
	}
}

func TestStatsSuspendedTime(t *testing.T) {
	s, _ := statsMP()
	s.suspend(nil)
	s.SetNow(s.now.Add(3 * Second))
	st := s.SessionStats()
	if st.Suspensions != 1 || st.SuspendedTime != 3*Second {
		t.Fatalf("during the suspension: %d suspensions, %v, want 1, 3s", st.Suspensions, st.SuspendedTime)
	}
	s.Close(0, nil) // ends the suspension
	s.SetNow(s.now.Add(5 * Second))
	if st := s.SessionStats(); st.Suspensions != 1 || st.SuspendedTime != 3*Second {
		t.Fatalf("after the session closed: %d suspensions, %v, want 1, 3s", st.Suspensions, st.SuspendedTime)
	}
}

func TestStatsJoinAuthAndReplays(t *testing.T) {
	s, p := statsMP()
	s.role, s.roleKnown = RoleInitiator, true
	key := make([]byte, 32)
	s.mp.keyPeer = key
	mac := []byte("mac input")
	// A token that does not verify.
	j := &wire.Join{PipeID: 3, Token: make([]byte, 16)}
	if r := s.AcceptJoin(j, mac, nil, Carrier{HasIn: true, HasOut: true}); r != JoinClose {
		t.Fatalf("bad token: %v", r)
	}
	// A verified join for an ID the session already has: a replay.
	p.id = 3
	j = &wire.Join{PipeID: 3, Token: wire.JoinToken(key, mac, nil)}
	if r := s.AcceptJoin(j, mac, nil, Carrier{HasIn: true, HasOut: true}); r != JoinClose {
		t.Fatalf("replay: %v", r)
	}
	st := s.SessionStats()
	if st.JoinAuthFailures != 1 || st.JoinReplays != 1 {
		t.Fatalf("JoinAuthFailures %d, JoinReplays %d, want 1, 1", st.JoinAuthFailures, st.JoinReplays)
	}
}

func TestStatsJoinRefusalsAndFailures(t *testing.T) {
	s, p := statsMP()
	p.mp.joinerLocal = true
	s.peerAbandon(p, wire.PipeResourceLimit)
	if got := s.SessionStats().JoinRefusals; got != 1 {
		t.Fatalf("JoinRefusals = %d, want 1", got)
	}
	_, q := statsMP()
	s.pipes = append(s.pipes, q)
	q.id = 5
	cause := errors.New("carrier lost")
	before := s.SessionStats().PipeFailures
	s.failPipe(q, wire.PipeFailed, cause)
	s.failPipe(q, wire.PipeFailed, cause) // failed already
	if got := s.SessionStats().PipeFailures - before; got != 1 {
		t.Fatalf("PipeFailures rose by %d, want 1", got)
	}
	if pi, ok := s.PipeInfo(5); !ok || !errors.Is(pi.Failure, cause) || pi.FailureCount != 1 {
		t.Fatalf("PipeInfo failure %v (%d)", pi.Failure, pi.FailureCount)
	}
}

func TestStatsSchedulerAnswers(t *testing.T) {
	s, _ := statsMP()
	st := s.newStream(true, true)
	st.snd.mpState()
	req := &PickRequest{Gen: s.mp.pickGen, Pipes: []PickPipe{{Pipe: 0}}, keys: []*Stream{st, st, st}}
	s.AnswerPick(req, []PickAnswer{{Index: 0, Bytes: 10}, {Wait: true}, {Index: 7}}, false)
	got := s.SessionStats()
	// The Wait answer is neither applied as a pipe nor a fallback.
	if got.SchedulerAnswers != 1 || got.SchedulerFallbacks != 1 {
		t.Fatalf("answers %d, fallbacks %d, want 1, 1", got.SchedulerAnswers, got.SchedulerFallbacks)
	}
	s.AnswerPick(req, nil, true) // the policy failed: every answer falls back
	if got := s.SessionStats().SchedulerFallbacks; got != 4 {
		t.Fatalf("fallbacks after a failed policy = %d, want 4", got)
	}
}

func TestStatsPingsAnswered(t *testing.T) {
	s, p := statsMP()
	a := p.mp
	a.pingEnds = []uint64{100, 200}
	p.out.snd.cum = 150
	s.pingsAcked(p)
	if a.pingsAnswered != 1 || len(a.pingEnds) != 1 {
		t.Fatalf("answered %d, outstanding %v", a.pingsAnswered, a.pingEnds)
	}
	p.out.snd.cum = 200
	s.pingsAcked(p)
	if pi, _ := s.PipeInfo(p.id); pi.PingsAnswered != 2 {
		t.Fatalf("PingsAnswered = %d, want 2", pi.PingsAnswered)
	}
}

func TestStatsDuplicateReceived(t *testing.T) {
	s, p := statsMP()
	s.feat.Multipath = true
	s.rxPipe = p
	st := s.newStream(true, false)
	st.id, st.idSet = 1, true
	st.rcv.adv, st.rcv.window = 1<<20, 1<<20
	s.conn.adv = 1 << 20
	s.streams[1] = st
	r := &st.rcv
	r.arrived.add(0, 100) // received already
	r.highest = 100
	if dup := r.arrived.covered(50, 150); dup != 50 {
		t.Fatalf("covered = %d", dup)
	}
	s.countDuplicate(r, 50, 150)
	if got := s.SessionStats().BytesDuplicateReceived; got != 50 {
		t.Fatalf("BytesDuplicateReceived = %d, want 50", got)
	}
	if pi, _ := s.PipeInfo(p.id); pi.BytesDuplicateRecv != 50 {
		t.Fatalf("pipe BytesDuplicateRecv = %d, want 50", pi.BytesDuplicateRecv)
	}
}

func TestStatsLaneInfos(t *testing.T) {
	s, p := statsMP()
	if pi, _ := s.PipeInfo(p.id); pi.Lanes != nil || pi.LanesOut != 1 {
		t.Fatalf("a pipe without lanes: %+v, %d", pi.Lanes, pi.LanesOut)
	}
	p.out.lanes = append(p.out.lanes, &outLane{lane: 2, pos: 70, acked: 30, refused: true})
	p.out.lanes[0].pos, p.out.lanes[0].acked = 10, 5
	p.in = &inDir{lanes: []*inLane{{lane: 0, pos: 40}, {lane: 1, pos: 9}}}
	pi, _ := s.PipeInfo(p.id)
	want := []LaneInfo{{Lane: 0, PositionSent: 10, PositionAcked: 5, PositionReceived: 40, HasOut: true, HasIn: true},
		{Lane: 1, PositionReceived: 9, HasIn: true}, {Lane: 2, PositionSent: 70, PositionAcked: 30, HasOut: true}}
	if len(pi.Lanes) != len(want) {
		t.Fatalf("lanes %+v", pi.Lanes)
	}
	for i := range want {
		if pi.Lanes[i] != want[i] {
			t.Fatalf("lane %d: %+v, want %+v", i, pi.Lanes[i], want[i])
		}
	}
	if pi.LanesOut != 2 || pi.LanesIn != 2 || pi.LanesRefused != 1 || pi.BytesSent != 80 {
		t.Fatalf("lanes out %d in %d refused %d, sent %d", pi.LanesOut, pi.LanesIn, pi.LanesRefused, pi.BytesSent)
	}
}

func TestStatsGauges(t *testing.T) {
	s, p := statsMP()
	s.mp.retained = 123
	p.mp.out = OutSuspect
	g := s.Gauges()
	if g.PipesActive != 1 || g.Retained != 123 || g.StreamsActive != 0 {
		t.Fatalf("gauges %+v", g)
	}
	st := s.newStream(true, true)
	st.id, st.idSet = 0, true
	st.snd.preLen = 3
	st.snd.q.write([]byte("pre-payload"))
	st.snd.sent = 5
	st.rcv.buf.bytes = 7
	s.streams[0] = st
	g = s.Gauges()
	if g.StreamsActive != 1 || g.SendBuffered != 6 || g.RecvBuffered != 7 {
		t.Fatalf("stream gauges %+v", g)
	}
	if g.HasRTT {
		t.Fatal("an RTT before the first sample")
	}
}

func TestStatsStreamInfo(t *testing.T) {
	s := capacitySession()
	s.cfg.DgramStreamQ = 1
	st := s.newStream(true, false)
	st.id, st.idSet = 1, true
	// Two datagrams for a queue of one: the oldest is dropped.
	s.queueDatagram(st, streamDgram(0, []byte("ab")))
	s.queueDatagram(st, streamDgram(0, []byte("cde")))
	if _, p, ok := s.RecvStreamDatagram(st); !ok || string(p) != "cde" {
		t.Fatalf("datagram %q %v", p, ok)
	}
	in := s.StreamInfo(st)
	if in.DatagramsReceived != 2 || in.DatagramsDroppedRecv != 1 || in.DatagramBytesReceived != 3 {
		t.Fatalf("datagrams %+v", in)
	}
	// Received bytes are distinct and exclude the preamble; once the
	// application stopped reading, everything below the highest offset
	// counts.
	r := &st.rcv
	r.preDone, r.preSize = true, 4
	r.arrived.add(0, 10)
	r.arrived.add(20, 30)
	r.highest = 30
	if in := s.StreamInfo(st); in.BytesReceived != 16 {
		t.Fatalf("BytesReceived = %d, want 16", in.BytesReceived)
	}
	r.stopped = true
	if in := s.StreamInfo(st); in.BytesReceived != 26 {
		t.Fatalf("BytesReceived after stopping = %d, want 26", in.BytesReceived)
	}
	// A frame's bytes over the preamble count only above it.
	lst := s.newStream(true, true)
	lst.snd.preLen = 5
	lst.countFrame(0, 8)
	lst.countFrame(8, 4)
	if in := s.StreamInfo(lst); in.BytesSent != 7 {
		t.Fatalf("BytesSent = %d, want 7", in.BytesSent)
	}
}

func TestStatsBytesLost(t *testing.T) {
	s, p := statsMP()
	ol := p.out.lanes[0]
	ol.pos, p.out.records, p.out.snd.cum = 1000, 10, 400
	if pi, _ := s.PipeInfo(p.id); pi.BytesLost != 0 || pi.RecordsLost != 0 {
		t.Fatalf("before the failure: %d bytes, %d records lost", pi.BytesLost, pi.RecordsLost)
	}
	s.failPipe(p, 0, nil)
	pi, _ := s.PipeInfo(p.id)
	if pi.BytesLost != 600 || pi.RecordsLost != 6 {
		t.Fatalf("%d bytes, %d records lost, want 600, 6", pi.BytesLost, pi.RecordsLost)
	}
}

func TestStatsLossRate(t *testing.T) {
	s, p := statsMP()
	ol := p.out.lanes[0]
	s.SetCarrierRetransmitted(p.id, 0) // baseline
	ol.pos += 1000
	s.SetNow(s.now.Add(Second / 2))
	s.SetCarrierRetransmitted(p.id, 100) // too early: no sample
	if pi, _ := s.PipeInfo(p.id); pi.LossRate != 0 || pi.BytesRetransmitted != 100 {
		t.Fatalf("before a sample: rate %v, %d retransmitted", pi.LossRate, pi.BytesRetransmitted)
	}
	s.SetNow(s.now.Add(Second / 2))
	s.SetCarrierRetransmitted(p.id, 100)
	if pi, _ := s.PipeInfo(p.id); pi.LossRate != 0.1 {
		t.Fatalf("first sample: rate %v, want 0.1", pi.LossRate)
	}
	ol.pos += 1000
	s.SetNow(s.now.Add(Second))
	s.SetCarrierRetransmitted(p.id, 100) // no loss in this interval
	if pi, _ := s.PipeInfo(p.id); math.Abs(pi.LossRate-(0.1-0.1/8)) > 1e-12 {
		t.Fatalf("second sample: rate %v, want %v", pi.LossRate, 0.1-0.1/8)
	}
}
