package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// statsMP is a multipath session with pipe 0 and one more attachment.
func statsMP() (*Session, *Pipe) {
	s := capacityMP()
	s.prepareAdditional()
	s.mp.usableGroups = []uint64{join.GroupX25519, join.GroupX25519MLKEM768}
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
	s.peerSeen = true
	su, _ := join.LookupSuite(join.SuiteAES)
	key := make([]byte, 32)
	setTestKeys(s, su.ID, key)
	hello := func(psk []byte) join.Hello {
		j, err := join.NewJoiner(join.JoinerConfig{Suite: su, Group: testJoinGroup, Mode: join.Trusted, Framing: join.Stream, Locator: []byte("L"), PSK: psk})
		if err != nil {
			t.Fatal(err)
		}
		b, err := j.WriteHello(nil, join.HelloPayload{Pipe: join.Pipe{ID: 3, Direction: 2}})
		if err != nil {
			t.Fatal(err)
		}
		h, _, err := join.ParseHello(b, join.Stream)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	// A Hello that does not open.
	if _, r := s.OpenHello(hello(bytes.Repeat([]byte{1}, 32)), FramingBytes, nil); r != JoinClose {
		t.Fatalf("bad key: %v", r)
	}
	// A verified join for an ID the session already has: a replay.
	p.id = 3
	if _, r := s.OpenHello(hello(key), FramingBytes, nil); r != JoinClose {
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
	s.feat.PipeAck = true
	if pi, _ := s.PipeInfo(p.id); pi.Lanes != nil || pi.LanesOut != 1 {
		t.Fatalf("a pipe without lanes: %+v, %d", pi.Lanes, pi.LanesOut)
	}
	p.out.lanes = append(p.out.lanes, &outLane{lane: 1 << 32, pos: 1 << 40, acked: 30, refused: true})
	p.out.lanes[0].pos, p.out.lanes[0].acked = 10, 5
	p.in = &inDir{lanes: []*inLane{{lane: 0, pos: 40}, {lane: 1, pos: 0}}}
	pi, _ := s.PipeInfo(p.id)
	want := []LaneInfo{{Lane: 0, PositionSent: 10, PositionAcked: 5, PositionReceived: 40, HasOut: true, HasAck: true, HasIn: true},
		{Lane: 1, HasIn: true}, {Lane: 1 << 32, PositionSent: 1 << 40, PositionAcked: 30, HasOut: true, HasAck: true}}
	if len(pi.Lanes) != len(want) {
		t.Fatalf("lanes %+v", pi.Lanes)
	}
	for i := range want {
		if pi.Lanes[i] != want[i] {
			t.Fatalf("lane %d: %+v, want %+v", i, pi.Lanes[i], want[i])
		}
	}
	if pi.LanesOut != 2 || pi.LanesIn != 2 || pi.LanesRefused != 1 || pi.BytesSent != 1<<40+10 {
		t.Fatalf("lanes out %d in %d refused %d, sent %d", pi.LanesOut, pi.LanesIn, pi.LanesRefused, pi.BytesSent)
	}
	s.feat.PipeAck, s.feat.Multipath = false, false
	pi, _ = s.PipeInfo(p.id)
	if l := pi.Lanes[0]; !l.HasOut || l.HasAck || l.PositionAcked != 0 || !l.HasIn {
		t.Fatalf("positions without acknowledgements: %+v", l)
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
	s.syncUnsent(st)
	s.conn.buffered = 7
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
	s.queueDatagram(st, receivedDgram{data: []byte("ab")})
	s.queueDatagram(st, receivedDgram{data: []byte("cde")})
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

var testJoinGroup, _ = join.LookupGroup(join.GroupX25519MLKEM768)

// A local join's Group must be usable in the session.
func TestAddPipeGroupNotUsable(t *testing.T) {
	s, _ := statsMP()
	s.mp.usableGroups = []uint64{join.GroupX25519MLKEM768}
	setTestKeys(s, join.SuiteAES, make([]byte, 32))
	s.mp.ep.join.peer = []byte("L")
	if _, _, err := s.AddPipe(JoinOptions{Group: join.GroupX25519}); !errors.Is(err, ErrGroupNotUsable) {
		t.Fatal(err)
	}
}

// setTestKeys gives the session epoch 0 with one join key for both roles.
func setTestKeys(s *Session, suite uint64, key []byte) {
	e := &epoch{keys: map[uint64]*joinKeys{suite: {i: key, r: key}}, local: s.cfg.Local.Locator}
	s.mp.ep = epochs{list: []*epoch{e}, join: e, last: -1}
}

// TestStreamDatagramBytes (N18): a stream's datagram queue is bounded by
// count and by payload bytes, the oldest dropped and counted one by one;
// an unlimited byte bound leaves the count; a datagram above the byte
// bound is kept alone; the session's byte bound is unaffected; the
// defaults keep 32 datagrams of 9000 bytes.
func TestStreamDatagramBytes(t *testing.T) {
	fill := func(q int, size uint64, n, each int) (*Session, *Stream) {
		s := capacitySession()
		s.cfg.DgramStreamQ, s.cfg.DgramStreamSize = q, size
		st := s.newStream(true, false)
		st.id, st.idSet = 1, true
		for i := range n {
			p := make([]byte, each)
			p[0] = byte(i)
			s.queueDatagram(st, receivedDgram{data: p})
		}
		return s, st
	}
	s, st := fill(32, 16<<10, 10, 4<<10)
	if in := s.StreamInfo(st); len(st.dg.q) != 4 || in.DatagramsDroppedRecv != 6 || st.dg.size != 16<<10 {
		t.Fatalf("16 KiB: %d kept, %d dropped, %d bytes", len(st.dg.q), in.DatagramsDroppedRecv, st.dg.size)
	}
	if s.dg.recvSize != 0 || s.stats.DatagramsDroppedRecv != 6 {
		t.Fatalf("session: %d bytes, %d dropped", s.dg.recvSize, s.stats.DatagramsDroppedRecv)
	}
	for i := 6; i < 10; i++ {
		if _, p, ok := s.RecvStreamDatagram(st); !ok || p[0] != byte(i) {
			t.Fatalf("datagram %d: %v %v", i, p[:1], ok)
		}
	}
	if st.dg.size != 0 {
		t.Fatalf("an empty queue holds %d bytes", st.dg.size)
	}
	if s, st = fill(32, 0, 10, 4<<10); len(st.dg.q) != 10 || s.stats.DatagramsDroppedRecv != 0 {
		t.Fatalf("unlimited: %d kept", len(st.dg.q))
	}
	if s, st = fill(32, 16<<10, 3, 20<<10); len(st.dg.q) != 1 || s.stats.DatagramsDroppedRecv != 2 {
		t.Fatalf("above the bound: %d kept, %d dropped", len(st.dg.q), s.stats.DatagramsDroppedRecv)
	}
	d := DefaultConfig()
	if s, st = fill(d.DgramStreamQ, d.DgramStreamSize, 32, 9000); len(st.dg.q) != 32 || s.stats.DatagramsDroppedRecv != 0 {
		t.Fatalf("defaults: %d kept", len(st.dg.q))
	}
}

func TestBootstrapJoinStateAllocatedOnDemand(t *testing.T) {
	s := capacitySession()
	crypto := wire.SessionCrypto{Suites: []uint64{wire.RequiredSuite}, Groups: []uint64{wire.RequiredGroup}}
	s.cfg.Local.SessionCrypto, s.peer.SessionCrypto = crypto, crypto
	s.declared = true
	s.peer.Locator = []byte("peer")
	if err := s.startMultipath(); err != nil {
		t.Fatal(err)
	}
	ep := s.mp.ep.join
	if ep == nil || !s.UsableSuite(wire.RequiredSuite) || ep.keys != nil || s.mp.additionalPipes != nil {
		t.Fatal("bootstrap initialized join-only state or lost epoch zero")
	}
	if _, _, err := s.AddPipe(JoinOptions{}); err != nil {
		t.Fatal(err)
	}
	if s.mp.additionalPipes == nil || ep.keys[wire.RequiredSuite] == nil {
		t.Fatal("join did not materialize negotiated keys")
	}
}

func TestFirstAdditionalPipeAfterEpochChange(t *testing.T) {
	s, _ := probeSession(DefaultConfig())
	s.role = RoleResponder
	suite, _ := join.LookupSuite(join.SuiteAES)
	group, _ := join.LookupGroup(join.GroupX25519)
	s.cfg.Local.SessionCrypto.Suites = []uint64{suite.ID}
	s.peer.SessionCrypto.Suites = []uint64{suite.ID}
	s.mp.usableGroups = []uint64{group.ID}
	old := &epoch{secret: bytes.Repeat([]byte{1}, 32), local: []byte("old-phase"), peer: []byte("peer-loc0")}
	s.mp.ep = epochs{list: []*epoch{old}, join: old, crypto: []byte("crypto"), last: -1}
	if err := s.commit(nil, nil, nil, []byte("nextphase"), nil); err != nil {
		t.Fatal(err)
	}
	s.useNext()
	if s.mp.additionalPipes != nil {
		t.Fatal("rotating epoch initialized additional pipe state")
	}
	hello := func(e *epoch, id uint64) join.Hello {
		key, err := join.PSK(suite, e.secret, join.RoleInitiator, s.mp.ep.crypto)
		if err != nil {
			t.Fatal(err)
		}
		j, err := join.NewJoiner(join.JoinerConfig{Suite: suite, Group: group, PSK: key, Locator: e.local, Phase: uint8(e.n % 2), Mode: join.Sealed, Framing: join.Stream})
		if err != nil {
			t.Fatal(err)
		}
		msg, err := j.WriteHello(nil, join.HelloPayload{Pipe: join.Pipe{ID: id, Direction: 2}})
		if err != nil {
			t.Fatal(err)
		}
		h, _, err := join.ParseHelloExact(msg, join.Stream)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	prior := hello(old, 2)
	current := hello(s.mp.ep.join, 4)
	check := func(h join.Hello, want JoinResult) {
		_, r := s.OpenHello(h, FramingBytes, nil)
		if r != want {
			t.Fatalf("phase %d result %v, want %v", h.Phase, r, want)
		}
	}
	check(prior, JoinAccepted)
	check(current, JoinAccepted)
	s.deleteOld()
	check(prior, JoinClose)
	check(current, JoinAccepted)
}

func TestConfiguredDeadlines(t *testing.T) {
	s := capacitySession()
	s.cfg.KeepAlive = 0
	s.cfg.IdleTimeout = 1500 * Microsecond
	s.cfg.Local.MaxIdleTimeout, s.peer.MaxIdleTimeout = 2, 2
	s.timers.lastActivity = 1
	if at := s.sessionTimeout(); at != Time(1+1500*Microsecond) {
		t.Fatalf("idle deadline %d", at)
	}
	s.peer.MaxIdleTimeout = 1
	if at := s.sessionTimeout(); at != Time(1+Millisecond) {
		t.Fatalf("peer-limited idle deadline %d", at)
	}
	s.cfg.IdleTimeout = Duration(Never)
	s.peer.MaxIdleTimeout = 1<<62 - 1
	if at := s.sessionTimeout(); at != Never || !s.sessionTimers(Never) {
		t.Fatalf("overflowing idle deadline %d", at)
	}
	s.state = StateSuspended
	s.timers.grace = Time(1).Add(Duration(Never))
	if at := s.sessionTimeout(); at != Never || !s.sessionTimers(Never) {
		t.Fatalf("overflowing resume deadline %d", at)
	}
}

func TestPipeReplacementDeadline(t *testing.T) {
	s := capacitySession()
	if st, _ := s.PipeInfo(0); st.EpochKnown || st.ReplaceBy != Never {
		t.Fatalf("non-multipath pipe: epoch known %t, replace by %d", st.EpochKnown, st.ReplaceBy)
	}
	s, p := statsMP()
	p.mp.joinEpoch, p.mp.joinEpochKnown, p.mp.attachedAt = 5, true, 100
	s.cfg.EpochPeriod = Second
	if st, _ := s.PipeInfo(p.id); !st.EpochKnown || st.Epoch != 5 || st.ReplaceBy != Time(100+2*Second) {
		t.Fatalf("epoch %d (known %t), replace by %d", st.Epoch, st.EpochKnown, st.ReplaceBy)
	}
	s.cfg.EpochPeriod = Duration(Never)
	if st, _ := s.PipeInfo(p.id); st.ReplaceBy != Never {
		t.Fatalf("overflowing replacement deadline %d", st.ReplaceBy)
	}
}
