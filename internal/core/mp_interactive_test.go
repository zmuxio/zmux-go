package core

import (
	"bytes"
	"cmp"
	"slices"
	"testing"
	"time"
)

type rrArrival struct {
	to   *Session
	data []byte
	pipe uint64
	at   Time
}

// TestSpreadETAInteractive replays write opportunities on actual core
// sessions. It seeds the estimates at the start of each exchange to replay
// the same short observation window after a collapse. The carrier clock
// models data delivery independently of those estimates. The replay does not
// drive timer callbacks between write opportunities.
func TestSpreadETAInteractive(t *testing.T) {
	cfg := cwMPConfigs()
	for i := range cfg {
		cfg[i].SessionPolicy.Limits.SteadyAttachments = 12
		cfg[i].Policy = PolicySpreadETA
		cfg[i].TraceStreams = true
		cfg[i].Probation = -1
		cfg[i].Test.Seed = uint64(i + 1)
	}
	m := newCWSim(t, cfg, CarrierInfo{Framing: FramingRecords}, Millisecond)
	m.established()
	fast, peer := m.end[0], m.end[1]
	var ids []uint64
	for range 8 {
		ids = append(ids, m.join())
	}
	high, impaired := ids[6], ids[7]
	m.run(500*Millisecond, func() bool { return false })

	var arrivals []rrArrival
	transmit := func(s, to *Session, id uint64, delay Duration, rate uint64) {
		b := GetBatch()
		defer PutBatch(b)
		if !s.PollTransmit(id, 0, b, DefaultBatch, m.now) {
			return
		}
		at := m.now.Add(delay)
		for _, rec := range b.Messages() {
			if rate > 0 {
				at = at.Add(timeAt(uint64(len(rec)), float64(rate)))
			}
			arrivals = append(arrivals, rrArrival{to: to, pipe: id, at: at, data: bytes.Clone(rec)})
		}
		s.WriteDone(b, b.Len(), nil, m.now)
		slices.SortStableFunc(arrivals, func(a, b rrArrival) int { return cmp.Compare(a.at, b.at) })
	}
	deliver := func() *Session {
		a := arrivals[0]
		arrivals[0] = rrArrival{}
		arrivals = arrivals[1:]
		m.now = a.at
		receiveMessageTest(a.to, a.pipe, 0, a.data, a.at)
		return a.to
	}
	drain := func(s *Session, kind EventKind, id, offset uint64) (Event, bool) {
		var found Event
		for e, ok := s.PollEvent(); ok; e, ok = s.PollEvent() {
			if e.Kind == kind && e.Stream == id && e.Offset == offset {
				found = e
			}
		}
		return found, found.Kind != 0
	}
	seedEstimates := func(s *Session) {
		for i, p := range s.pipes {
			rtt := Duration(277+4*i) * Millisecond
			minRTT := rtt
			switch p.id {
			case high:
				rtt, minRTT = 970*Millisecond, 970*Millisecond
			case impaired:
				// The impaired pipe remains in the same active tier.
				rtt, minRTT = 2770*Millisecond, 295*Millisecond
			}
			p.mp.rtt = rttEst{has: true, min: minRTT, smoothed: rtt}
			*p.mp.quality = pathQuality{minRTT: minRTT, minAt: s.now, rate: 2e6, rateAt: s.now}
			p.out.snd.re.r, p.out.snd.re.measured = 2e6, true
			p.out.snd.re.hasSample = true
		}
		if !s.pipeUsable(s.pipe(impaired), false) {
			t.Fatal("impaired pipe left the candidate set")
		}
	}
	write := func(s *Session, st *Stream, data []byte) {
		t.Helper()
		if n, err := s.Write(st, data, false); n != len(data) || err != nil {
			t.Fatalf("write %d of %d: %v", n, len(data), err)
		}
	}
	read := func(s *Session, st *Stream, data []byte) int {
		t.Helper()
		n, _, err := s.Read(st, data)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	st, err := fast.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	write(fast, st, []byte{1})
	transmit(fast, peer, 0, 138500*Microsecond, 0)
	for len(arrivals) != 0 {
		deliver()
	}
	remote, ok := peer.AcceptStream(true)
	if !ok || read(peer, remote, make([]byte, 1)) != 1 {
		t.Fatal("stream did not open")
	}
	id, _ := st.StreamID()
	drain(fast, 0, 0, 0)
	drain(peer, 0, 0, 0)

	var rtts, gaps []Duration
	for i := range 100 {
		seedEstimates(fast)
		seedEstimates(peer)
		start := m.now
		off := st.snd.sent
		// Two smaller writes before waiting on the response. A slow pipe
		// takes its write opportunity first in 1/16 of the exchanges.
		n := 256 + (i/16)*64
		request := make([]byte, n+32)
		for j := range request {
			request[j] = byte(i + j)
		}
		write(fast, st, request[:n])
		if i%16 == 15 {
			// The high-RTT pipe also gets an opportunity but is never due
			// this prefix, with its current minimum RTT correctly estimated.
			transmit(fast, peer, high, 485*Millisecond, 0)
			transmit(fast, peer, impaired, 1385*Millisecond, 12)
		}
		placement, placed := drain(fast, EvStreamPlaced, id, off)
		secondWrite := start.Add(Millisecond)
		for len(arrivals) != 0 && arrivals[0].at <= secondWrite {
			deliver()
		}
		m.now = secondWrite
		write(fast, st, request[n:])
		transmit(fast, peer, 0, 138500*Microsecond, 0)
		got := make([]byte, len(request))
		var gotN int
		var exposed Event
		// Reply when the stream is readable in full. Control records still
		// in flight stay on the carrier while the exchange continues.
		for gotN < len(got) && len(arrivals) != 0 {
			if deliver() != peer {
				continue
			}
			gotN += read(peer, remote, got[gotN:])
			for e, ok := peer.PollEvent(); ok; e, ok = peer.PollEvent() {
				if e.Stream != id || e.Offset != off {
					continue
				}
				switch e.Kind {
				case EvStreamGapStarted:
					if e.Pipe != 0 || gotN != 0 {
						t.Fatalf("unexpected gap start %+v after reading %d bytes", e, gotN)
					}
					exposed = e
				case EvStreamGapFilled:
					if !placed || placement.Pipe != impaired || e.Pipe != impaired ||
						exposed.Kind != EvStreamGapStarted || e.Wait != e.At.Sub(exposed.At) {
						t.Fatalf("placement %+v, gap start %+v, fill %+v", placement, exposed, e)
					}
					gaps = append(gaps, e.Wait)
				default:
					// Other events of this stream are not part of the replay.
				}
			}
		}
		if gotN != len(request) || !bytes.Equal(got, request) {
			t.Fatalf("request %d: got %d of %d ordered bytes", i, gotN, len(request))
		}
		write(peer, remote, got)
		transmit(peer, fast, 0, 138500*Microsecond, 0)
		gotN = 0
		for gotN < len(got) && len(arrivals) != 0 {
			if deliver() == fast {
				gotN += read(fast, st, got[gotN:])
			}
		}
		if gotN != len(got) || !bytes.Equal(got, request) {
			t.Fatalf("echo %d: invalid response", i)
		}
		drain(fast, 0, 0, 0)
		drain(peer, 0, 0, 0)
		rtts = append(rtts, m.now.Sub(start))
	}
	slices.Sort(rtts)
	slices.Sort(gaps)
	t.Logf("100 RR exchanges: p50=%v p99=%v; gap waits=%v",
		time.Duration(rtts[49]), time.Duration(rtts[98]), gapsAsDurations(gaps))
	if len(gaps) != 0 || rtts[98] != 278*Millisecond {
		t.Fatal("a slow placement delayed an interactive exchange")
	}
	stats := peer.SessionStats()
	if stats.StreamGapsStarted != uint64(len(gaps)) || stats.StreamGapsFilled != uint64(len(gaps)) {
		t.Fatalf("gap counters: started %d, filled %d, read %d", stats.StreamGapsStarted, stats.StreamGapsFilled, len(gaps))
	}
}

func gapsAsDurations(gaps []Duration) []time.Duration {
	out := make([]time.Duration, len(gaps))
	for i, gap := range gaps {
		out[i] = time.Duration(gap)
	}
	return out
}

// The gap charge is the same for interactive and bulk sends: serialization
// of a real ready tail can pay the cost of sending on the slower pipe.
func TestSpreadETAOrderedBudget(t *testing.T) {
	s, g := mpBenchSession(2)
	s.cfg.Policy = PolicySpreadETA
	fast, slow := g.pipes[0], g.pipes[1]
	fast.mp.rtt = rttEst{has: true, min: 100 * Millisecond, smoothed: 100 * Millisecond}
	slow.mp.rtt = rttEst{has: true, min: 108 * Millisecond, smoothed: 108 * Millisecond}
	measuredRate(fast, 1e6)
	slow.out.snd.re.r, slow.out.snd.re.measured = 1e6, true
	s.conn.sendMax = 1 << 20
	st := &Stream{urgency: 3, incr: true, hasS: true}
	st.snd.maxData = 1 << 20
	for _, x := range []struct {
		want  *Pipe
		ready uint64
	}{{ready: 256, want: fast}, {ready: 256 << 10, want: slow}} {
		st.snd.q.end = x.ready
		s.sched.ensure(st)
		var d decision
		s.newDecision(&d, slow)
		if got := s.onPipe(&d, st); got != (x.want == slow) {
			t.Fatalf("%d ready bytes: slow pipe allowed=%v, want pipe %d", x.ready, got, x.want.id)
		}
	}
	for range 20 {
		slow.out.snd.re.sample(12, true, true, 0, s.now, slow.mp.quality)
	}
	var d decision
	s.newDecision(&d, slow)
	if s.onPipe(&d, st) {
		t.Fatal("a measured 12 B/s pipe took bulk data within the same budget")
	}
}

// While a pipe remains active, overdue progress increases its ETA. The
// same ACK-stall deadline still triggers the existing suspicion transition.
func TestSpreadETAAckStall(t *testing.T) {
	cfg := DefaultConfig()
	s, impaired := stallSession(cfg, false, PipeLiveness{})
	fast := capacityExtra(s)
	for i, p := range []*Pipe{impaired, fast} {
		rtt := Duration(100+i) * Millisecond
		p.mp.rtt = rttEst{has: true, min: rtt, smoothed: rtt}
		measuredRate(p, 1e6)
	}
	s.refreshPathTopology()
	a := impaired.mp
	if etaOf(s, impaired) >= etaOf(s, fast) {
		t.Fatal("before ACK delay, the initially faster pipe has a worse ETA")
	}
	s.SetNow(a.tref().Add(s.pto(a) + 10*Millisecond))
	if !s.pipeUsable(impaired, false) || etaOf(s, impaired) <= etaOf(s, fast) {
		t.Fatal("overdue but still active pipe did not receive an ETA penalty")
	}
	due := a.tref().Add(s.tSuspect(a))
	s.SetNow(due)
	s.mpHandleTimeout(due)
	if a.out != OutSuspect || s.pipeUsable(impaired, false) {
		t.Fatal("the existing liveness timer did not remove the suspect pipe")
	}
}

func TestSpreadETAOtherBacklogCannotDelaySingleChunk(t *testing.T) {
	s, g := mpBenchSession(2)
	s.cfg.Policy = PolicySpreadETA
	fast, slow := g.pipes[0], g.pipes[1]
	observePath(s, fast, 100*Millisecond, 1e6)
	observePath(s, slow, 108*Millisecond, 1e6)
	s.conn.sendMax = 1 << 20
	bulk := &Stream{hasS: true, urgency: 3, incr: false}
	bulk.snd.q.end, bulk.snd.maxData = 256<<10, 1<<20
	s.sched.ensure(bulk)
	rr := &Stream{hasS: true, urgency: 3, incr: true}
	rr.snd.q.end, rr.snd.maxData = 64, 1<<20
	s.sched.ensure(rr)
	var d decision
	s.newDecision(&d, slow)
	if s.onPipe(&d, rr) || !s.onPipe(&d, bulk) {
		t.Fatal("other-stream backlog changed single-chunk placement or blocked useful bulk spreading")
	}
}

func TestSpreadETAQueuesAndACKDelayHaveOneAccountingEntry(t *testing.T) {
	s, p := statsMP()
	s.ack = newAckState(s)
	s.SetNow(Time(Second))
	observePath(s, p, 100*Millisecond, 1e6)
	p.mp.quality.rtt = 200 * Millisecond
	p.mp.quality.minRTT = 100 * Millisecond
	pr, _ := p.mp.effPrio()
	tops := s.fastestOf(pr)
	got := s.estimate(p, &tops, 50_000, 0)
	if got.arrival != .1 {
		t.Fatalf("RTT and queue overlap counted twice: %.6f", got.arrival)
	}
	s.ack.peerDelay = 10 * Millisecond
	if again := s.estimate(p, &tops, 50_000, 0); again != got {
		t.Fatal("ETA subtracted ACK delay from an already adjusted observation")
	}
	s.SetNow(Time(5 * Second))
	if idle := s.estimate(p, &tops, 50_000, 0); idle.arrival != got.arrival {
		t.Fatal("idle time counted as unacknowledged data")
	}
	p.mp.unacked = true
	if pending := s.estimate(p, &tops, 50_000, 0); pending.arrival <= got.arrival {
		t.Fatal("pending data with no ACK made no difference")
	}
}

func TestStreamSpreadETAUsesTierSnapshot(t *testing.T) {
	s, _ := mpBenchSession(3)
	s.cfg.Policy = PolicySessionAffinity
	s.cfg.OverflowDelay = Second
	s.SetNow(Time(Second))
	s.pipes[2].mp.sendPrio = new(uint64(1))
	for _, p := range s.pipes {
		observePath(s, p, 100*Millisecond, 1000)
		p.out.snd.re.r = 1e8 // protocol pacing peak is unrelated to ETA
	}
	s.conn.sendMax = 1 << 20
	st := &Stream{hasS: true}
	st.snd.maxData, st.snd.q.end = 4096, 4096
	s.sched.ensure(st)
	s.SetStreamPolicy(st, PolicySpreadETA, false)
	g := s.buildGate(&gate{})
	if g == nil {
		t.Fatal("no gate for a multipath session")
	}
	q0, q1 := g.quality[0], g.quality[1]
	if g.affinityOnly || g.tiers[0].r != q0.rate+q1.rate || g.last != 1 {
		t.Fatalf("stream ETA override did not drive tier overflow: %+v", g.tiers)
	}
}
