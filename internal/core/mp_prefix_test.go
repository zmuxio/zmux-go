package core

import (
	"bytes"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

type prefixReplay struct {
	t           *testing.T
	sim         *cwSim
	st          *Stream
	source, alt *Pipe
}

func newPrefixReplay(t *testing.T) prefixReplay {
	t.Helper()
	return newSeededPrefixReplay(t, [2]uint64{})
}

func newSeededPrefixReplay(t *testing.T, seeds [2]uint64) prefixReplay {
	t.Helper()
	cfg := cwMPConfigs()
	for i := range cfg {
		cfg[i].Policy, cfg[i].TraceStreams, cfg[i].Probation = PolicySpreadETA, true, -1
		cfg[i].Test.Seed = seeds[i]
	}
	m := newCWSim(t, cfg, CarrierInfo{Framing: FramingRecords}, Millisecond)
	m.established()
	alt := m.join()
	m.run(500*Millisecond, func() bool { return false })
	// A record sent on the last tick can arrive on the next tick.
	// Deliver queued records before switching to manual delivery.
	for _, link := range m.links {
		if n := len(link.q); n != 0 {
			m.now = maxT(m.now, link.q[n-1].at)
		}
	}
	for i := range 2 {
		m.end[i].SetNow(m.now)
		m.deliver(i)
	}
	for from, end := range m.end {
		for _, p := range end.pipes {
			sent := p.out.lanes[0].pos
			received := m.end[from^1].pipe(p.id).in.lanes[0].pos
			if sent != received {
				t.Fatalf("replay handoff side %d pipe %d: sent=%d received=%d", from, p.id, sent, received)
			}
		}
	}
	s := m.end[0]
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range s.pipes {
		p.mp.rtt = rttEst{}
		*p.mp.quality = pathQuality{}
		rtt := 277 * Millisecond
		if p.id == alt {
			rtt = 300 * Millisecond
		}
		observePath(s, p, rtt, 2e6)
	}
	return prefixReplay{t: t, sim: m, st: st, source: s.pipe(0), alt: s.pipe(alt)}
}

func (x prefixReplay) put(p *Pipe, b []byte) [][]byte {
	x.t.Helper()
	if len(b) > 0 {
		if n, err := x.sim.end[0].Write(x.st, b, false); n != len(b) || err != nil {
			x.t.Fatalf("write %d of %d: %v", n, len(b), err)
		}
	}
	return x.transmit(0, p.id)
}

func (x prefixReplay) transmit(from int, pipe uint64) [][]byte {
	s := x.sim.end[from]
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(pipe, 0, b, DefaultBatch, x.sim.now) {
		return nil
	}
	var records [][]byte
	for _, r := range b.Messages() {
		records = append(records, bytes.Clone(r))
	}
	s.WriteDone(b, nil, x.sim.now)
	return records
}

func (x prefixReplay) arrive(to int, pipe uint64, msg [][]byte, now Time) {
	x.sim.now = now
	for _, m := range msg {
		x.sim.end[to].ReceiveMessage(pipe, 0, &Buf{B: m}, m, now)
	}
}

func (x prefixReplay) ackOriginal() {
	x.t.Helper()
	s := x.sim.end[0]
	if e := s.mpAckEntry(wire.AckEntry{PipeID: x.source.id, Position: x.source.out.lanes[0].pos}, x.sim.now); e != nil {
		x.t.Fatal(e)
	}
}

// The originally selected pipe is healthy at handover. Immediately after
// that write, its simulated delivery collapses to 12 B/s. Its ACK-derived
// quality is still unchanged when the deadline fires.
func TestSpreadETAPrefixRepairAfterCollapse(t *testing.T) {
	seed := rand.Uint64()
	if value, ok := os.LookupEnv("ZMUX_PREFIX_REPLAY_SEED"); ok {
		var err error
		seed, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	runPrefixCollapseReplay(t, seed)
}

func TestSpreadETAPrefixRepairAfterCollapseQueuedSetupRecord(t *testing.T) {
	// The 14th zero-based case leaves 22 bytes in flight on the backup
	// at the end of setup. Its ACK must cover the later repair as well.
	runPrefixCollapseReplay(t, 14154243759933347797)
}

func runPrefixCollapseReplay(t *testing.T, seed uint64) {
	t.Helper()
	t.Logf("prefix replay seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	var gaps []Duration
	var copied, app uint64
	for i := range 100 {
		seeds := [2]uint64{rng.Uint64() | 1, rng.Uint64() | 1}
		x := newSeededPrefixReplay(t, seeds)
		m := x.sim
		s, peer := m.end[0], m.end[1]
		start := m.now

		x.source.mp.rtt.rttvar = Duration(30+i%101) * Millisecond
		a := bytes.Repeat([]byte{1}, 256+i*3)
		b := bytes.Repeat([]byte{2}, 32)
		original := x.put(x.source, a)
		if len(original) == 0 || s.mp.prefixAt <= start {
			t.Fatal("healthy pipe did not take and retain the original prefix")
		}
		deadline := s.mp.prefixAt
		originalAt := start.Add(1385*Millisecond + timeAt(uint64(len(a)), 12))
		m.now = start.Add(Millisecond)
		// The carrier still accepts writes; queue growth is visible sooner
		// than the delivery/RTT samples for the record already handed off.
		s.SetCarrierUnsent(x.source.id, 0, 128<<10, m.now)
		suffix := x.put(x.alt, b)
		x.arrive(1, x.alt.id, suffix, m.now.Add(150*Millisecond))
		if len(suffix) == 0 || peer.SessionStats().StreamGapsStarted != 1 {
			t.Fatal("the suffix did not expose the retained prefix hole")
		}
		m.now = deadline - 1
		s.HandleTimeout(m.now)
		if x.st.snd.mps.resend.len() != 0 {
			t.Fatal("repair queued before the frozen deadline")
		}
		m.now = deadline
		s.HandleTimeout(m.now)
		if x.source.mp.out != OutActive || s.mp.prefixBytes == 0 {
			t.Fatal("deadline failed to queue repair while the original was active")
		}
		copyBefore := s.SessionStats().BytesReinjected
		repair := x.put(x.alt, nil)
		copyBytes := s.SessionStats().BytesReinjected - copyBefore
		if len(repair) == 0 || copyBytes < uint64(len(a)) || copyBytes > uint64(s.cfg.SpreadChunk) ||
			s.mp.prefixBytes != copyBytes {
			t.Fatalf("repair size=%d or reservation=%d", copyBytes, s.mp.prefixBytes)
		}
		x.arrive(1, x.alt.id, repair, deadline.Add(150*Millisecond))
		remote, ok := peer.AcceptStream(true)
		want := append(bytes.Clone(a), b...)
		got := make([]byte, len(want))
		if !ok {
			t.Fatal("repair did not open the remote stream")
		}
		if n, _, err := peer.Read(remote, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
			t.Fatalf("read %d of %d, %v", n, len(got), err)
		}
		ps := peer.SessionStats()
		if ps.StreamGapsFilled != 1 || ps.StreamGapWait > 500*Millisecond {
			t.Fatalf("gap count %d, wait %v", ps.StreamGapsFilled, time.Duration(ps.StreamGapWait))
		}
		gaps = append(gaps, ps.StreamGapWait)
		// Native loss of the original must not queue a third copy.
		s.makeSuspect(x.source)
		if x.st.snd.mps.resend.len() != 0 {
			t.Fatal("original suspicion queued an already in-flight copy")
		}
		// A real PIPE_ACK for the backup releases the byte reservation.
		m.now = m.now.Add(30 * Millisecond)
		peer.HandleTimeout(m.now)
		for _, pipe := range peer.pipes {
			ack := x.transmit(1, pipe.id)
			x.arrive(0, pipe.id, ack, m.now.Add(150*Millisecond))
		}
		if s.mp.prefixBytes != 0 {
			lane := x.alt.out.lanes[0]
			t.Fatalf("case %d: backup acknowledgement did not release reservation (sent=%d received=%d acked=%d)",
				i, lane.pos, peer.pipe(x.alt.id).in.lanes[0].pos, lane.acked)
		}
		x.arrive(1, x.source.id, original, originalAt)
		if n, _, err := peer.Read(remote, got); n != 0 || err != nil {
			t.Fatalf("late original delivered a duplicate: %d, %v", n, err)
		}
		copied += copyBytes
		app += uint64(len(want))
	}
	slices.Sort(gaps)
	t.Logf("100 immediate collapses: gap p50=%v p99=%v max=%v, %d retransmitted wire stream bytes / %d application bytes",
		time.Duration(gaps[49]), time.Duration(gaps[98]), time.Duration(gaps[99]), copied, app)
}

func TestPrefixRepairOriginalACKCancels(t *testing.T) {
	for _, when := range []string{"before deadline", "at deadline", "after reservation"} {
		t.Run(when, func(t *testing.T) {
			x := newPrefixReplay(t)
			s := x.sim.end[0]
			x.put(x.source, bytes.Repeat([]byte{7}, 256))
			deadline := s.mp.prefixAt
			if when != "before deadline" {
				x.sim.now = deadline
			}
			if when == "after reservation" {
				s.HandleTimeout(deadline)
				if s.mp.prefixBytes == 0 {
					t.Fatal("no repair pending for the cancellation test")
				}
			}
			x.ackOriginal()
			x.sim.now = deadline
			s.HandleTimeout(deadline)
			x.put(x.alt, nil)
			if s.mp.prefixBytes != 0 || x.st.snd.mps.resend.len() != 0 || s.SessionStats().BytesReinjected != 0 {
				t.Fatal("an original ACK did not cancel the unsent copy")
			}
		})
	}
}

func TestPrefixRepairNativeLossTakesPendingRange(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{7}, 256))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	if s.mp.prefixBytes == 0 {
		t.Fatal("deadline did not reserve a pending repair")
	}
	// Even an unmeasured backup can take ordinary reinjection when the
	// original actually fails. Only speculative copies require fresh RTT.
	x.alt.mp.quality.rtt = 0
	s.makeSuspect(x.source)
	if s.mp.prefixBytes != 0 || x.st.snd.mps.resend.len() == 0 {
		t.Fatal("native loss did not inherit the pending range")
	}
	if len(x.put(x.alt, nil)) == 0 || s.SessionStats().BytesReinjected < 256 ||
		s.SessionStats().BytesOpportunistic != 0 {
		t.Fatal("native reinjection did not take over")
	}
}

func TestPrefixRepairSharesSessionCopyBound(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	bound := s.copyBound()
	// Another stream already reserved most of the existing redundant
	// copy budget. The timer may only use the remaining bytes.
	disjoint := &Stream{hasS: true}
	s.mp.copies = append(s.mp.copies, &copyRec{st: disjoint, end: bound - 128})
	x.put(x.source, bytes.Repeat([]byte{3}, 256))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	if live := s.mp.prefixBytes; live != 128 || s.copiesOutstanding() != bound {
		t.Fatalf("prefix reservation %d, all copies %d, shared limit %d", live, s.copiesOutstanding(), bound)
	}
}

func TestPrefixRepairRequiresTrustedAlternative(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{7}, 256))
	x.alt.mp.quality.rtt = 0
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	if s.mp.prefixBytes != 0 || x.st.snd.mps.resend.len() != 0 {
		t.Fatal("an unmeasured alternative justified a speculative copy")
	}
}

func TestPrefixRepairWaitsForPeerAttachment(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{7}, 256))
	x.alt.mp.peer = peerPending
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	if s.mp.prefixBytes != 0 || x.st.snd.mps.resend.len() != 0 {
		t.Fatal("unconfirmed alternative reserved a speculative copy")
	}

	s.sawPeer(x.alt)
	s.HandleTimeout(x.sim.now)
	if s.mp.prefixBytes == 0 || len(x.put(x.alt, nil)) == 0 {
		t.Fatal("peer attachment did not make the alternative eligible")
	}
}

func TestPrefixRepairWaitsForMeaningfulProgress(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{7}, 512))
	preceding := x.source.out.lanes[0].pos
	var err error
	x.st, err = s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	x.put(x.source, bytes.Repeat([]byte{8}, 256))
	deadline := s.mp.prefixAt
	x.sim.now = deadline - 1
	s.SetNow(x.sim.now)
	if e := s.mpAckEntry(wire.AckEntry{PipeID: x.source.id, Position: preceding}, x.sim.now); e != nil {
		t.Fatal(e)
	}
	s.HandleTimeout(deadline)
	s.HandleTimeout(deadline.Add(60 * Millisecond))
	if s.mp.prefixBytes != 0 || x.st.snd.mps.resend.len() != 0 {
		t.Fatal("a recent substantive ACK advance did not defer repair")
	}
}

func TestResendTrimsNoncontiguousACK(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{7}, 256))
	st, m := x.st, x.st.snd.mps
	m.resend.add(64, 160)
	s.acked(&frameDesc{kind: dStream, id: st.id, off: 96, n: 32})
	if m.acked.prefix() != 0 || m.resend.covered(96, 128) != 0 {
		t.Fatal("higher ACK did not remove the pending range")
	}
	m.resend.add(64, 160) // stale enqueue: the encoder still has to trim
	for _, want := range []rng{{64, 96}, {128, 160}} {
		p, ok := s.planResend(st)
		if !ok || p.off != want.lo || p.off+p.n != want.hi {
			t.Fatalf("plan %+v contains acknowledged data, want %+v", p, want)
		}
		s.dropResend(m, p.off, p.off+p.n)
	}
	if _, ok := s.planResend(st); ok {
		t.Fatal("an ACKed hole was sent after both missing pieces")
	}
}

func TestPrefixRepairBucketChargeIsCommittedPayload(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{3}, 256))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	n, b := s.mp.prefixPending, &s.mp.prefixBucket
	if n < 256 || b.tokens != b.burst {
		t.Fatal("pending reservation spent bucket credit prematurely")
	}
	if len(x.put(x.alt, nil)) == 0 || b.tokens != b.burst-int64(n) || s.mp.prefixPending != 0 ||
		s.stats.BytesOpportunistic != n {
		t.Fatalf("commit did not charge exactly %d STREAM bytes", n)
	}
	x.ackOriginal()
	if s.mp.prefixBytes != 0 || b.tokens != b.burst-int64(n) || s.prefixTimers(s.now, false) != Never {
		t.Fatal("ACK refunded spent bucket credit or left a timer")
	}
}

func TestPrefixRepairWaitsForBucketRefill(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	if err := s.SetOpenInfo(x.st, bytes.Repeat([]byte{3}, 256)); err != nil {
		t.Fatal(err)
	}
	x.put(x.source, []byte("request"))
	if x.st.snd.preLen <= 32 || s.mp.prefixAt == 0 {
		t.Fatal("fixture did not send and register a preamble exceeding the remaining quota")
	}
	x.sim.now = s.mp.prefixAt
	s.SetNow(x.sim.now)
	b := &s.mp.prefixBucket
	*b = tokenBucket{rate: uint64(s.cfg.SpreadChunk), burst: int64(s.cfg.SpreadChunk)}
	b.reset(usOf(s.now))
	b.tokens = 32
	s.HandleTimeout(x.sim.now)
	ready := s.prefixTimers(s.now, false)
	if s.mp.prefixPending != 0 || s.mp.prefixBytes != 0 || b.tokens != 32 || ready <= s.now || ready >= s.now.Add(Second) {
		t.Fatal("low credit did not schedule a one-shot refill for the complete repair")
	}
	s.HandleTimeout(ready.Add(-1))
	if s.mp.prefixPending != 0 || s.prefixTimers(s.now, false) != ready {
		t.Fatal("candidate retried before credit was ready")
	}
	x.sim.now = ready
	s.HandleTimeout(ready)
	if s.mp.prefixPending <= 32 || len(x.put(x.alt, nil)) == 0 || s.stats.BytesOpportunistic <= 32 {
		t.Fatal("refilled credit did not repair the complete preamble")
	}
}

func TestPrefixRepairBucketKeepsRefillingOverDays(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	chunk := uint64(s.cfg.SpreadChunk)
	// Exercise reservations and actual-payload accounting without sending
	// a day's worth of carrier traffic.
	spendBurst := func(delay Duration) {
		t.Helper()
		if !s.reservePrefixCredit(chunk) {
			t.Fatal("one full burst should be ready")
		}
		m := mpSend{repair: prefixRepair{hi: chunk}}
		if s.reservePrefixCredit(1) {
			t.Fatal("reserved credit leaked to another stream")
		}
		s.SetNow(s.now.Add(delay))
		s.commitPrefixRepair(&m, 0, chunk)
		if s.mp.prefixPending != 0 || s.reservePrefixCredit(1) {
			t.Fatal("same instant admitted more than one burst")
		}
	}
	start := s.now
	for range (1<<20)/chunk + 2 {
		spendBurst(0)
		s.SetNow(s.now.Add(Second))
	}
	s.SetNow(start.Add(3 * 24 * 60 * 60 * Second))
	spendBurst(Second) // delayed submission must still spend the entire burst at commit
	if s.mp.prefixBucket.tokens != 0 {
		t.Fatal("idle time accumulated more than one burst")
	}
}

func TestPrefixRepairWaitsForInputInsteadOfPolling(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{7}, 256))
	x.alt.mp.quality.rtt = 0
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	if !s.mp.prefixWaiting || s.prefixTimers(s.now, false) != Never {
		t.Fatal("an expired candidate without a target kept polling")
	}
	observePath(s, x.alt, 300*Millisecond, 2e6)
	s.mpAdvanced(x.alt, x.alt.out.lanes[0], &x.alt.out.snd, s.now)
	s.HandleTimeout(x.sim.now)
	if s.mp.prefixBytes == 0 {
		t.Fatal("new ACK evidence did not reconsider the registered candidate")
	}
}

func TestPrefixAndTraceOffAtGeneration(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	s.cfg.Test.NoPrefixRepair, s.cfg.TraceStreams = true, false
	x.put(x.source, []byte("one ordinary STREAM"))
	if s.mp.prefixAt != 0 || x.source.out.lanes[0].prefixCount != 0 || s.mp.gapTrace != nil {
		t.Fatal("disabled instrumentation allocated tracking or armed a timer")
	}
	for e, ok := s.PollEvent(); ok; e, ok = s.PollEvent() {
		if e.Kind == EvStreamPlaced || e.Kind == EvStreamGapStarted || e.Kind == EvStreamGapFilled {
			t.Fatal("core produced a disabled trace event")
		}
	}
}

func TestPrefixRepairResetCancelsPending(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{3}, 256))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	if s.mp.prefixPending == 0 {
		t.Fatal("no reservation to reset")
	}
	s.Reset(x.st, 1)
	if s.mp.prefixBytes != 0 || s.mp.prefixPending != 0 || s.mp.prefixBucket.tokens != s.mp.prefixBucket.burst {
		t.Fatal("reset left a pending speculative reservation")
	}
}

func TestResendFINAcknowledgedAboveGap(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{3}, 256))
	st, m := x.st, x.st.snd.mps
	end := st.snd.sent
	st.snd.finSent, m.finResend = true, true
	m.resend.add(0, end)
	s.acked(&frameDesc{kind: dStream, id: st.id, off: 64, n: end - 64, fin: true})
	p, ok := s.planResend(st)
	if !ok || p.off != 0 || p.n != 64 || p.fin {
		t.Fatalf("plan repeated a higher acknowledged fragment or FIN: %+v", p)
	}
	s.acked(&frameDesc{kind: dStream, id: st.id, off: 0, n: 64})
	if st.snd.state != sendDataRecvd || m.resend.len() != 0 || m.finResend {
		t.Fatal("filling the lower gap failed to finish the already acknowledged FIN")
	}
}

func TestPrefixRepairOriginalCanArriveBeforeCommittedCopy(t *testing.T) {
	x := newPrefixReplay(t)
	s, peer := x.sim.end[0], x.sim.end[1]
	want := bytes.Repeat([]byte{9}, 256)
	if n, err := s.Write(x.st, want, true); err != nil || n != len(want) {
		t.Fatalf("write plus FIN: %d, %v", n, err)
	}
	original := x.put(x.source, nil)
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	copyBatch := x.put(x.alt, nil)
	if len(copyBatch) == 0 || s.stats.BytesOpportunistic == 0 {
		t.Fatal("no committed copy")
	}
	x.arrive(1, x.source.id, original, x.sim.now.Add(Millisecond))
	st, ok := peer.AcceptStream(true)
	if !ok {
		t.Fatal("original did not open the stream")
	}
	got := make([]byte, len(want))
	n, end, err := peer.Read(st, got)
	if err != nil || n != len(want) || !end || !bytes.Equal(got, want) {
		t.Fatalf("original data and FIN: %d, %v, %v", n, end, err)
	}
	x.arrive(1, x.alt.id, copyBatch, x.sim.now.Add(Millisecond))
	n, end, err = peer.Read(st, got)
	if err != nil || n != 0 || !end {
		t.Fatalf("late copy changed the completed stream: %d, %v, %v", n, end, err)
	}
}

func TestPrefixRepairClosingDoesNotArmTimer(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{3}, 256))
	deadline := s.mp.prefixAt
	s.Close(0, nil)
	s.SetNow(deadline)
	s.wakePrefix()
	if s.prefixTimers(deadline, true) != Never || s.mp.prefixBucket.rate != 0 || s.mp.prefixPending != 0 {
		t.Fatal("closing session created an anticipatory repair")
	}
}

func TestPrefixRepairInvalidTargetDoesNotBlockNewData(t *testing.T) {
	for _, assembly := range []bool{false, true} {
		name := "scheduler"
		if assembly {
			name = "assembly"
		}
		t.Run(name, func(t *testing.T) {
			x := newPrefixReplay(t)
			s := x.sim.end[0]
			x.put(x.source, bytes.Repeat([]byte{3}, 256))
			x.sim.now = s.mp.prefixAt
			s.HandleTimeout(x.sim.now)
			if s.mp.prefixPending == 0 {
				t.Fatal("fixture did not reserve the original")
			}

			for range 3 {
				observePath(s, x.alt, 2*Second, 2e6)
			}
			before := x.st.snd.sent
			m := x.st.snd.mps
			if assembly {
				var d decision
				s.newDecision(&d, x.alt)
				if s.sendChunk(&recWriter{}, x.st, &d) != chunkSkip || m.resend.len() != 0 {
					t.Fatal("encoder did not cancel its stale speculative target")
				}
			}
			const newBytes = 4096
			x.put(x.source, bytes.Repeat([]byte{7}, newBytes))
			if x.st.snd.sent != before+newBytes || s.mp.prefixPending != 0 || m.resend.len() != 0 {
				t.Fatalf("stale repair blocked new data: sent=%d (was %d), pending=%d, resend=%d",
					x.st.snd.sent, before, s.mp.prefixPending, m.resend.len())
			}

			other, err := s.OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			x.st = other
			x.put(x.source, bytes.Repeat([]byte{1}, 256))
			*x.alt.mp.quality = pathQuality{}
			observePath(s, x.alt, 300*Millisecond, 2e6)
			_, original, _ := s.prefixHead(other, 0, nil)
			if original == nil {
				t.Fatal("second stream did not register its original")
			}
			x.sim.now = original.confirm
			s.HandleTimeout(x.sim.now)
			if other.snd.mps.repair.source != x.source || s.mp.prefixPending == 0 {
				t.Fatal("a different stream could not reserve the released credit")
			}
		})
	}
}

func TestPrefixRepairChecksConstantEntriesPerACK(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	st := x.st
	x.put(x.source, bytes.Repeat([]byte{7}, 256))
	ol := x.source.out.lanes[0]
	expired := s.mp.prefixAt

	const count, size = 1000, 16
	if n, err := s.Write(st, make([]byte, (count-1)*size), false); n != (count-1)*size || err != nil {
		t.Fatalf("could not stage %d retained records: %v", count, err)
	}
	// Model one stream spread over many handed-off records, so the fixture
	// controls frame size and ACK progression independently of coalescing.
	for i := 1; i < count; i++ {
		prev := ol.log[len(ol.log)-1]
		off := st.snd.sent
		r := retained{
			end: prev.end + size, confirm: expired, serial: prev.serial + 1,
			d: frameDesc{kind: dStream, id: st.id, off: off, n: size, fresh: true},
		}
		st.snd.sent += size
		s.mp.retain(size)
		ol.prefixCount++
		ol.log = append(ol.log, r)
		s.trackPrefix(st, x.source, ol, &r)
	}
	ol.pos = ol.log[len(ol.log)-1].end
	// Keep the known prefix blocked on a route so every ACK can cause a
	// retry, including ACKs unrelated to the missing prefix.
	x.alt.mp.quality.rtt = 0
	s.SetNow(expired)
	s.prefixTimersExamined(expired, true, nil) // discover the contiguous span once
	for i := range count {
		if len(ol.log) == 0 {
			t.Fatalf("retention log emptied at ACK %d", i)
		}
		pos := ol.log[0].end
		ol.acked = pos
		s.ackLog(ol, pos)
		s.mpLaneAdvanced(x.source, ol)
		examined := 0
		s.prefixTimersExamined(expired, true, &examined)
		if examined > 3 {
			t.Fatalf("ACK %d examined %d retained originals, want at most 3", i, examined)
		}
	}
	if ol.prefixCount != 0 || len(s.mp.prefixStreams) != 0 {
		t.Fatal("draining the original log left indexed candidates")
	}
}

func TestPrefixRepairLaneCountTracksOriginalsAndNativeLoss(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	ol := x.source.out.lanes[0]
	x.put(x.source, bytes.Repeat([]byte{3}, 256))
	if ol.prefixCount == 0 {
		t.Fatal("handoff did not register original candidates on the lane")
	}
	x.ackOriginal()
	if ol.prefixCount != 0 {
		t.Fatal("acknowledgement did not retire the original lane count")
	}
	x.put(x.source, bytes.Repeat([]byte{4}, 256))
	if ol.prefixCount == 0 {
		t.Fatal("second handoff did not register an original")
	}
	s.reinjectPipe(x.source)
	if ol.prefixCount != 0 {
		t.Fatal("native reinjection did not retire the lane's speculative originals")
	}
	x.ackOriginal()
	if ol.prefixCount != 0 {
		t.Fatal("late ACK retired the same original twice")
	}
}

func TestPrefixRepairKeepsReservationWhileTargetWrites(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{1}, 256))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	m := x.st.snd.mps
	repair := m.repair
	pending := s.mp.prefixPending
	if pending == 0 {
		t.Fatal("fixture did not queue a repair")
	}

	busy := GetBatch()
	defer PutBatch(busy)
	busy.Pipe = x.alt.id
	x.alt.out.lanes[0].writing = true
	before := x.st.snd.sent
	x.put(x.source, bytes.Repeat([]byte{2}, 4096))
	if x.st.snd.sent != before+4096 || m.repair != repair || s.mp.prefixPending != pending ||
		!m.resend.contains(repair.sent, repair.hi) {
		t.Fatal("temporary target unavailability blocked new data or canceled the repair")
	}

	s.WriteDone(busy, nil, x.sim.now)
	if len(x.put(x.alt, nil)) == 0 || s.mp.prefixPending != 0 || m.resend.len() != 0 ||
		s.stats.BytesOpportunistic != pending {
		t.Fatal("available target did not send the preserved repair")
	}
}

func TestPrefixRepairCanRetrySameRangeAfterLostEligibility(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{1}, 256))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)
	m := x.st.snd.mps
	old := m.repair
	if old.source == nil {
		t.Fatal("fixture did not queue a repair")
	}
	x.alt.mp.quality.rtt = 0
	x.put(x.source, bytes.Repeat([]byte{2}, 32))
	if m.repair.source != nil || s.mp.prefixPending != 0 || m.oppSent.covered(old.sent, old.hi) != 0 {
		t.Fatal("lost eligibility did not release the reservation and opportunity")
	}
	retry := s.mp.prefixAt
	if retry <= x.sim.now {
		t.Fatal("cancellation did not arm a later deadline")
	}
	observePath(s, x.alt, 300*Millisecond, 2e6)
	s.HandleTimeout(retry - 1)
	if s.mp.prefixPending != 0 {
		t.Fatal("repair retried before the next deadline")
	}
	x.sim.now = retry
	s.HandleTimeout(retry)
	if m.repair.source != x.source || m.repair.lo != old.lo || m.repair.hi < old.hi ||
		len(x.put(x.alt, nil)) == 0 {
		t.Fatal("next deadline did not repair the same missing range")
	}
}

func TestPrefixRepairDrainingSourceWaitsForPipeState(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{1}, 256))
	expired := s.mp.prefixAt
	x.source.mp.out = OutDraining
	s.SetNow(expired)
	s.prefixTimers(expired, true)
	if s.mp.prefixWaiting || !s.mp.prefixStateWaiting {
		t.Fatal("draining source did not park specifically on pipe state")
	}

	b := GetBatch()
	defer PutBatch(b)
	b.Pipe = x.source.id
	for i := range 8 {
		now := expired.Add(Duration(i+1) * Millisecond)
		s.WriteDone(b, nil, now)
		var examined int
		if s.prefixTimersExamined(now, true, &examined) != Never || examined != 0 {
			t.Fatalf("write completion %d scanned %d dormant candidates", i, examined)
		}
	}
	x.source.mp.out = OutActive
	s.emitPipeState(x.source)
	s.prefixTimers(s.now, true)
	if s.mp.prefixPending == 0 {
		t.Fatal("pipe state transition did not wake the dormant repair")
	}
}

func TestPrefixRepairNativeLossAfterPartialSubmission(t *testing.T) {
	x := newPrefixReplay(t)
	s := x.sim.end[0]
	x.put(x.source, bytes.Repeat([]byte{1}, 512))
	x.sim.now = s.mp.prefixAt
	s.HandleTimeout(x.sim.now)

	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(x.alt.id, 0, b, 128, x.sim.now) {
		t.Fatal("no partial repair batch")
	}
	s.WriteDone(b, nil, x.sim.now)
	m := x.st.snd.mps
	r := m.repair
	if r.sent <= r.lo || r.sent >= r.hi {
		t.Fatalf("expected a partially submitted repair, got %+v", r)
	}
	s.makeSuspect(x.source)
	if s.mp.prefixPending != 0 || s.mp.prefixBytes != r.sent-r.lo ||
		m.repair.hi != r.sent || m.resend.covered(r.lo, r.sent) != 0 ||
		!m.resend.contains(r.sent, r.hi) {
		t.Fatalf("loss did not preserve the in-flight copy and transfer the tail: %+v", m.repair)
	}

	stats := s.SessionStats()
	tokens := s.mp.prefixBucket.tokens
	x.put(x.alt, nil)
	got := s.SessionStats()
	if got.BytesReinjected-stats.BytesReinjected != r.hi-r.sent ||
		got.BytesOpportunistic != stats.BytesOpportunistic || s.mp.prefixBucket.tokens != tokens {
		t.Fatalf("native retransmission used the repair budget: before %+v after %+v", stats, got)
	}
	if err := s.mpAckEntry(wire.AckEntry{PipeID: x.alt.id, Position: x.alt.out.lanes[0].pos}, x.sim.now); err != nil {
		t.Fatal(err)
	}
	if s.mp.prefixBytes != 0 {
		t.Fatal("acknowledging the submitted copy did not release its reservation")
	}
}
