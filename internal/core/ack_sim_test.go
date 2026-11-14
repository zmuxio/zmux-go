package core

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// A virtual-clock simulation of two sessions over one carrier, the
// regression base of the acknowledgement rules and the carrier queue
// limiter. The reports run with ZMUX_ACK_SIM=1: TestAckSimMatrix sweeps
// link rate x RTT x traffic x carrier kind over ZMUX_ACK_SEEDS seeds (3)
// with delay jitter; ZMUX_ACK_ONLY limits it to one traffic mode,
// ZMUX_ACK_CASE="mode MB/s ms seed queueBound" runs one case, ZMUX_ACK_TRACE
// logs the sender's limiter every 50 ms and ZMUX_ACK_THR sets the
// receiver's zmux_ack_threshold.

// simLink is one direction of a carrier on a virtual clock: a serialising
// bottleneck of rate bytes per second, then a fixed delay.
type simLink struct {
	q     []simMsg
	rate  float64
	delay Duration
	free  Time // when the bottleneck finishes what it holds
	// jitter: each message's delay gains a uniform extra in [0, jitter),
	// order kept.
	jitter Duration
	rnd    uint64
	last   Time
	bytes  uint64
}

func (l *simLink) extra() Duration {
	if l.jitter <= 0 {
		return 0
	}
	l.rnd ^= l.rnd << 13
	l.rnd ^= l.rnd >> 7
	l.rnd ^= l.rnd << 17
	return Duration(l.rnd % uint64(l.jitter))
}

type simMsg struct {
	data []byte
	at   Time
}

func (l *simLink) send(now Time, b []byte) {
	l.bytes += uint64(len(b))
	start := max(l.free, now)
	l.free = start.Add(Duration(float64(len(b)) * 1e9 / l.rate))
	at := max(l.free.Add(l.delay+l.extra()), l.last)
	l.last = at
	l.q = append(l.q, simMsg{at: at, data: b})
}

// queued is the bytes waiting at the bottleneck.
func (l *simLink) queued(now Time) float64 {
	return max(float64(l.free.Sub(now)), 0) * l.rate / 1e9
}

// simEnd is one endpoint with its outbound link.
type simEnd struct {
	s    *Session
	out  *simLink
	info CarrierInfo
	acks int // PIPE_ACK frames sent
	recs int
}

// simCfg describes a carrier scenario.
type simCfg struct {
	framing    Framing
	queueBound bool    // the carrier bounds its queue (no limiter)
	maxRecord  int     // carrier record bound; 0: none
	rate       float64 // bottleneck bytes/s
	rtt        Duration
	dur        Duration
	// Application-limited sender: appBytes every appEvery; 0: as fast as
	// the session allows.
	appBytes int
	appEvery Duration
	seed     uint64   // jitter seed; 0: no jitter
	jitter   Duration // link delay jitter with seed
	start    Duration // when the application starts writing
}

type simResult struct {
	srtt, minRTT Duration
	rate         float64 // sender's delivery-rate estimate
	goodput      float64 // bytes/s read by the receiver after warm-up
	acksPerSec   float64
	recsPerSec   float64
	swin, cwin   uint64 // receiver's stream and session windows
	loop         Duration
}

func (r simResult) String() string {
	return fmt.Sprintf("srtt %v min_rtt %v R_p %.1f MB/s goodput %.1f MB/s PIPE_ACK %.0f/s records %.0f/s",
		time.Duration(r.srtt), time.Duration(r.minRTT), r.rate/1e6, r.goodput/1e6, r.acksPerSec, r.recsPerSec) + fmt.Sprintf(" win %dK/%dK loop %v", r.swin>>10, r.cwin>>10, time.Duration(r.loop))
}

func countAcks(t testing.TB, rec []byte) int {
	n := 0
	for len(rec) > 0 {
		var f wire.Frame
		l, err := wire.DecodeFrame(rec, &f)
		if err != nil {
			t.Fatal(err)
		}
		if f.Kind == wire.KindPipeAck {
			n++
		}
		rec = rec[l:]
	}
	return n
}

// flush writes everything the endpoint wants to write now.
func (e *simEnd) flush(t testing.TB, now Time, sc simCfg) {
	for range 1000 {
		if sc.queueBound && e.out.queued(now) >= 256<<10 {
			return
		}
		if !e.s.WantsWrite(0, 0) {
			return
		}
		b := GetBatch()
		if !e.s.PollTransmit(0, 0, b, DefaultBatch, now) {
			PutBatch(b)
			return
		}
		if sc.framing == FramingRecords {
			for _, m := range b.Messages() {
				e.acks += countAcks(t, m)
				e.recs++
				e.out.send(now, append([]byte(nil), m...))
			}
		} else {
			all := b.Flat()
			for r := all; len(r) > 0; {
				size, n := varint.ConsumeVarint(r)
				e.acks += countAcks(t, r[n:n+int(size)])
				e.recs++
				r = r[n+int(size):]
			}
			e.out.send(now, all)
		}
		e.s.WriteDone(b, nil, now)
		PutBatch(b)
	}
	t.Fatal("writer spins")
}

func (e *simEnd) deliver(now Time, in *simLink, sc simCfg) {
	for len(in.q) > 0 && in.q[0].at <= now {
		m := in.q[0].data
		in.q = in.q[1:]
		if sc.framing == FramingRecords {
			e.s.ReceiveMessage(0, 0, &Buf{B: m}, m, now)
		} else {
			e.s.Receive(0, 0, &Buf{B: m}, m, now)
		}
	}
}

// runSim sends one stream as fast as the session allows from client to
// server over sc and reports the client's estimates.
func runSim(t testing.TB, sc simCfg) simResult {
	t.Helper()
	info := CarrierInfo{Framing: sc.framing, QueueBound: sc.queueBound, MaxRecord: sc.maxRecord}
	t0 := Time(Second)
	ccfg := DefaultConfig()
	scfg := DefaultConfig()
	scfg.Role, scfg.TransportRole = RoleResponder, RoleResponder
	if v := os.Getenv("ZMUX_ACK_THR"); v != "" {
		var thr uint64
		_, _ = fmt.Sscan(v, &thr)
		scfg.Local.Has |= wire.HasAckThreshold
		scfg.Local.AckThreshold = thr
	}
	up := &simLink{rate: sc.rate, delay: sc.rtt / 2}
	down := &simLink{rate: sc.rate, delay: sc.rtt / 2}
	if sc.seed != 0 {
		up.jitter, up.rnd = sc.jitter, sc.seed*0x9e3779b97f4a7c15|1
		down.jitter, down.rnd = sc.jitter, sc.seed*0xc2b2ae3d27d4eb4f|1
	}
	c := &simEnd{s: New(ccfg, t0), out: up, info: info}
	sv := &simEnd{s: New(scfg, t0), out: down, info: info}
	c.s.AttachBootstrap(info, info, nil)
	sv.s.AttachBootstrap(info, info, nil)
	chunk := make([]byte, 256<<10)
	rbuf := make([]byte, 1<<20)
	var st, rs *Stream
	var read, readWarm uint64
	var acksWarm, recsWarm int
	warm := t0.Add(sc.dur / 4)
	end := t0.Add(sc.dur)
	warmed := false
	now := t0
	nextApp := t0.Add(sc.start)
	nextTrace := t0
	for now < end {
		c.s.SetNow(now)
		sv.s.SetNow(now)
		if c.s.NextTimeout() <= now {
			c.s.HandleTimeout(now)
		}
		if sv.s.NextTimeout() <= now {
			sv.s.HandleTimeout(now)
		}
		c.deliver(now, down, sc)
		sv.deliver(now, up, sc)
		if c.s.Err() != nil || sv.s.Err() != nil {
			t.Fatalf("session failed: %v %v", c.s.Err(), sv.s.Err())
		}
		if st == nil && c.s.Established() && now >= t0.Add(sc.start) {
			var err error
			if st, err = c.s.OpenStream(true); err != nil {
				t.Fatal(err)
			}
		}
		if st != nil && sc.appBytes > 0 {
			for nextApp <= now {
				if _, err := c.s.Write(st, chunk[:sc.appBytes], false); err != nil {
					t.Fatal(err)
				}
				nextApp = nextApp.Add(sc.appEvery)
			}
		} else if st != nil {
			for {
				n, err := c.s.Write(st, chunk, false)
				if err != nil {
					t.Fatal(err)
				}
				if n < len(chunk) {
					break
				}
			}
		}
		if rs == nil {
			rs, _ = sv.s.AcceptStream(true)
		}
		if rs != nil {
			for {
				n, _, err := sv.s.Read(rs, rbuf)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				read += uint64(n)
			}
		}
		c.flush(t, now, sc)
		sv.flush(t, now, sc)
		if !warmed && now >= warm {
			warmed = true
			readWarm, acksWarm, recsWarm = read, sv.acks, c.recs
		}
		if simTrace && now >= nextTrace {
			nextTrace = nextTrace.Add(50 * Millisecond)
			p := c.s.pipes[0]
			var credit uint64
			if st != nil {
				credit = st.snd.maxData - min64(st.snd.maxData, st.snd.sent)
			}
			l := p.out.snd.lim
			if l != nil {
				pp := c.s.limParams()
				t.Logf("base=%v cap=%d tok=%.0f min=%v t=%v R=%.1f in=%d mode=%d held=%v srtt=%v credit=%d conn=%d read=%d acks=%d sent=%d room=%d ww=%v rel=%v", time.Duration(l.baseD), l.cap(p.out.snd.re.value(), p.rtt().smoothed, &pp), l.tokens, time.Duration(p.rtt().min),
					time.Duration(now.Sub(t0)), p.out.snd.re.value()/1e6, c.s.limInFlight(p), l.mode, l.held,
					time.Duration(p.rtt().smoothed), credit, c.s.conn.sendMax-c.s.conn.sent, read, sv.acks, up.bytes, c.s.paceRoom(p), c.s.WantsWrite(0, 0), time.Duration(l.release-now))
			}
		}
		next := now.Add(50 * Microsecond)
		if sc.appBytes > 0 {
			next = minT(next, nextApp)
		}
		next = minT(next, c.s.NextTimeout())
		next = minT(next, sv.s.NextTimeout())
		if len(up.q) > 0 {
			next = minT(next, up.q[0].at)
		}
		if len(down.q) > 0 {
			next = minT(next, down.q[0].at)
		}
		now = max(next, now+1)
	}
	p := c.s.pipes[0]
	r := p.rtt()
	secs := float64(sc.dur-sc.dur/4) / 1e9
	res := simResult{srtt: r.smoothed, minRTT: r.min, rate: p.out.snd.re.value(),
		goodput: float64(read-readWarm) / secs, acksPerSec: float64(sv.acks-acksWarm) / secs,
		recsPerSec: float64(c.recs-recsWarm) / secs, cwin: sv.s.conn.win, loop: sv.s.tn.loopMin.v}
	if rs != nil {
		res.swin = rs.rcv.window
	}
	return res
}

var simTrace = os.Getenv("ZMUX_ACK_TRACE") != ""

var simScenarios = []struct {
	name string
	sc   simCfg
}{
	// A message carrier paced by the limiter, a record just under the
	// threshold per message.
	{"messages-65525", simCfg{framing: FramingRecords, maxRecord: 65525, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second}},
	{"messages-16k", simCfg{framing: FramingRecords, maxRecord: 16384, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second}},
	{"messages-65525-rtt20", simCfg{framing: FramingRecords, maxRecord: 65525, rate: 50e6, rtt: 20 * Millisecond, dur: 3 * Second}},
	// Application-limited senders on a message carrier: a 60 KiB message
	// every 30 ms (a media stream), 4 KiB every 10 ms (interactive).
	{"app-60k/30ms", simCfg{framing: FramingRecords, maxRecord: 65525, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second,
		appBytes: 60 << 10, appEvery: 30 * Millisecond}},
	{"app-4k/10ms", simCfg{framing: FramingRecords, maxRecord: 65525, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second,
		appBytes: 4 << 10, appEvery: 10 * Millisecond}},
	{"app-4k/40ms", simCfg{framing: FramingRecords, maxRecord: 65525, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second,
		appBytes: 4 << 10, appEvery: 40 * Millisecond}},
	{"trace", simCfg{framing: FramingBytes, rate: 50e6, rtt: 20 * Millisecond, dur: 2 * Second}},
	// A byte stream paced by the limiter.
	{"bytes-paced", simCfg{framing: FramingBytes, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second}},
	// A TCP-like byte stream bounding its own queue.
	{"bytes-queue-bound", simCfg{framing: FramingBytes, queueBound: true, rate: 200e6, rtt: 2 * Millisecond, dur: 3 * Second}},
}

// TestAckSimReport prints the scenarios' estimates (ZMUX_ACK_SIM=1).
func TestAckSimReport(t *testing.T) {
	if os.Getenv("ZMUX_ACK_SIM") == "" {
		t.Skip("set ZMUX_ACK_SIM=1")
	}
	for _, c := range simScenarios {
		if o := os.Getenv("ZMUX_ACK_ONLY"); o != "" && o != c.name {
			continue
		}
		t.Logf("%-22s %v", c.name, runSim(t, c.sc))
	}
}

// simModes are the traffic patterns of the matrix.
var simModes = []struct {
	name string
	set  func(*simCfg)
}{
	{"bulk", func(c *simCfg) { c.framing = FramingBytes }},
	{"app-4k/10ms", func(c *simCfg) { c.framing = FramingBytes; c.appBytes, c.appEvery = 4<<10, 10*Millisecond }},
	{"app-60k/30ms", func(c *simCfg) {
		c.framing, c.maxRecord = FramingRecords, 65525
		c.appBytes, c.appEvery = 60<<10, 30*Millisecond
	}},
	{"msg-64k", func(c *simCfg) { c.framing, c.maxRecord = FramingRecords, 65525 }},
}

// TestAckSimMatrix sweeps link rate x RTT x traffic x carrier kind over
// several seeds (ZMUX_ACK_SIM=1; ZMUX_ACK_SEEDS, default 3).
func TestAckSimMatrix(t *testing.T) {
	if os.Getenv("ZMUX_ACK_SIM") == "" {
		t.Skip("set ZMUX_ACK_SIM=1")
	}
	seeds := 3
	if v := os.Getenv("ZMUX_ACK_SEEDS"); v != "" {
		_, _ = fmt.Sscan(v, &seeds)
	}
	for _, qb := range []bool{false, true} {
		for _, m := range simModes {
			if o := os.Getenv("ZMUX_ACK_ONLY"); o != "" && o != m.name {
				continue
			}
			for _, rate := range []float64{5e6, 20e6, 50e6, 200e6} {
				for _, rtt := range []Duration{1, 10, 20, 80} {
					var line string
					minG, maxG := 1e18, 0.0
					for seed := 1; seed <= seeds; seed++ {
						sc := simCfg{queueBound: qb, rate: rate, rtt: rtt * Millisecond, dur: 4 * Second,
							seed: uint64(seed), jitter: Duration(seed-1) * 150 * Microsecond,
							start: Duration(seed-1) * 7 * Millisecond}
						m.set(&sc)
						r := runSim(t, sc)
						minG, maxG = min(minG, r.goodput), max(maxG, r.goodput)
						line += fmt.Sprintf(" | srtt %5.1f min %5.1f R %6.1f G %6.1f ack %4.0f",
							float64(r.srtt)/1e6, float64(r.minRTT)/1e6, r.rate/1e6, r.goodput/1e6, r.acksPerSec)
					}
					t.Logf("qb=%-5v %-12s %3.0fMB/s %2dms G[%6.1f..%6.1f]%s", qb, m.name, rate/1e6, rtt, minG/1e6, maxG/1e6, line)
				}
			}
		}
	}
}

// TestAckSimOne runs one case (ZMUX_ACK_CASE="mode rateMB rttms seed").
func TestAckSimOne(t *testing.T) {
	v := os.Getenv("ZMUX_ACK_CASE")
	if v == "" {
		t.Skip("set ZMUX_ACK_CASE")
	}
	var mode string
	var rate float64
	var rtt, seed int
	var qb bool
	_, _ = fmt.Sscan(v, &mode, &rate, &rtt, &seed, &qb)
	sc := simCfg{queueBound: qb, rate: rate * 1e6, rtt: Duration(rtt) * Millisecond, dur: 4 * Second,
		seed: uint64(seed), jitter: Duration(seed-1) * 150 * Microsecond, start: Duration(seed-1) * 7 * Millisecond}
	for _, m := range simModes {
		if m.name == mode {
			m.set(&sc)
		}
	}
	t.Logf("%v", runSim(t, sc))
}

// A carrier paced by the limiter carries a bulk transfer at the link rate
// on a long path: the pacing deadline must not move with the events that
// read it (pacing then degenerates to bursts released by acknowledgements,
// 3 to 7 of 50 MB/s).
func TestAckSimPacedBulkFillsLink(t *testing.T) {
	for _, rtt := range []Duration{10, 80} {
		sc := simCfg{framing: FramingBytes, rate: 20e6, rtt: rtt * Millisecond, dur: 3 * Second}
		if r := runSim(t, sc); r.goodput < 0.95*sc.rate {
			t.Fatalf("rtt %d ms: %v", rtt, r)
		}
	}
}

// A sparse record below the threshold is acknowledged at once (MULTIPATH.md
// Section 6.3 rule 2a): the smoothed and the minimum RTT are the path's, not
// the path's plus zmux_max_ack_delay.
func TestAckSimSparseRecordRTT(t *testing.T) {
	sc := simCfg{framing: FramingRecords, maxRecord: 65525, rate: 50e6, rtt: 10 * Millisecond, dur: 2 * Second,
		appBytes: 4 << 10, appEvery: 40 * Millisecond}
	r := runSim(t, sc)
	if r.minRTT > 11*Millisecond || r.srtt > 12*Millisecond {
		t.Fatalf("%v", r)
	}
}
