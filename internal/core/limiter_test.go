package core

import "testing"

// pathSim is a pipe of rate bytes/s and round trip rtt paced by the
// limiter: each millisecond the sender sends what the limiter allows (it
// always has data), the path delivers at its rate, and the peer
// acknowledges what arrived one way back later.
type pathSim struct {
	lim      *limiter
	est      rateEst
	rtt      rttEst
	now      Time
	rate     float64
	delay    Duration // round trip
	sent     uint64
	acked    uint64
	flight   []simBatch
	busy     Time // the path has delivered everything sent by then
	arrivals []simArrival
	roundEnd uint64
	lastAck  Time
}

type simBatch struct {
	at        Time
	end       uint64
	delivered uint64
	ackAt     Time
}

type simArrival struct {
	at  Time
	pos uint64
}

func newPathSim(rate float64, rtt Duration) *pathSim {
	p := &pathSim{rate: rate, delay: rtt, now: Time(Second)}
	p.lim = newLimiter(p.now)
	p.rtt = rttEst{smoothed: 333 * Millisecond, rttvar: 166 * Millisecond}
	p.lastAck = p.now
	return p
}

func simParams() limParams {
	return limParams{budget: 100 * Microsecond, threshold: 64 << 10, ackDelay: 25 * Millisecond, chunk: 16 << 10}
}

func (p *pathSim) step() {
	pp := simParams()
	p.now = p.now.Add(Millisecond)
	now := p.now
	var ack uint64
	for len(p.arrivals) > 0 && p.arrivals[0].at.Add(p.delay/2) <= now {
		ack = p.arrivals[0].pos
		p.arrivals = p.arrivals[1:]
	}
	if ack > p.acked {
		i := 0
		for p.flight[i].end < ack {
			i++
		}
		sentAt := p.flight[i].at
		r := now.Sub(sentAt)
		adj := p.rtt.update(r, 0, 25*Millisecond, now)
		j := 0
		for j+1 < len(p.flight) && p.flight[j+1].end <= ack {
			j++
		}
		last := p.flight[j]
		iv := maxD(now.Sub(last.ackAt), Millisecond)
		rate := float64(ack-last.delivered) * 1e9 / float64(iv)
		ev := limAck{rtt: adj, rttSentAt: sentAt, acked: ack, sent: p.sent}
		p.est.blockEpoch++ // the sender always has data: every range is a bottleneck sample
		ev.rate = p.est.sample(rate, false, 0, now)
		p.lastAck = now
		p.acked = ack
		if ack >= p.roundEnd {
			p.est.round++
			p.roundEnd = p.sent
			ev.roundEnd = true
		}
		k := 0
		for k < len(p.flight) && p.flight[k].end <= ack {
			k++
		}
		p.flight = p.flight[k:]
		p.lim.onAck(now, &ev, &p.est, p.rtt.smoothed, &pp)
	}
	if p.lim.held && p.lim.release <= now {
		p.lim.refresh(now, p.sent-p.acked, p.est.value(), p.rtt.smoothed, &pp)
	}
	room := p.lim.room(now, p.sent-p.acked, p.est.value(), p.rtt.smoothed, &pp)
	if room > 0 && !p.lim.held {
		n := min64(room, 256<<10)
		p.sent += n
		p.flight = append(p.flight, simBatch{now, p.sent, p.acked, p.lastAck})
		start := maxT(p.busy, now)
		done := start.Add(timeAt(n, p.rate))
		p.busy = done
		p.arrivals = append(p.arrivals, simArrival{done.Add(p.delay / 2), p.sent})
		p.lim.settle(now, n, p.est.value(), p.rtt.smoothed, &pp)
		p.lim.refresh(now, p.sent-p.acked, p.est.value(), p.rtt.smoothed, &pp)
	}
}

func (p *pathSim) queue() Duration { return max(p.busy.Sub(p.now), 0) }

func (p *pathSim) run(d Duration) uint64 {
	start := p.acked
	for range int(d / Millisecond) {
		p.step()
	}
	return p.acked - start
}

func TestLimiterStartupEndsAndQueueStaysSmall(t *testing.T) {
	// 100 Mbit/s x 10 ms behind a carrier that hides any buffer.
	p := newPathSim(12.5e6, 10*Millisecond)
	p.run(2 * Second)
	if p.lim.mode != limCruise && p.lim.mode != limProbe {
		t.Fatalf("mode %d after 2 s", p.lim.mode)
	}
	got := float64(p.run(2*Second)) * 8 / 2 / 1e6
	if got < 90 {
		t.Fatalf("%.1f Mbit/s", got)
	}
	var worst Duration
	for range 1000 {
		p.step()
		worst = max(worst, p.queue())
	}
	if worst >= 25*Millisecond {
		t.Fatalf("queue %v", worst)
	}
}

func TestLimiterRateDropBoundedWithinProbe(t *testing.T) {
	// 100 Mbit/s x 100 ms, then 20 Mbit/s.
	p := newPathSim(12.5e6, 100*Millisecond)
	p.run(15 * Second)
	p.rate = 2.5e6
	p.run(2500 * Millisecond)
	var worst Duration
	for range 2500 {
		p.step()
		worst = max(worst, p.queue())
	}
	if worst >= 300*Millisecond {
		t.Fatalf("queue %v", worst)
	}
	if got := float64(p.run(5*Second)) * 8 / 5 / 1e6; got < 17 {
		t.Fatalf("%.1f Mbit/s", got)
	}
}

func TestLimiterDelayRiseIsLearnt(t *testing.T) {
	// 10 Mbit/s whose round trip rises from 10 to 300 ms.
	p := newPathSim(1.25e6, 10*Millisecond)
	p.run(5 * Second)
	p.delay = 300 * Millisecond
	p.run(5 * Second)
	if !p.lim.baseHas || p.lim.baseD < 250*Millisecond {
		t.Fatalf("base %v", p.lim.baseD)
	}
	if p.est.lo != 0 {
		t.Fatalf("rate bound %v", p.est.lo)
	}
	if got := float64(p.run(5*Second)) * 8 / 5 / 1e6; got < 8.5 {
		t.Fatalf("%.1f Mbit/s", got)
	}
}

func TestLimiterPacingQuantum(t *testing.T) {
	pp := simParams()
	now := Time(Second)
	var est rateEst
	est.blockEpoch = 1
	est.sample(1.25e9, false, 0, now) // 10 Gbit/s
	l := newLimiter(now)
	l.mode, l.phaseSince = limCruise, now
	l.baseD, l.baseHas = 10*Millisecond, true
	at := now.Add(30 * Millisecond)
	room := l.room(at, 0, est.value(), 10*Millisecond, &pp)
	if room < 1_200_000 || room > 1_300_000 {
		t.Fatalf("room %d", room)
	}
	l.settle(at, room, est.value(), 10*Millisecond, &pp)
	l.refresh(at, 0, est.value(), 10*Millisecond, &pp)
	if !l.held || l.release <= at || l.release.Sub(at) >= limGranularity {
		t.Fatalf("held %v release %v", l.held, l.release.Sub(at))
	}
}
