package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// rateFeeder acknowledges ranges of n bytes on pipe 0, each sent at sent
// and acknowledged at now, delivered over iv: a delivery-rate sample of
// n / iv, in an application-limited bubble.
type rateFeeder struct {
	a   *ackState
	p   *Pipe
	end uint64
}

func (f *rateFeeder) ack(sent, now Time, n uint64, iv Duration) {
	ol := f.p.out.lanes[0]
	ps := &f.p.out.snd
	start := f.end
	f.end += n
	ol.pos, ol.acked, ps.so = f.end, f.end, f.end
	ol.segs = []sendSeg{{pos: start, end: f.end, so: start}}
	ps.samples = []sample{{end: f.end, sent: sent, firstSentAt: sent, delivered: ps.delivered,
		deliveredAt: now.Add(-iv), bubble: true}}
	ps.deliveredAt = now.Add(-iv)
	f.a.advanced(f.p, ol, wire.AckEntry{Position: f.end}, now)
}

// On a deep-buffered carrier the pipe stays writable and its ranges are
// application-limited, below the initial 1 MB/s: only the queue-proven rule
// lets R_p converge to the path rate.
func TestRateConvergesOnDeepBuffer(t *testing.T) {
	s := capacitySession()
	f := &rateFeeder{a: newAckState(s), p: s.pipes[0]}
	e := &f.p.out.snd.re
	f.ack(0, Time(20*Millisecond), 3000, 100*Millisecond) // min_rtt 20 ms, no queue
	if e.measured || e.value() != initialRate {
		t.Fatalf("an unqueued application-limited sample was taken: R_p %v", e.value())
	}
	at := Time(Second)
	for range 20 {
		f.ack(at, at.Add(500*Millisecond), 3000, 100*Millisecond) // queued 480 ms
		at = at.Add(100 * Millisecond)
	}
	if !e.measured || e.value() != 3e4 {
		t.Fatalf("R_p %v (measured %v), want the path's 30 kB/s", e.value(), e.measured)
	}
	if r := f.p.rtt(); r.min != 20*Millisecond {
		t.Fatalf("min_rtt %v", r.min)
	}
}

// Bytes sent before the pipe turned suspect give no RTT sample, but a
// queue-proven rate sample: R_p leaves its old maximum once that ages out
// instead of keeping it for want of samples.
func TestRateLeavesOldMaximumAfterSuspect(t *testing.T) {
	s := capacityMP()
	f := &rateFeeder{a: newAckState(s), p: s.pipes[0]}
	e := &f.p.out.snd.re
	f.ack(0, Time(20*Millisecond), 3000, 100*Millisecond)
	measuredRate(f.p, 2e6)
	e.samples[0].at = Time(20 * Millisecond)
	a := f.p.mp
	suspectAt := Time(30 * Second)
	a.noSampleAt, a.noSample = suspectAt, true
	srtt := f.p.rtt().smoothed
	at := suspectAt.Add(-5 * Second)
	for range 10 {
		f.ack(at, suspectAt.Add(Second), 3000, 100*Millisecond) // sent before the suspicion, queued
		at = at.Add(100 * Millisecond)
	}
	if got := f.p.rtt().smoothed; got != srtt {
		t.Fatalf("bytes sent before the suspicion gave an RTT sample: %v, was %v", got, srtt)
	}
	if e.value() != 3e4 {
		t.Fatalf("R_p %v after the suspicion, want 30 kB/s, not the old 2 MB/s", e.value())
	}
	// An unqueued range sent before the suspicion still gives nothing.
	n := len(e.samples)
	f.ack(suspectAt.Add(-Second), suspectAt.Add(-Second+20*Millisecond), 30000, 100*Millisecond)
	if len(e.samples) != n {
		t.Fatal("an unqueued range sent before the suspicion gave a rate sample")
	}
}
