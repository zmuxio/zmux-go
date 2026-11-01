package core

import "testing"

// Q_p is what the core handed to a pipe that its carrier has not taken
// (the batch being written) plus the carrier's own unsent count: the
// scheduler's Unsent and tier saturation's committed bytes read it, and
// QueuedBytes reports the carrier's part alone.
func TestQueuedOnSlowCarrier(t *testing.T) {
	s, p := statsMP()
	ol := p.out.lanes[0]
	if s.queuedOn(p) != 0 || s.committed(p) != 0 {
		t.Fatal("an idle pipe has bytes queued")
	}
	// A batch produced and not yet taken by a slow carrier.
	b := GetBatch()
	defer PutBatch(b)
	b.Pipe, b.pos = p.id, 3000
	ol.handed += b.pos
	ol.writing = true
	s.SetCarrierUnsent(p.id, 0, 500)
	if q := s.queuedOn(p); q != 3500 || s.committed(p) != 3500 {
		t.Fatalf("Q_p %d, committed %v, want 3500", q, s.committed(p))
	}
	// The scheduler sees the queue: the same chunk is due later on p
	// than on the idle pipe 0 of the same rate.
	idle := s.pipes[0]
	measuredRate(p, 1e6)
	measuredRate(idle, 1e6)
	if s.eta(p, 1000) <= s.eta(idle, 1000) {
		t.Fatalf("ETA on the slow pipe %v, on the idle one %v", s.eta(p, 1000), s.eta(idle, 1000))
	}
	var req PickRequest
	s.TakePick(&req)
	var unsent uint64
	for _, pp := range req.Pipes {
		if pp.Pipe == p.id {
			unsent = pp.Unsent
		}
	}
	if unsent != 3500 {
		t.Fatalf("PickPipe.Unsent %d, want 3500", unsent)
	}
	if pi, _ := s.PipeInfo(p.id); pi.QueuedBytes != 500 {
		t.Fatalf("QueuedBytes %d, want the carrier's 500", pi.QueuedBytes)
	}
	s.WriteDone(b, nil, s.now) // the carrier took the batch
	if q := s.queuedOn(p); q != 500 {
		t.Fatalf("Q_p after the write %d, want 500", q)
	}
}

// T_suspect and T_fail include queue_p = min(1 s, unsent_ahead / R_p),
// with unsent_ahead the bytes that waited ahead of the oldest
// unacknowledged ack-eliciting byte when it was handed over: it does not
// grow afterwards, and it moves to the next byte's snapshot when the
// oldest is acknowledged.
func TestUnsentAheadInSuspectTimer(t *testing.T) {
	s, p := statsMP()
	a := p.mp
	measuredRate(p, 1e6) // 1 MB/s
	base := s.tSuspect(a)
	var w recWriter
	w.anyElic = true
	s.mpAfterBatch(&w, p, p.out.lanes[0], 0, aheadSnap{}) // the oldest: nothing ahead
	if a.unsentAhead != 0 || s.tSuspect(a) != base {
		t.Fatalf("nothing ahead: unsent_ahead %d, T_suspect %v (base %v)", a.unsentAhead, s.tSuspect(a), base)
	}
	s.mpAfterBatch(&w, p, p.out.lanes[0], 0, aheadSnap{400_000, 400 * Millisecond}) // a later one: the snapshot stays
	if a.unsentAhead != 0 {
		t.Fatalf("the snapshot grew to %d", a.unsentAhead)
	}
	// The oldest is acknowledged: the next one, with 400 kB ahead of it,
	// is the oldest now.
	ps := &p.out.snd
	ps.samples = []sample{{end: 1, ahead: aheadSnap{400_000, 400 * Millisecond}}}
	s.mpAdvanced(p, p.out.lanes[0], ps, s.now)
	if a.unsentAhead != 400_000 {
		t.Fatalf("unsent_ahead %d, want 400000", a.unsentAhead)
	}
	if got, want := s.tSuspect(a), maxD(base, 2*(s.pto(a)+400*Millisecond)); got != want {
		t.Fatalf("T_suspect %v, want %v", got, want)
	}
	ps.samples = []sample{{end: 2, ahead: aheadSnap{50_000_000, Second}}}
	s.mpAdvanced(p, p.out.lanes[0], ps, s.now)
	if got := s.queueDelay(a); got != Second {
		t.Fatalf("queue_p %v, want the 1 s cap", got)
	}
	ps.samples = nil
	s.mpAdvanced(p, p.out.lanes[0], ps, s.now)
	if a.unsentAhead != 0 || s.tSuspect(a) != base {
		t.Fatal("the snapshot outlived the unacknowledged bytes")
	}
}

// queue_p takes R_p with unsent_ahead, when the bytes are handed to the
// carrier: a later change of the rate leaves the snapshot alone.
func TestQueueDelaySnapshotsRate(t *testing.T) {
	s, p := statsMP()
	ol := p.out.lanes[0]
	measuredRate(p, 1e6)
	s.SetCarrierUnsent(p.id, 0, 200_000)
	snap := s.snapAhead(p)
	if snap.bytes != 200_000 || snap.delay != 200*Millisecond {
		t.Fatalf("snapshot %+v, want 200000 B, 200 ms", snap)
	}
	ps := &p.out.snd
	ps.samples = []sample{{end: 1, ahead: snap}}
	measuredRate(p, 1e5) // the rate falls before the oldest moves to it
	s.mpAdvanced(p, ol, ps, s.now)
	if got := s.queueDelay(p.mp); got != 200*Millisecond {
		t.Fatalf("queue_p %v, want the 200 ms of the handover", got)
	}
}
