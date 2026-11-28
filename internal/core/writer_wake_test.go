package core

import (
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// Start with two quiet, confirmed pipes. Tests drive only the named input:
// neither a peer ACK nor the session timer runs in the background.
func quietWriterPipes() (*Session, *Pipe, *Pipe) {
	s, extra := statsMP()
	s.ack = newAckState(s)
	for _, p := range s.pipes {
		p.out.sig = &WriteSignal{}
		p.out.lanes[0].confirmed = true
		p.out.lanes[0].tpSent = true
	}
	s.peerMaxRecord = 65535
	return s, s.pipes[0], extra
}

// Run one requested writer to its first empty poll without advancing time.
// Bound the loop so a self-wake on every empty poll fails deterministically.
func drainRequested(t *testing.T, s *Session, p *Pipe) []wire.Kind {
	t.Helper()
	sig := p.out.sig
	if !sig.Ready.Load() {
		t.Fatal("no poll requested for pipe", p.id)
	}
	b := GetBatch()
	defer PutBatch(b)
	var kinds []wire.Kind
	for range 12 {
		sig.Ready.Store(false)
		if !s.PollTransmit(p.id, 0, b, DefaultBatch, s.now) {
			if !sig.Ready.Load() {
				return kinds
			}
			continue
		}
		for _, record := range batchFrames(t, b) {
			kinds = append(kinds, record...)
		}
		s.WriteDone(b, b.Len(), nil, s.now)
	}
	t.Fatalf("pipe %d kept polling at a fixed time, sent %v", p.id, kinds)
	return nil
}

func TestWriterWakeEpoch(t *testing.T) {
	s, _, p := quietWriterPipes()
	p.mp.mode = join.Sealed
	s.mp.ep.list = []*epoch{{}}
	if err := s.AdvanceEpoch(); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if i == 1 {
			s.SetNow(p.mp.epochAt)
			s.expirePathDeadlines(s.now)
		}
		if k := drainRequested(t, s, p); !slices.Equal(k, []wire.Kind{wire.KindSessionEpoch}) {
			t.Fatalf("epoch poll sent %v", k)
		}
	}
}

func TestWriterWakeFollowingDatagram(t *testing.T) {
	s, a, b := quietWriterPipes()
	prim, _ := s.primary()
	if prim == b {
		a, b = b, a
	}
	s.peerMaxDgram, s.gates.MaxDatagram = 65535, 65535
	s.feat.DatagramFamily = true
	st := &Stream{id: 4, idSet: true, bidi: true, local: true, opened: true, hasS: true}
	st.snd.mpState().openSet = true
	st.snd.mps.openPipe = b.id
	s.streams[st.id] = st
	if err := s.SendDatagram(st, 0, []byte{9}); err != nil {
		t.Fatal(err)
	}
	if a.out.sig.Ready.Load() {
		t.Fatal("primary was offered the opening pipe's datagram")
	}
	if k := drainRequested(t, s, b); !slices.Equal(k, []wire.Kind{wire.KindDatagram}) {
		t.Fatalf("opening pipe sent %v", k)
	}
}

func TestWriterWakeExhaustedCredit(t *testing.T) {
	for _, kind := range []wire.Kind{wire.KindStreamDataBlocked, wire.KindDataBlocked} {
		s, p := channelSession(t, nil, false)
		p.out.sig = &WriteSignal{}
		s.loc[0].limit = 4
		s.peer.MaxStreamDataBidiRem, s.conn.sendMax = 16, 16
		if kind == wire.KindDataBlocked {
			s.conn.sendMax = 1
		} else {
			s.peer.MaxStreamDataBidiRem = 1
		}
		st, err := s.OpenStream(true)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			s.TakePollRequests()
			if n, err := s.Write(st, []byte{1}, false); n != 1 || err != nil {
				t.Fatal(n, err)
			}
			if i == 2 {
				if p.out.sig.Ready.Load() || len(s.TakePollRequests()) != 0 {
					t.Fatal("another write at the same limit published work")
				}
				continue
			}
			frames := drainRequested(t, s, p)
			if slices.Contains(frames, kind) != (i == 1) {
				t.Fatalf("write %d, wanted blocked frame %v only on second write, got %v", i, kind, frames)
			}
		}
		if kind == wire.KindStreamDataBlocked {
			if !s.onSendCredit(st, 16) {
				t.Fatal("stream credit was ignored")
			}
		} else if err := s.dispatch(p, &wire.Frame{Kind: wire.KindMaxData, Value: 16}, Input{}); err != nil {
			t.Fatal(err)
		}
		if frames := drainRequested(t, s, p); !slices.Contains(frames, wire.KindStream) {
			t.Fatalf("credit released %v", frames)
		}
	}
}

func TestUnwritableLaneGetsOnePermitOnRelease(t *testing.T) {
	s, p := channelSession(t, nil, false)
	p.out.sig = &WriteSignal{}
	s.loc[0].limit = 1
	s.peer.MaxStreamDataBidiRem, s.conn.sendMax = 8, 8
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	s.SetWritable(p.id, 0, false, s.now)
	for range 4 {
		if _, err := s.Write(st, []byte{1}, false); err != nil {
			t.Fatal(err)
		}
	}
	if p.out.sig.Ready.Load() || len(s.TakePollRequests()) != 0 {
		t.Fatal("an unwritable lane acquired a permit")
	}
	s.SetWritable(p.id, 0, true, s.now)
	s.SetWritable(p.id, 0, true, s.now)
	if got := s.TakePollRequests(); len(got) != 1 {
		t.Fatalf("writable transition registered %d permits", len(got))
	}
	if frames := drainRequested(t, s, p); !slices.Equal(frames, []wire.Kind{wire.KindStream}) {
		t.Fatalf("writable lane sent %v", frames)
	}
}

func TestCrossPathPlacementWaitsForCurrentCompletion(t *testing.T) {
	s, a, b := quietWriterPipes()
	s.loc[0].limit, s.peer.MaxStreamDataBidiRem, s.conn.sendMax = 1, 8, 8
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Write(st, []byte{1}, false); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	for _, p := range s.pipes {
		p.out.sig.Ready.Store(false)
	}
	s.TakePollRequests()
	lane := b.out.lane(0)
	if lane == nil {
		t.Fatal("second pipe has no outbound lane zero")
	}
	lane.writing, lane.handed = true, 1
	d := decision{p: a}
	for range 4 {
		s.chosen(&d, b)
	}
	if !lane.placementWaiting || len(s.TakePollRequests()) != 0 {
		t.Fatal("in-flight lane published another permit")
	}
	stale := Batch{Pipe: b.id, owner: &outLane{}, pos: 1}
	s.WriteDone(&stale, stale.Len(), nil, s.now)
	if !lane.writing || lane.handed != 1 || len(s.TakePollRequests()) != 0 {
		t.Fatal("obsolete completion changed the current lane")
	}
	active := Batch{Pipe: b.id, owner: lane, pos: 1}
	s.WriteDone(&active, active.Len(), nil, s.now)
	req := s.TakePollRequests()
	if lane.writing || lane.placementWaiting || len(req) != 1 || req[0].Pipe != b.id {
		t.Fatalf("completion registered %v", req)
	}
}

func TestWriterWakeHeldCopy(t *testing.T) {
	s, original, p := quietWriterPipes()
	s.feat.Multipath = true
	s.replicaBytes(5, false)
	st := &Stream{id: 4, idSet: true, opened: true, hasS: true, redund: 1}
	st.snd.q.write([]byte{7})
	s.SetPipeShaper(p.id, &delayShaper{})
	p.sh.hold = Time(Second)
	var d decision
	s.newDecision(&d, original)
	p.out.sig.Ready.Store(false) // discard installation wake
	s.copyChunk(&d, &recWriter{}, st, 0, 1, false)
	if k := drainRequested(t, s, p); len(k) != 0 || len(p.mp.copyQ) != 1 {
		t.Fatalf("held copy sent %v", k)
	}
	if d := s.shapeDeadline(); d != p.sh.hold {
		t.Fatalf("shaping deadline %v, want %v", d, p.sh.hold)
	}
	s.SetNow(p.sh.hold)
	s.shapeTimeout(s.now)
	if k := drainRequested(t, s, p); !slices.Equal(k, []wire.Kind{wire.KindStream}) || len(p.mp.copyQ) != 0 {
		t.Fatalf("released copy sent %v", k)
	}
}

func TestWriterWakeSingleLane(t *testing.T) {
	var s Session
	for id := range uint64(64) {
		p := &Pipe{id: id, out: &outDir{sig: &WriteSignal{}}}
		for lane := range uint64(16) {
			p.out.lanes = append(p.out.lanes, &outLane{lane: lane, sig: &WriteSignal{}})
		}
		s.pipes = append(s.pipes, p)
	}
	p := s.pipes[57]
	lane := p.out.lanes[12]
	for range 2 {
		s.wakeLaneWriter(p, lane)
		s.wakeLaneWriter(p, lane)
		s.QueueWriter(p.id, lane.lane, lane.sig) // a concurrent shutdown in the same critical section
		requests := s.TakePollRequests()
		if len(requests) != 1 || requests[0].Pipe != p.id || requests[0].Lane != lane.lane {
			t.Fatalf("requested lanes = %v", requests)
		}
		s.wakeLaneWriter(p, lane)
		if len(s.TakePollRequests()) != 0 || !lane.sig.Ready.Load() {
			t.Fatal("draining reservations consumed or duplicated a pending poll")
		}
		lane.sig.Ready.Store(false) // next poll can raise another request
	}
}

func TestRedundantSnapshotDeadlineWakesOnlyBackup(t *testing.T) {
	s, _, second := quietWriterPipes()
	third := capacityExtra(s)
	third.out.sig = &WriteSignal{}
	second.mp.sendPrio, third.mp.sendPrio = new(uint64(1)), new(uint64(2))
	for _, p := range s.pipes {
		p.out.sig.Ready.Store(false)
	}
	s.TakePollRequests()
	now := Time(Second)
	s.SetNow(now)
	s.mp.ackChanged, s.mp.redundantPending, s.mp.redundantAt = true, true, now
	s.mpHandleTimeout(now)
	for _, p := range s.pipes {
		if got, want := p.out.sig.Ready.Load(), p == second; got != want {
			t.Fatalf("writer %d ready = %t, want %t", p.id, got, want)
		}
	}
}
