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
	if !sig.Pending {
		t.Fatal("no poll requested for pipe", p.id)
	}
	b := GetBatch()
	defer PutBatch(b)
	var kinds []wire.Kind
	for range 12 {
		sig.Pending = false
		if !s.PollTransmit(p.id, 0, b, DefaultBatch, s.now) {
			if !sig.Pending {
				return kinds
			}
			continue
		}
		for _, record := range batchFrames(t, b) {
			kinds = append(kinds, record...)
		}
		s.WriteDone(b, nil, s.now)
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
			s.epochTimers(true, func(at Time) bool { return at <= s.now })
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
	s.cfg.Datagrams = true
	s.peerMaxDgram, s.gates.MaxDatagram = 65535, 65535
	s.feat.Flows = true
	st := &Stream{id: 4, idSet: true, bidi: true, local: true, opened: true, hasS: true}
	st.snd.mpState().openSet = true
	st.snd.mps.openPipe = b.id
	s.streams[st.id] = st
	if err := s.SendDatagram(st, 0, []byte{9}); err != nil {
		t.Fatal(err)
	}
	if k := drainRequested(t, s, a); len(k) != 0 {
		t.Fatalf("primary sent %v", k)
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
			if n, err := s.Write(st, []byte{1}, false); n != 1 || err != nil {
				t.Fatal(n, err)
			}
			frames := drainRequested(t, s, p)
			if slices.Contains(frames, kind) != (i == 1) {
				t.Fatalf("write %d, wanted blocked frame %v only on second write, got %v", i, kind, frames)
			}
		}
	}
}

func TestWriterWakeHeldCopy(t *testing.T) {
	s, original, p := quietWriterPipes()
	s.feat.Multipath = true
	st := &Stream{id: 4, idSet: true, opened: true, hasS: true, redund: 1}
	st.snd.q.write([]byte{7})
	s.SetPipeShaper(p.id, &delayShaper{})
	p.sh.hold = Time(Second)
	var d decision
	s.newDecision(&d, original)
	p.out.sig.Pending = false // discard installation wake
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

func TestWriterWakeOpportunistic(t *testing.T) {
	s, fast, slow := quietWriterPipes()
	fast.mp.rtt.smoothed = Millisecond
	slow.mp.rtt.smoothed = 100 * Millisecond
	s.conn.sendMax = 65535
	s.loc[0].limit = 10
	s.peer.MaxStreamDataBidiRem = 65535
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PinID(st); err != nil {
		t.Fatal(err)
	}
	st.snd.q.write([]byte{1})
	st.snd.sent = 1
	st.opened = true
	slow.out.lanes[0].log = []retained{{d: frameDesc{kind: dStream, id: st.id, n: 1}, due: Millisecond}}
	st.snd.mpState().bind = binding{set: true, moving: true, pipe: slow.id, moveTo: fast.id}
	s.noteBlocked(st)
	fast.out.sig.Pending = false // discard setup wakes before the new write
	s.SetNow(Time(Second))
	if n, err := s.Write(st, []byte{2}, false); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	if k := drainRequested(t, s, fast); !slices.Contains(k, wire.KindStream) || s.stats.BytesOpportunistic != 1 {
		t.Fatalf("fast pipe sent %v, opportunistic bytes %d", k, s.stats.BytesOpportunistic)
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
		if len(s.TakePollRequests()) != 0 || !lane.sig.Pending {
			t.Fatal("draining reservations consumed or duplicated a pending poll")
		}
		lane.sig.Pending = false // next poll can raise another request
	}
}

func TestRedundantSnapshotDeadlineWakesAllTiers(t *testing.T) {
	s, _, second := quietWriterPipes()
	third := capacityExtra(s)
	third.out.sig = &WriteSignal{}
	second.mp.sendPrio, third.mp.sendPrio = new(uint64(1)), new(uint64(2))
	for _, p := range s.pipes {
		p.out.sig.Pending = false
	}
	s.TakePollRequests()
	now := Time(Second)
	s.SetNow(now)
	s.mp.ackChanged, s.mp.redundantAt = true, now
	s.mpHandleTimeout(now)
	for _, p := range s.pipes {
		if !p.out.sig.Pending {
			t.Fatalf("redundant snapshot deadline skipped writer %d", p.id)
		}
	}
}
