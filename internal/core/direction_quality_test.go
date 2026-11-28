package core

import "testing"

func TestDirectionQualityProvesFixedBacklogOnBothPaths(t *testing.T) {
	s := capacityMP()
	pipes := []*Pipe{s.pipes[0], capacityExtra(s)}
	s.conn.sendMax = 8192
	for i, p := range pipes {
		id := uint64(i) * 4
		st := &Stream{id: id, idSet: true, hasS: true}
		st.snd.maxData, st.snd.q.end = 8192, 4096
		st.snd.mpState().bind = binding{pipe: p.id, set: true}
		s.streams[id] = st
		s.sched.ensure(st)
	}
	first := Time(Second)
	s.SetNow(first)
	for _, p := range pipes {
		s.RecordDelivery(p.id, first-Time(Second), first, 100, 0, false)
	}
	end := first.Add(Second)
	s.SetNow(end)
	for _, p := range pipes {
		p.out.snd.delivered = 100
		s.RecordDelivery(p.id, first, end, 100, 0, false)
		if got := s.DirectionQuality(p.id).Delivery.Load; got != LoadBacklogged {
			t.Fatalf("pipe %d load = %v", p.id, got)
		}
	}
	p := pipes[1]
	p.out.snd.deliveryRestrictedThrough = 150 // A different stream used a restricted share.
	p.out.snd.delivered = 200
	s.SetNow(end.Add(Second))
	s.RecordDelivery(p.id, end, end.Add(Second), 100, 0, false)
	if got := s.DirectionQuality(p.id).Delivery.Load; got == LoadBacklogged {
		t.Fatal("the witness did not cover the other stream's bytes")
	}
}
