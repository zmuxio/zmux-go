package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// Data lanes take the paced room in turns; lane 0
// and the copy lane are never held back.
func TestDataLaneTurns(t *testing.T) {
	s := &Session{}
	l0, l1, l2, l3 := &outLane{lane: 0}, &outLane{lane: 1}, &outLane{lane: 2}, &outLane{lane: 3}
	p := &Pipe{out: &outDir{lanes: []*outLane{l0, l1, l2, l3}}}
	p.out.lo.copyLane = l3
	take := func(l *outLane) bool {
		if !s.dataTurn(p, l) {
			return false
		}
		s.tookTurn(p, l)
		return true
	}
	if !take(l1) {
		t.Fatal("first turn refused")
	}
	if !take(l1) { // nobody else asked: a new round
		t.Fatal("lone lane refused")
	}
	if !take(l2) {
		t.Fatal("unserved lane refused")
	}
	l2.served = false
	l2.waiting = true // lane 2 asked and waits
	if take(l1) {
		t.Fatal("served lane took the turn of a waiting one")
	}
	if !take(l0) || !take(l3) || !take(l3) {
		t.Fatal("lane 0 or the copy lane held back")
	}
	if !take(l2) || !take(l1) {
		t.Fatal("turns did not advance")
	}
}

// A record batch must retain the direction's lane until its final position is accounted.
func TestBatchKeepsDirectionOnItsPhysicalLane(t *testing.T) {
	s := New(DefaultConfig(), 0)
	info := CarrierInfo{Lanes: true}
	p := s.AttachBootstrap(info, info, nil)
	s.peerSeen = true
	s.state = StateActive
	s.peer.MaxLanes = 3
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	s.feat = Features{PipeAck: true, LanesOut: true, LanesIn: true}
	p.out.lo.offered = true
	for i := uint64(1); i <= 2; i++ {
		p.out.lanes = append(p.out.lanes, &outLane{lane: i, info: info, confirmed: true})
	}
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	st.urgency = 3
	if _, err := s.Write(st, make([]byte, 48<<10), false); err != nil {
		t.Fatal(err)
	}
	var b Batch
	var w recWriter
	w.init(&b, info.Framing, 65536, 65536)
	defer b.Discard()
	s.writeData(&w, p, p.out.lanes[1])
	w.end()
	if len(w.touched) == 0 {
		t.Fatal("batch did not send stream data")
	}
	if st.snd.lane != 1 {
		t.Fatalf("batch rebound its direction to lane %d, want 1", st.snd.lane)
	}
	s.afterBatch(&w, p, p.out.lanes[1])
	if st.snd.laneEnd <= p.out.lanes[1].acked {
		t.Fatal("batch already covered by old acknowledgement")
	}
	if got, _, _ := s.laneFor(p, st); got != p.out.lanes[1] {
		t.Fatal("direction moved before its batch was acknowledged")
	}
	s.Release(st)
}

// planShaper plans every record with a fixed delay and padding target and
// records the lanes it planned.
type planShaper struct {
	delay time.Duration
	padTo int
	lanes []uint64
}

func (d *planShaper) Capabilities() shaping.Caps { return shaping.CapPlan }
func (d *planShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	d.lanes = append(d.lanes, v.Lane)
	p.Delay, p.PadTo = d.delay, d.padTo
}
func (d *planShaper) OnEvent(*shaping.Event, *shaping.Reply) {}

// A lane's PIPE_LANE record is a record of its pipe like any other
// (MULTIPATH.md 5.8): the pipe's Shaper plans it, its delay holds it and,
// while it waits, every other lane of the pipe, and its padding is
// applied, PIPE_LANE and PADDING being all the record holds.
func TestLaneFirstRecordShaped(t *testing.T) {
	s := New(DefaultConfig(), 0)
	info := CarrierInfo{Lanes: true}
	p := s.AttachBootstrap(info, info, nil)
	p.out.lanes[0].tpSent = true
	s.peerSeen = true
	s.state = StateActive
	s.peer.MaxLanes = 3
	s.peerMaxRecord = 65535
	s.feat = Features{PipeAck: true, LanesOut: true, LanesIn: true}
	p.out.lo.offered = true
	ps := &planShaper{delay: 20 * time.Millisecond, padTo: 200}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return ps })
	lane, ok := s.LaneOpened(p.id, info, nil)
	if !ok {
		t.Fatal("lane not opened")
	}
	now := Time(Second)
	s.SetNow(now)
	b := GetBatch()
	defer PutBatch(b)
	if s.PollTransmit(p.id, lane, b, DefaultBatch, now) {
		t.Fatal("the PIPE_LANE record went during its delay")
	}
	if len(ps.lanes) != 1 || ps.lanes[0] != lane {
		t.Fatalf("planned lanes %v, want [%d]", ps.lanes, lane)
	}
	if s.WantsWrite(p.id, lane) || s.WantsWrite(p.id, 0) {
		t.Fatal("a lane of the pipe wants to write while its record is delayed")
	}
	if d := s.ShapingDeadline(); d != now.Add(20*Millisecond) {
		t.Fatalf("shaping deadline %v, want the delay's end", d)
	}
	now = now.Add(20 * Millisecond)
	s.SetNow(now)
	s.shapeTimeout(now)
	if !s.WantsWrite(p.id, lane) {
		t.Fatal("the lane does not want to write after the delay")
	}
	if !s.PollTransmit(p.id, lane, b, DefaultBatch, now) {
		t.Fatal("no PIPE_LANE record after the delay")
	}
	recs := batchFrames(t, b)
	if len(recs) != 1 || len(recs[0]) < 2 || recs[0][0] != wire.KindPipeLane {
		t.Fatalf("records %v, want one PIPE_LANE record", recs)
	}
	for _, k := range recs[0][1:] {
		if k != wire.KindPadding {
			t.Fatalf("the PIPE_LANE record holds %v", recs[0])
		}
	}
	if size, _ := varint.ConsumeVarint(b.Bufs()[0]); size != 200 {
		t.Fatalf("PIPE_LANE record of %d bytes, want the planned 200", size)
	}
}
