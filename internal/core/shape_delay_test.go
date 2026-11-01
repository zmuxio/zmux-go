package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// delayShaper plans every record with a delay.
type delayShaper struct {
	delay time.Duration
	plans int
}

func (d *delayShaper) Capabilities() shaping.Caps { return shaping.CapPlan }
func (d *delayShaper) Plan(_ *shaping.RecordView, p *shaping.RecordPlan) {
	d.plans++
	p.Delay = d.delay
}
func (d *delayShaper) OnEvent(*shaping.Event, *shaping.Reply) {}

// batchFrames returns the frame kinds of each record of a byte-stream
// batch.
func batchFrames(t *testing.T, b *Batch) [][]wire.Kind {
	t.Helper()
	var all []byte
	for _, x := range b.Bufs() {
		all = append(all, x...)
	}
	var recs [][]wire.Kind
	for len(all) > 0 {
		size, n := varint.ConsumeVarint(all)
		rec := all[n : n+int(size)]
		all = all[n+int(size):]
		var kinds []wire.Kind
		for len(rec) > 0 {
			var f wire.Frame
			l, err := wire.DecodeFrame(rec, &f)
			if err != nil {
				t.Fatal(err)
			}
			kinds = append(kinds, f.Kind)
			rec = rec[l:]
		}
		recs = append(recs, kinds)
	}
	return recs
}

// A record delayed by its plan holds the control frames owed meanwhile:
// nothing goes before the delay ends, then one record carries them with
// the stream data, under the one plan. A planned delay holds the whole
// record, control frames included; CONNECTION_CLOSE is exempt.
func TestShapingDelayHoldsControlFrames(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	ds := &delayShaper{delay: 20 * time.Millisecond}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return ds })
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(st, []byte("data"), false); err != nil {
		t.Fatal(err)
	}
	s.pend.maxData, s.pend.forceMaxData = true, true
	now := Time(Second)
	b := GetBatch()
	defer PutBatch(b)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
		t.Fatalf("a record went during its delay: %v", batchFrames(t, b))
	}
	if s.WantsWrite(p.id, 0) {
		t.Fatal("write interest during the delay")
	}
	due := now.Add(Duration(ds.delay))
	if got := s.NextTimeout(); got != due {
		t.Fatalf("deadline %v, want %v (the delay's end, exactly)", got, due)
	}
	// A CONNECTION_CLOSE is never held: checked below on a second session.
	s.HandleTimeout(due)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, due) {
		t.Fatal("nothing sent once the delay ended")
	}
	recs := batchFrames(t, b)
	first := recs[0]
	has := func(k wire.Kind) bool {
		for _, x := range first {
			if x == k {
				return true
			}
		}
		return false
	}
	if !has(wire.KindMaxData) || !has(wire.KindStream) {
		t.Fatalf("first record %v: control frames and data not together", first)
	}
	if ds.plans != len(recs)+1 && ds.plans != len(recs) {
		t.Fatalf("%d plans for %d records", ds.plans, len(recs))
	}
	s.WriteDone(b, nil, due)

	// CONNECTION_CLOSE goes during a delay.
	s2, p2 := channelSession(t, nil, false)
	ds2 := &delayShaper{delay: time.Hour}
	s2.SetShaper(func(uint64, uint8, int) shaping.Shaper { return ds2 })
	s2.pend.maxData, s2.pend.forceMaxData = true, true
	b2 := GetBatch()
	defer PutBatch(b2)
	if s2.PollTransmit(p2.id, 0, b2, DefaultBatch, now) {
		t.Fatal("a record went during its delay")
	}
	s2.Close(0, nil)
	if !s2.PollTransmit(p2.id, 0, b2, DefaultBatch, now) {
		t.Fatal("CONNECTION_CLOSE held by the delay")
	}
	recs = batchFrames(t, b2)
	last := recs[len(recs)-1]
	if last[len(last)-1] != wire.KindClose {
		t.Fatalf("records %v", recs)
	}
}

// PadTo within the last frame's Length field: PADDING before the last
// frame, which ends the record without Length; Pad alone: appended in
// full after the last frame, which keeps its Length field.
func TestShapingPadPlacement(t *testing.T) {
	data := make([]byte, 100) // a 2-byte Length field
	for i := range data {
		data[i] = 7
	}
	bare := wire.StreamHeaderLen(0, 0, len(data), false) + len(data)
	with := wire.StreamHeaderLen(0, 0, len(data), true) + len(data)
	for name, tc := range map[string]struct {
		plan  shaping.RecordPlan
		size  int
		kinds []wire.Kind
		zeros int // trailing PADDING bytes
	}{
		"pad_to inside Length": {shaping.RecordPlan{PadTo: bare + 1}, bare + 1,
			[]wire.Kind{wire.KindPadding, wire.KindStream}, 0},
		"pad_to at Length end": {shaping.RecordPlan{PadTo: with}, with, []wire.Kind{wire.KindStream}, 0},
		"pad_to beyond":        {shaping.RecordPlan{PadTo: with + 10}, with + 10, []wire.Kind{wire.KindStream, wire.KindPadding}, 10},
		"pad only":             {shaping.RecordPlan{Pad: 5}, with + 5, []wire.Kind{wire.KindStream, wire.KindPadding}, 5},
		"pad and pad_to":       {shaping.RecordPlan{PadTo: bare + 1, Pad: 3}, with + 3, []wire.Kind{wire.KindStream, wire.KindPadding}, 3},
	} {
		t.Run(name, func(t *testing.T) {
			s := New(DefaultConfig(), 0)
			s.shapeSess()
			ps := &pipeShape{s: s, sh: nopShaper{}, planned: true, plan: tc.plan}
			b := GetBatch()
			defer PutBatch(b)
			var w recWriter
			w.init(b, FramingBytes, 65535, 1<<20)
			w.sh = ps
			if !w.stream(0, 0, data, pin{}, false) {
				t.Fatal("stream frame refused")
			}
			w.end()
			var all []byte
			for _, x := range b.Bufs() {
				all = append(all, x...)
			}
			size, n := varint.ConsumeVarint(all)
			rec := all[n:]
			if int(size) != tc.size || len(rec) != tc.size {
				t.Fatalf("record of %d bytes, want %d", size, tc.size)
			}
			kinds := batchFrames(t, b)[0]
			// PADDING bytes decode one frame each: collapse runs.
			var runs []wire.Kind
			for _, k := range kinds {
				if len(runs) == 0 || k != wire.KindPadding || runs[len(runs)-1] != wire.KindPadding {
					runs = append(runs, k)
				}
			}
			if len(runs) != len(tc.kinds) {
				t.Fatalf("frames %v, want %v", runs, tc.kinds)
			}
			for i := range runs {
				if runs[i] != tc.kinds[i] {
					t.Fatalf("frames %v, want %v", runs, tc.kinds)
				}
			}
			z := 0
			for i := len(rec) - 1; i >= 0 && rec[i] == 0; i-- {
				z++
			}
			if z != tc.zeros {
				t.Fatalf("%d trailing PADDING bytes, want %d", z, tc.zeros)
			}
		})
	}
}

// stateShaper moves its state on every plan, like a state machine on
// record_out: state 0 pads to 768 after a delay and arms timer 0 on its
// first plan, state 1 pads to 300. The timer only arrives as an event.
type stateShaper struct {
	state int
	armed bool
	seqs  []uint64 // RecordView.Seq of each Plan call
}

func (x *stateShaper) Capabilities() shaping.Caps { return shaping.CapPlan }
func (x *stateShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	x.seqs = append(x.seqs, v.Seq)
	if x.state == 0 {
		p.PadTo, p.Delay = 768, 20*time.Millisecond
		if !x.armed {
			x.armed = true
			p.Reply.Timer[0] = &shaping.Timer{After: 10 * time.Millisecond}
		}
	} else {
		p.PadTo = 300
	}
	x.state ^= 1
}
func (x *stateShaper) OnEvent(*shaping.Event, *shaping.Reply) {}

// Plan is called once per record: an event between a plan and the
// sending of its record does not plan the record again, which would move
// a shaper that counts plans as records sent one step too far.
func TestShapingPlansARecordOnce(t *testing.T) {
	s, p := channelSession(t, nil, false)
	x := &stateShaper{}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return x })
	s.pend.maxData, s.pend.forceMaxData = true, true
	now := Time(Second)
	b := GetBatch()
	defer PutBatch(b)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
		t.Fatal("a record went during its delay")
	}
	at := now.Add(10 * Millisecond)
	if got := s.NextTimeout(); got != at {
		t.Fatalf("deadline %v, want the timer at %v", got, at)
	}
	s.HandleTimeout(at)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, at) {
		t.Fatal("the record went before its delay ended")
	}
	end := now.Add(20 * Millisecond)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, end) {
		t.Fatal("the planned record did not go after its delay")
	}
	if n := b.Len(); n < 768 {
		t.Fatalf("%d bytes: the record did not keep its plan's pad_to 768", n)
	}
	s.WriteDone(b, nil, end)
	if len(x.seqs) == 0 || len(x.seqs) > 2 || x.seqs[0] != 0 || len(x.seqs) == 2 && x.seqs[1] != 1 {
		t.Fatalf("Plan calls by Seq %v, want one for record 0 (then record 1)", x.seqs)
	}
}
