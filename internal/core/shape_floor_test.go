package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
)

// A MaxLen below shaping.MinMaxLen is raised to it and counted as clamped.
func TestShapingMaxLenFloor(t *testing.T) {
	s := New(DefaultConfig(), 0)
	s.shapeSess()
	p := &Pipe{}
	ps := &pipeShape{s: s, p: p, sh: &fnPlan{max: 10}, caps: shaping.CapPlan}
	b := GetBatch()
	defer PutBatch(b)
	var w recWriter
	w.init(b, FramingBytes, 65535, 1<<20)
	w.sh = ps
	ps.prep(&w)
	ps.prep(&w) // the same plan
	if w.cut != shaping.MinMaxLen || s.ShapingStats().Clamped != 1 {
		t.Fatalf("cut %d, clamped %d; want %d, 1", w.cut, s.ShapingStats().Clamped, shaping.MinMaxLen)
	}
}

type fnPlan struct{ max int }

func (f *fnPlan) Capabilities() shaping.Caps                        { return shaping.CapPlan }
func (f *fnPlan) Plan(_ *shaping.RecordView, p *shaping.RecordPlan) { p.MaxLen = f.max }
func (f *fnPlan) OnEvent(*shaping.Event, *shaping.Reply)            {}

// Every record of a message-framed carrier starts a carrier write; under
// byte framing only the first record of a batch does.
func TestShapingWriteStart(t *testing.T) {
	for _, tc := range []struct {
		framing Framing
		second  bool
	}{{FramingBytes, false}, {FramingRecords, true}} {
		s := New(DefaultConfig(), 0)
		s.shapeSess()
		ps := &pipeShape{s: s, p: &Pipe{}, sh: &fnPlan{}, caps: shaping.CapPlan}
		b := GetBatch()
		var w recWriter
		w.init(b, tc.framing, 65535, 1<<20)
		w.sh = ps
		ps.prep(&w)
		if !ps.v.WriteStart {
			t.Fatalf("framing %v: first record not a write start", tc.framing)
		}
		w.recs, ps.planned = 1, false
		ps.prep(&w)
		if ps.v.WriteStart != tc.second {
			t.Fatalf("framing %v: second record WriteStart %v", tc.framing, ps.v.WriteStart)
		}
		PutBatch(b)
	}
}
