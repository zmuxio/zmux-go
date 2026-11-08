package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// gateAtShaper plans records of shaping.MinMaxLen bytes before record seq at and
// of maxLen bytes from it on; the plan of record at sets a gate.
type gateAtShaper struct {
	at     uint64
	maxLen int
	gate   gateSpec
}

// gateSpec is the gate a plan sets: a pause from the plan's time, a rate and
// a burst.
type gateSpec struct {
	pause       time.Duration
	rate, burst uint64
}

func (g *gateAtShaper) Capabilities() shaping.Caps { return shaping.CapPlan }
func (g *gateAtShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	p.MaxLen = shaping.MinMaxLen
	if v.Seq >= g.at {
		p.MaxLen = g.maxLen
	}
	if v.Seq == g.at {
		p.Effects.Set = shaping.EffectGate
		p.Effects.Rate, p.Effects.Burst = g.gate.rate, g.gate.burst
		if g.gate.pause > 0 {
			p.Effects.PauseUntil = v.Now + g.gate.pause
		}
	}
}
func (g *gateAtShaper) OnEvent(*shaping.Event, *shaping.Effects) {}

// recordData returns the STREAM data bytes of each record of a byte-stream
// batch.
func recordData(t *testing.T, b *Batch) []int {
	t.Helper()
	var all []byte
	for _, x := range b.Bufs() {
		all = append(all, x...)
	}
	var out []int
	for len(all) > 0 {
		size, n := varint.ConsumeVarint(all)
		rec := all[n : n+int(size)]
		all = all[n+int(size):]
		d := 0
		for len(rec) > 0 {
			var f wire.Frame
			l, err := wire.DecodeFrame(rec, &f)
			if err != nil {
				t.Fatal(err)
			}
			if f.Kind == wire.KindStream {
				d += len(f.Data)
			}
			rec = rec[l:]
		}
		out = append(out, d)
	}
	return out
}

// A gate set by the plan of a record in the middle of a batch bounds the
// data of that record and of the rest of the batch, and its bucket is
// charged only for the data written after it took effect. DATAGRAM frames
// fill the records before the gate, one each; stream data follows in the
// gating record.
func TestShapingGateMidBatch(t *testing.T) {
	for name, gate := range map[string]gateSpec{
		"rate":  {rate: 1_000_000, burst: 100},
		"pause": {pause: 50 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			s, p := channelSession(t, nil, false)
			s.peer.MaxStreamDataBidiRem = 1 << 20
			s.loc[0].limit = 10
			s.conn.sendMax = 1 << 20
			const at = 3
			sh := &gateAtShaper{at: at, maxLen: 500, gate: gate}
			s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return sh })
			st, err := s.OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(st, make([]byte, 64<<10), false); err != nil {
				t.Fatal(err)
			}
			for range at + 1 {
				if err := s.SendDatagram(nil, 0, make([]byte, 50)); err != nil {
					t.Fatal(err)
				}
			}
			now := Time(Second)
			b := GetBatch()
			defer PutBatch(b)
			if !s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
				t.Fatal("nothing sent")
			}
			recs := recordData(t, b)
			s.WriteDone(b, nil, now)
			if len(recs) <= at {
				t.Fatalf("records %v: the gating record (seq %d) not in the batch", recs, at)
			}
			after := 0
			for i, n := range recs {
				if i < at && n > 0 {
					t.Fatalf("records %v: stream data before the gating record", recs)
				}
				after += n
			}
			ps := p.sh
			switch {
			case gate.pause > 0:
				if after != 0 {
					t.Fatalf("records %v: %d data bytes during the pause", recs, after)
				}
			default:
				if after > int(gate.burst) {
					t.Fatalf("records %v: %d data bytes after the gate, burst %d", recs, after, gate.burst)
				}
				if after == 0 {
					t.Fatalf("records %v: no data within the burst", recs)
				}
				if want := int64(int(gate.burst) - after); ps.bucket.tokens != want {
					t.Fatalf("bucket %d after %d gated bytes, want %d (data before the gate not charged)",
						ps.bucket.tokens, after, want)
				}
			}
		})
	}
}
