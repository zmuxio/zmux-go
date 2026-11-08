package core

import (
	"fmt"
	"hash/fnv"
	"os"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// diffShaper plans from a fixed random source, with no injection.
type diffShaper struct {
	x    uint64
	gate bool
	n    int
}

func (d *diffShaper) next(m uint64) uint64 {
	d.x += 0x9E3779B97F4A7C15
	z := d.x
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return (z ^ (z >> 31)) % m
}

func (d *diffShaper) Capabilities() shaping.Caps { return shaping.CapPlan }

func (d *diffShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	switch d.next(4) {
	case 0:
		t := 64 + int(d.next(3000))
		p.MaxLen, p.PadTo = t, t
	case 1:
		p.MaxLen = 100 + int(d.next(4000))
		p.PadMultiple = 1 + int(d.next(512))
	case 2:
		p.Pad = int(d.next(300))
	}
	p.Flush = d.next(5) == 0
	if d.next(3) == 0 {
		p.Delay = time.Duration(d.next(28_000)) * time.Microsecond
	}
	d.n++
	if d.gate && d.n%40 == 0 {
		p.Effects.Set = shaping.EffectGate
		p.Effects.Rate = 20_000 + d.next(2_000_000)
		p.Effects.Burst = 2000 + d.next(30_000)
		p.Effects.PauseUntil = v.Now + time.Duration(d.next(20_000))*time.Microsecond
	}
}

func (d *diffShaper) OnEvent(*shaping.Event, *shaping.Effects) {}

// shapeTrace is the (time, record length) sequence of a session under d.
func shapeTrace(t *testing.T, d *diffShaper) string {
	t.Helper()
	s, p := channelSession(t, nil, false)
	s.peerZmux = true
	s.peer.MaxStreamDataBidiRem = 1 << 30
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 30
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return d })
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	out := ""
	for i := range 6000 {
		now := Time(Second) + Time(i)*Time(Millisecond)
		s.SetNow(now)
		s.shapeTimeout(now)
		if i%3 == 0 {
			if _, err := s.Write(st, make([]byte, int(d.next(9000))), false); err != nil {
				t.Fatal(err)
			}
		}
		for range 4 {
			sizes := poll(t, s, p, 6000, now)
			if sizes == nil {
				break
			}
			out += fmt.Sprintln(i, sizes)
		}
	}
	return out
}

// With no limit set, record lengths and times follow the plans alone.
func TestShapeTraceUnchanged(t *testing.T) {
	want := map[string]uint64{"plain": 0x1daa17b290d9266f, "gate": 0x814ea95a88dff94c}
	for name, gate := range map[string]bool{"plain": false, "gate": true} {
		tr := shapeTrace(t, &diffShaper{x: 42, gate: gate})
		if f := os.Getenv("ZMUX_SHAPE_TRACE"); f != "" {
			if err := os.WriteFile(f+"."+name, []byte(tr), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(tr))
		if h.Sum64() != want[name] {
			t.Errorf("%s: trace hash %#x, want %#x", name, h.Sum64(), want[name])
		}
	}
}
