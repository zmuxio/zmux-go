package zmux

import (
	"context"

	"slices"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/shaping"
)

// kindRecorder is a ShaperFactory that pads every record to padTo and
// records the pipes it was asked for.
type kindRecorder struct {
	pipes []ShapedPipe
	padTo int
	mu    sync.Mutex
}

func (k *kindRecorder) factory(p ShapedPipe) shaping.Shaper {
	k.mu.Lock()
	k.pipes = append(k.pipes, p)
	k.mu.Unlock()
	return &fnShaper{caps: shaping.CapPlan, plan: func(_ *shaping.RecordView, rp *shaping.RecordPlan) { rp.PadTo = k.padTo }}
}

func (k *kindRecorder) seen() []ShapedPipe {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.pipes)
}

// TestShaperFromFirstRecord shapes an accepted session from its first
// record by the listener's Config.Shaping.Factory, which sees the carrier's kind.
func TestShaperFromFirstRecord(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	rs := &recConn{Conn: b}
	k := &kindRecorder{padTo: 700}
	lst := mustNewListener(&ListenerConfig{Config: &Config{Shaping: &ShapingPolicy{Factory: k.factory}}})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: rs, W: rs, Carrier: CarrierRaw}, PipeHint{}) }()
	c, err := Start(ctx, PipeIO{R: a, W: a, Carrier: CarrierRaw}, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transfer(t, c, s, 1000)
	// The first record is shaped, its padding paid by the profile's floor
	// allowance alone: no baseline precedes it. The trailing zero count also
	// takes the zero bytes the frames themselves may end with (a varint
	// field of zero), at most a few.
	sizes, zeros := rs.records(false)
	floor := shaping.Lightweight().FloorBytes
	if len(sizes) == 0 || zeros[0] == 0 || uint64(zeros[0]) > floor+4 {
		t.Fatalf("first record %v with %v padding bytes, want some and at most the %d floor", sizes, zeros, floor)
	}
	if got := k.seen(); len(got) == 0 || got[0] != (ShapedPipe{PipeID: 0, Carrier: CarrierRaw}) {
		t.Fatalf("factory saw %v", got)
	}
	for _, x := range []*Conn{c, s} {
		if st := x.PipeStats(); len(st) != 1 || st[0].Carrier != CarrierRaw {
			t.Fatalf("pipe stats %+v", st)
		}
	}
	_ = c.Close()
	_ = s.Close()
	waitDone(t, c)
	waitDone(t, s)
}
