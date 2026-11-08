package zmux

import (
	"context"
	"net"
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
	padTo int
	mu    sync.Mutex
	pipes []ShapedPipe
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
// record by the listener's Config.Shaper, which sees the carrier's kind.
func TestShaperFromFirstRecord(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	rs := &recConn{Conn: b, on: true}
	k := &kindRecorder{padTo: 700}
	lst := mustNewListener(&ListenerConfig{Config: &Config{Shaper: k.factory}})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: rs, W: rs, Carrier: CarrierStreaming}, PipeHint{}) }()
	c, err := Start(ctx, PipeIO{R: a, W: a, Carrier: CarrierStreaming}, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transfer(t, c, s, 1000)
	sizes, _ := rs.records()
	if len(sizes) == 0 || wireLen(sizes[0]) < 700 {
		t.Fatalf("server records %v, the first not shaped", sizes)
	}
	if got := k.seen(); len(got) == 0 || got[0] != (ShapedPipe{ID: 0, Carrier: CarrierStreaming}) {
		t.Fatalf("factory saw %v", got)
	}
	for _, x := range []*Conn{c, s} {
		if st := x.PipeStats(); len(st) != 1 || st[0].Carrier != CarrierStreaming {
			t.Fatalf("pipe stats %+v", st)
		}
	}
	_ = c.Close()
	_ = s.Close()
	waitDone(t, c)
	waitDone(t, s)
}

// TestCarrierKindJoin reports each pipe's own carrier kind on both ends:
// to the factories and in PipeStats for the bootstrap and for a pipe
// joined through the listener, and in the joined pipe's events.
func TestCarrierKindJoin(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ks, kc := &kindRecorder{}, &kindRecorder{}
	scfg, ccfg := mpConfig(), mpConfig()
	scfg.Shaper, ccfg.Shaper = ks.factory, kc.factory
	lst := mustNewListener(&ListenerConfig{Config: scfg})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// The server labels its carriers in accept order, as an adapter of
	// two kinds would.
	kinds := []CarrierKind{CarrierWebSocket, CarrierQUIC}
	go func() {
		for i := 0; ; i++ {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			p := NetPipe(nc)
			p.Carrier = kinds[min(i, 1)]
			go func() { _ = lst.HandlePipe(ctx, p, PipeHint{}) }()
		}
	}()
	dial := func(k CarrierKind) PipeIO {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		p := NetPipe(nc)
		p.Carrier = k
		return p
	}
	c, err := Start(ctx, dial(CarrierWebSocket), RoleClient, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.AddPipe(ctx, dial(CarrierQUIC), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]CarrierKind{0: CarrierWebSocket, p.ID(): CarrierQUIC}
	// PipeAdded of the join is queued before AddPipe returned.
	for {
		ev, err := c.NextEvent(ctx)
		if err != nil {
			t.Fatalf("no PipeAdded for pipe %d: %v", p.ID(), err)
		}
		if ev.Kind == EventPipeAdded && ev.Pipe == p.ID() {
			if ev.Carrier != CarrierQUIC {
				t.Fatalf("PipeAdded %d: carrier %v", ev.Pipe, ev.Carrier)
			}
			break
		}
	}
	// The acceptor attaches at the confirmation, and its first record on a pipe
	// creates the shaper: traffic goes on until both ends have seen both pipes.
	for len(kc.seen()) < 2 || len(ks.seen()) < 2 || len(s.PipeStats()) < 2 {
		if ctx.Err() != nil {
			t.Fatalf("client factory %v, server factory %v, server pipes %+v", kc.seen(), ks.seen(), s.PipeStats())
		}
		transfer(t, c, s, 100)
		transfer(t, s, c, 100)
	}
	for name, k := range map[string]*kindRecorder{"client": kc, "server": ks} {
		for _, sp := range k.seen() {
			if want[sp.ID] != sp.Carrier {
				t.Fatalf("%s factory saw %v, want %v", name, k.seen(), want)
			}
		}
	}
	for _, x := range []*Conn{c, s} {
		st := x.PipeStats()
		if len(st) != 2 {
			t.Fatalf("pipe stats %+v", st)
		}
		for _, ps := range st {
			if want[ps.Pipe] != ps.Carrier {
				t.Fatalf("pipe %d: carrier %v, want %v", ps.Pipe, ps.Carrier, want[ps.Pipe])
			}
		}
	}
	_ = c.Close()
	_ = s.Close()
	waitDone(t, c)
	waitDone(t, s)
}
