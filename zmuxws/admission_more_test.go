package zmuxws

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// A WebSocket carrier handed to HandlePipe is admitted on its first
// message, whole, replayed to the listener with Skip 0 and labelled with
// the decision's variant.
func TestAdmissionFirstMessage(t *testing.T) {
	ctx := testContext(t)
	var seen atomic.Int32
	l := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{Peek: 1, MaxPeek: 1 << 16,
		Decide: func(_ context.Context, _ *zmux.CarrierInfo, prefix []byte) zmux.Decision {
			if c, _ := zmux.ClassifyFirstBytes(prefix, zmux.FramingRecords); c != zmux.FirstBytesRecord || len(prefix) < 9 {
				return zmux.Decision{Verdict: zmux.VerdictClose}
			}
			seen.Store(int32(len(prefix)))
			return zmux.Decision{Verdict: zmux.VerdictAccept, Variant: "ws"}
		}}})
	defer func() { _ = l.Close() }()
	srv, cli := pipePair(t)
	go func() { _ = l.HandlePipe(ctx, srv, zmux.JoinHint{}) }()
	c, err := zmux.Start(ctx, cli, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, ctx, c, s, 1<<10)
	closeBoth(c, s)
	if vs := l.VariantStats(); !slices.Contains(vs, zmux.VariantStat{Name: "ws", Accepted: 1}) || seen.Load() < 9 {
		t.Fatalf("variants %+v, first message %d bytes", vs, seen.Load())
	}
}

// An upgrade is decided on its head: VerdictMore is VerdictClose.
func TestAdmissionMoreOnHead(t *testing.T) {
	o := &zmux.HTTPOptions{Next: next}
	l := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{Peek: 4, MaxPeek: 64,
		Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictMore, Need: 8, Variant: "h"}
		}}})
	u := server(t, l, o)
	b, d := rawUpgrade(t, u)
	if len(b) != 0 || d > 5*time.Second {
		t.Fatalf("answered %q after %v", b, d)
	}
	if st, vs := l.Stats().Admission, l.VariantStats(); st.Closed != 1 || !slices.Contains(vs, zmux.VariantStat{Name: "h", Closed: 1}) {
		t.Fatalf("stats %+v, variants %+v", st, vs)
	}
}

// A dialed carrier exposes its 101 response header; one made by Pipe has
// none.
func TestResponseHeader(t *testing.T) {
	ctx := testContext(t)
	l := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig()})
	defer func() { _ = l.Close() }()
	u := server(t, l, &zmux.HTTPOptions{Next: next})
	p, err := DialPipe(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closePipe(p)
	h := p.W.(zmux.ResponseHeaderer).ResponseHeader()
	if h == nil || h.Get("Date") == "" {
		t.Fatalf("header %v", h)
	}
	srv, cli := pipePair(t)
	if srv.W.(zmux.ResponseHeaderer).ResponseHeader() != nil || cli.W.(zmux.ResponseHeaderer).ResponseHeader() != nil {
		t.Fatal("a header on a carrier made by Pipe")
	}
}

func closePipe(p zmux.PipeIO) { _ = p.W.Close() }
