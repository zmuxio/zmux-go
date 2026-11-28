package zmuxws

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// A WebSocket carrier handed to HandlePipe is admitted on its first
// message, whole, replayed to the listener with Skip 0 and labelled with
// the decision's variant.
func TestAdmissionFirstMessage(t *testing.T) {
	ctx := testContext(t)
	var seen atomic.Int32
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{Peek: 1, MaxPeek: 1 << 16,
		Decide: func(_ context.Context, _ zmux.CarrierInfo, prefix []byte) zmux.Decision {
			if c, _ := admit.ClassifyFirstBytes(prefix, zmux.FramingRecords); c != admit.FirstBytesRecord || len(prefix) < 9 {
				return zmux.Decision{Verdict: zmux.VerdictClose}
			}
			seen.Store(int32(len(prefix)))
			return zmux.Decision{Verdict: zmux.VerdictAccept, Variant: "ws"}
		}}})
	defer func() { _ = l.Close() }()
	srv, cli := pipePair(t)
	go func() { _ = l.HandlePipe(ctx, srv, zmux.PipeHint{}) }()
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

// VerdictMore on an upgrade's head is VerdictClose, before anything is
// answered: a head has no bytes to read more of (VerdictDefer decides on
// the carrier).
func TestAdmissionMoreOnHead(t *testing.T) {
	o := &zmuxhttp.Options{Next: next}
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{Peek: 4, MaxPeek: 64,
		Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictMore, Need: 8, Variant: "h"}
		}}})
	u := server(t, l, o)
	if b := rawUpgrade(t, u); len(b) != 0 {
		t.Fatalf("answered %q", b)
	}
	if st, vs := l.Stats().Admission, l.VariantStats(); st.Closed != 1 || !slices.Contains(vs, zmux.VariantStat{Name: "h", Closed: 1}) {
		t.Fatalf("stats %+v, variants %+v", st, vs)
	}
}
