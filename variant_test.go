package zmux

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/shaping"
)

// Report records the three outcomes in the entry of its kind and variant
// only.
func TestCarrierCacheReport(t *testing.T) {
	cc := &CarrierCache{BackoffMin: time.Minute, BackoffMax: time.Hour}
	x := Candidate{Kind: CarrierStreaming, Variant: "x"}
	now := time.Now()
	cc.Report("k", CarrierStreaming, "x", CacheUsable)
	if good, _ := cc.state("k", x, now); !good {
		t.Fatal("usable not recorded")
	}
	if good, until := cc.state("k", Candidate{Kind: CarrierStreaming}, now); good || !until.IsZero() {
		t.Fatal("variant \"\" shares the entry")
	}
	cc.Report("k", CarrierStreaming, "x", CacheFailed)
	if _, until := cc.state("k", x, now); until.Sub(now) < 50*time.Second || until.Sub(now) > 2*time.Minute {
		t.Fatalf("failed: skipped until %v from now", until.Sub(now))
	}
	cc.Report("k", CarrierStreaming, "x", CacheRejected)
	if _, until := cc.state("k", x, now); until.Sub(now) < 59*time.Minute {
		t.Fatalf("rejected: skipped until %v from now", until.Sub(now))
	}
	b, err := cc.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var snap cacheSnapshot
	if err := json.Unmarshal(b, &snap); err != nil || len(snap.Entries) != 1 || snap.Entries[0].Variant != "x" {
		t.Fatalf("snapshot %s", b)
	}
	re := &CarrierCache{BackoffMax: time.Hour}
	if err := re.Restore(b); err != nil {
		t.Fatal(err)
	}
	if _, until := re.state("k", x, now); until.IsZero() {
		t.Fatal("restored entry lost its variant")
	}
}

// Two candidates of one kind with different variants: the failed one is
// cached under its variant, the chosen one labels the pipe, its events,
// its statistics and its ShapedPipe.
func TestDialModeVariants(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lst := mustNewListener(&ListenerConfig{Config: &Config{}})
	defer func() { _ = lst.Close() }()
	failing := Candidate{Kind: CarrierStreaming, Variant: "a",
		Dial: func(context.Context, Credential) (PipeIO, error) { return PipeIO{}, errors.New("no") }}
	working := Candidate{Kind: CarrierStreaming, Variant: "b", Dial: func(context.Context, Credential) (PipeIO, error) {
		a, b := net.Pipe()
		go func() { _ = lst.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
		return NetPipe(a), nil
	}}
	var shaped []ShapedPipe
	cfg := &Config{Shaper: func(sp ShapedPipe) shaping.Shaper {
		shaped = append(shaped, sp)
		return nil
	}}
	cache := &CarrierCache{}
	c, err := DialMode(ctx, AutoMode(failing, working), cfg, &ModePolicy{Cache: cache, Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	if _, until := cache.state("k", failing, time.Now()); until.IsZero() {
		t.Fatal("failed variant not cached")
	}
	if good, _ := cache.state("k", working, time.Now()); !good {
		t.Fatal("chosen variant not cached")
	}
	if st := c.PipeStats(); len(st) != 1 || st[0].Variant != "b" {
		t.Fatalf("pipe stats %+v", st)
	}
	if cs := c.ConnectionStats(); len(cs.CarrierCandidates) != 2 || cs.CarrierCandidates[0].Variant != "a" {
		t.Fatalf("connection stats %+v", cs)
	}
	c.lock()
	if len(shaped) == 0 || shaped[0].Variant != "b" {
		c.unlock()
		t.Fatalf("shaped %+v", shaped)
	}
	c.unlock()
	var chosen bool
	for !chosen {
		ev, err := c.NextEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		chosen = ev.Kind == EventCarrierChosen && ev.Variant == "b"
	}
	if _, err := DialMode(ctx, AutoMode(working, working), cfg, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate kind and variant: %v", err)
	}
}
