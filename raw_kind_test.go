package zmux

import (
	"context"
	"strings"
	"testing"
)

// A raw byte connection (TCP, TLS, a NetPipe) is a carrier of its own kind:
// the controller takes such a candidate, and the cache keys and exports it
// as "raw".
func TestRawCarrierKind(t *testing.T) {
	if p := NetPipe(nil); p.Carrier != CarrierRaw || p.Carrier.String() != "raw" {
		t.Fatalf("NetPipe carrier %v", p.Carrier)
	}
	cache := testCarrierCache(t, CarrierCacheOptions{})
	ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), Cache: cache, Candidates: []Candidate{{
		ID: "raw", CacheKey: "raw", Kind: CarrierRaw,
		Open: func(context.Context, OpenRequest) ([]PipeIO, error) { return nil, nil },
	}}})
	if err != nil {
		t.Fatal(err)
	}
	_ = ctl.Close()
	<-ctl.Done()
	if err := cache.update(CarrierCacheKey{Key: "raw", Kind: CarrierRaw}, CacheUsable, CarrierObservation{}); err != nil {
		t.Fatal(err)
	}
	data, err := cache.Export()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"kind":"raw"`) {
		t.Fatalf("export %s", data)
	}
	other := testCarrierCache(t, CarrierCacheOptions{})
	if err := other.Import(data); err != nil {
		t.Fatal(err)
	}
	if other.Score("raw", CarrierRaw, "").Attempts == 0 {
		t.Fatal("the raw entry did not survive the import")
	}
}
