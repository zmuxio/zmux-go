package zmux

import (
	"encoding/json"
	"testing"
	"time"
)

// TestCarrierCacheRestoreBoundsTimes: a restored good time is at most
// SuccessTTL ahead and a fail time at most BackoffMax ahead.
func TestCarrierCacheRestoreBoundsTimes(t *testing.T) {
	far := formatCacheTime(time.Now().Add(10 * 365 * 24 * time.Hour))
	cc := &CarrierCache{}
	if err := cc.Restore([]byte(`{"version":1,"entries":[{"key":"g","kind":"split","good_until":"` + far +
		`"},{"key":"f","kind":"split","fail_until":"` + far + `","backoff_ms":60000}]}`)); err != nil {
		t.Fatal(err)
	}
	data, _ := cc.Snapshot()
	var snap cacheSnapshot
	if err := json.Unmarshal(data, &snap); err != nil || len(snap.Entries) != 2 {
		t.Fatalf("%s: %v", data, err)
	}
	limit := time.Now().Add(6 * time.Hour)
	for _, e := range snap.Entries {
		g, _ := parseCacheTime(e.GoodUntil)
		f, _ := parseCacheTime(e.FailUntil)
		if g.After(limit) || f.After(limit) {
			t.Fatalf("%+v beyond its bound", e)
		}
	}
}
