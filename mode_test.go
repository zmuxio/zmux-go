package zmux

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// TestModeBookkeepingBounded keeps the latest carrier tries.
func TestModeBookkeepingBounded(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, nil)
	var tries []CandidateResult
	for i := range 100 {
		tries = append(tries, CandidateResult{Kind: CarrierKind(i)})
	}
	c.reportCarrier(Candidate{}, tries, false)
	got := c.ConnectionStats().CarrierCandidates
	if len(got) != carrierTriesKept || got[0].Kind != CarrierKind(100-carrierTriesKept) || got[len(got)-1].Kind != 99 {
		t.Fatalf("%d tries kept, from %v", len(got), got[0].Kind)
	}
}

// TestCarrierCacheSnapshot restores a snapshot in a new cache (a restart):
// the entries round-trip, expired ones are dropped, unknown fields and
// kinds are ignored, entries merge newer first, and a bad version, type
// or backoff rejects the whole snapshot.
func TestCarrierCacheSnapshot(t *testing.T) {
	cc := &CarrierCache{}
	now := time.Now()
	st := targetCandidate(CarrierStream, "https://a.example/z")
	sp := targetCandidate(CarrierSplit, "https://a.example/z")
	cc.record("k", st, false, now)
	cc.record("k", st, false, now) // backoff 2 min
	st.Target.Redirected("https://b.example/z")
	cc.record("k2", st, true, now)
	cc.record("old", sp, false, now.Add(-7*time.Hour)) // expired: past the longest backoff
	data, err := cc.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil || raw["version"] != 1.0 || len(raw["entries"].([]any)) != 2 {
		t.Fatalf("snapshot %s", data)
	}
	re := &CarrierCache{}
	if err := re.Restore(data); err != nil {
		t.Fatal(err)
	}
	if _, until := re.state("k", Candidate{Kind: CarrierStream}, now); until.Sub(now.Add(2*time.Minute)).Abs() > time.Millisecond {
		t.Fatalf("restored backoff until %v", until)
	}
	if good, _ := re.state("k2", Candidate{Kind: CarrierStream}, now); !good {
		t.Fatal("usable entry not restored")
	}
	if _, ok := re.m[cacheKey{"old", CarrierSplit, ""}]; ok {
		t.Fatal("expired entry restored")
	}
	if e := re.m[cacheKey{"k", CarrierStream, ""}]; e.backoff != 2*time.Minute {
		t.Fatalf("backoff %v", e.backoff)
	}
	fresh := targetCandidate(CarrierStream, "https://a.example/z")
	re.prime("k2", fresh)
	if fresh.Target.URL() != "https://b.example/z" {
		t.Fatalf("restored URL %q", fresh.Target.URL())
	}
	// The restored backoff doubles on the next failure.
	re.record("k", st, false, now)
	if e := re.m[cacheKey{"k", CarrierStream, ""}]; e.backoff != 4*time.Minute {
		t.Fatalf("backoff after restore %v", e.backoff)
	}

	future := now.Add(time.Hour).UTC().Format(time.RFC3339Nano)
	later := now.Add(2 * time.Hour).UTC().Format(time.RFC3339Nano)
	past := now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	longAgo := now.Add(-7 * time.Hour).UTC().Format(time.RFC3339Nano)
	lc := &CarrierCache{}
	lenient := `{"version":1,"extra":true,"entries":[` +
		`{"key":"k","kind":"split","good_until":"` + future + `","fail_until":"","new_field":3},` +
		`{"key":"k","kind":"carrier-of-the-future","good_until":"` + future + `"},` +
		`{"key":"k","kind":"exchange","fail_until":"` + past + `","backoff_ms":60000},` + // backoff history kept
		`{"key":"k","kind":"quic","fail_until":"` + longAgo + `","backoff_ms":60000},` + // expired
		`{"key":"d","kind":"quic","fail_until":"` + later + `","backoff_ms":1},` + // newer, backoff to 1 min
		`{"key":"d","kind":"quic","fail_until":"` + future + `","backoff_ms":999999999999}]}`
	if err := lc.Restore([]byte(lenient)); err != nil {
		t.Fatal(err)
	}
	if len(lc.m) != 3 {
		t.Fatalf("restored %d entries", len(lc.m))
	}
	if good, _ := lc.state("k", Candidate{Kind: CarrierSplit}, now); !good {
		t.Fatal("split not restored")
	}
	if e := lc.m[cacheKey{"k", CarrierExchange, ""}]; e == nil || e.backoff != time.Minute {
		t.Fatal("backoff history not restored")
	}
	if e := lc.m[cacheKey{"d", CarrierQUIC, ""}]; e == nil || e.backoff != time.Minute {
		t.Fatalf("duplicate entry %+v", e)
	}
	// Merge: an older entry does not replace a newer one, a newer does.
	older := `{"version":1,"entries":[{"key":"k","kind":"split","fail_until":"` + future + `","backoff_ms":60000},` +
		`{"key":"d","kind":"quic","good_until":"` + later + `"},{"key":"n","kind":"websocket","good_until":"` + future + `"}]}`
	if err := lc.Restore([]byte(older)); err != nil {
		t.Fatal(err)
	}
	if good, _ := lc.state("k", Candidate{Kind: CarrierSplit}, now); !good {
		t.Fatal("an older entry replaced a newer one")
	}
	if good, _ := lc.state("n", Candidate{Kind: CarrierWebSocket}, now); !good || len(lc.m) != 4 {
		t.Fatal("new entry not merged")
	}
	newer := `{"version":1,"entries":[{"key":"k","kind":"split","fail_until":"` + later + `","backoff_ms":60000}]}`
	if err := lc.Restore([]byte(newer)); err != nil {
		t.Fatal(err)
	}
	if good, _ := lc.state("k", Candidate{Kind: CarrierSplit}, now); good {
		t.Fatal("a newer entry did not replace an older one")
	}
	for _, bad := range []string{`{"version":2,"entries":[]}`, `{"entries":[]}`, `not json`,
		`{"version":1,"entries":[{"key":"z","kind":"split","good_until":"` + future + `"},{"key":"k","kind":"split","backoff_ms":-1}]}`,
		`{"version":1,"entries":[{"key":"z","kind":"split","backoff_ms":1.5}]}`,
		`{"version":1,"entries":[{"key":"z","kind":"split","good_until":3}]}`,
		`{"version":1,"entries":[{"key":"z","kind":"split","good_until":"tomorrow"}]}`,
		`{"version":"1","entries":[]}`} {
		err := lc.Restore([]byte(bad))
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("restored %s: %v", bad, err)
		}
	}
	if len(lc.m) != 4 || lc.m[cacheKey{"z", CarrierSplit, ""}] != nil {
		t.Fatal("a rejected snapshot changed the cache")
	}
}

// testTarget is a CarrierTarget for the cache's own tests.
type testTarget struct{ base, cur string }

func (t *testTarget) URL() string {
	if t.cur != "" {
		return t.cur
	}
	return t.base
}
func (t *testTarget) Base() string { return t.base }
func (t *testTarget) Redirected(u string) {
	if u == t.base {
		u = ""
	}
	t.cur = u
}
func (t *testTarget) Reset() { t.cur = "" }

// targetCandidate is a candidate of kind dialed at url, for the cache.
func targetCandidate(kind CarrierKind, url string) Candidate {
	return Candidate{Kind: kind, Target: &testTarget{base: url}}
}
