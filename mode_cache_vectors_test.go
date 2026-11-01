package zmux

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"
)

// TestCarrierCacheVectors runs the carrier cache snapshot cases shared by
// every implementation (testdata/carrier-cache/vectors.json).
func TestCarrierCacheVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/carrier-cache/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	type expect struct {
		Key       string `json:"key"`
		Kind      string `json:"kind"`
		Usable    bool   `json:"usable"`
		Failed    bool   `json:"failed"`
		BackoffMS int64  `json:"backoff_ms"`
		URL       string `json:"url"`
	}
	var v struct {
		Cases []struct {
			Name  string `json:"name"`
			Steps []struct {
				Snapshot string `json:"snapshot"`
				Error    bool   `json:"error"`
			} `json:"steps"`
			Expect []expect `json:"expect"`
		} `json:"cases"`
		Exact []string `json:"exact"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	rel := regexp.MustCompile(`@([+-][0-9]+)`)
	for _, c := range v.Cases {
		t.Run(c.Name, func(t *testing.T) {
			now := time.Now()
			cc := &CarrierCache{}
			for i, st := range c.Steps {
				snap := rel.ReplaceAllStringFunc(st.Snapshot, func(m string) string {
					n, _ := strconv.Atoi(m[1:])
					return now.Add(time.Duration(n) * time.Second).UTC().Format(time.RFC3339Nano)
				})
				before, _ := cc.Snapshot()
				err := cc.Restore([]byte(snap))
				if st.Error {
					after, _ := cc.Snapshot()
					if !errors.Is(err, ErrInvalidConfig) || string(before) != string(after) {
						t.Fatalf("step %d: %v, cache changed %v", i, err, string(before) != string(after))
					}
				} else if err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
			}
			data, err := cc.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			var got cacheSnapshot
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			at := time.Now()
			var es []expect
			for _, e := range got.Entries {
				g, _ := parseCacheTime(e.GoodUntil)
				f, _ := parseCacheTime(e.FailUntil)
				es = append(es, expect{e.Key, e.Kind, at.Before(g), at.Before(f), e.BackoffMS, e.URL})
			}
			if !slices.Equal(es, c.Expect) {
				t.Fatalf("snapshot %s\n got %+v\nwant %+v", data, es, c.Expect)
			}
		})
	}
	for _, x := range v.Exact {
		cc := &CarrierCache{}
		if err := cc.Restore([]byte(x)); err != nil {
			t.Fatalf("%s: %v", x, err)
		}
		if data, _ := cc.Snapshot(); string(data) != x {
			t.Fatalf("round trip\n got %s\nwant %s", data, x)
		}
	}
}
