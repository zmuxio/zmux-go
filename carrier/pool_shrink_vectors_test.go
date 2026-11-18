package carrier

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/poolhost"
)

// TestPoolShrinkVectors runs the shrink order cases shared by every
// implementation (testdata/pool-shrink/vectors.json), with the growth
// dials of the case.
func TestPoolShrinkVectors(t *testing.T) {
	raw, err := os.ReadFile("../testdata/pool-shrink/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Cases []struct {
			Name    string `json:"name"`
			Members []struct {
				Key   uint64 `json:"key"`
				Idle  int64  `json:"idle"`
				Pipes int    `json:"pipes"`
				Age   int64  `json:"age"`
			} `json:"members"`
			Dialing  int `json:"dialing"`
			Decision struct {
				Count  int      `json:"count"`
				Retire []uint64 `json:"retire"`
			} `json:"decision"`
			Expect struct {
				CancelDials []int    `json:"cancel_dials"`
				Close       []uint64 `json:"close"`
			} `json:"expect"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range v.Cases {
		t.Run(c.Name, func(t *testing.T) {
			// Named connections retire first and no longer count.
			var named []uint64
			var ms []poolhost.Member
			for _, m := range c.Members {
				if slices.Contains(c.Decision.Retire, m.Key) {
					named = append(named, m.Key)
					continue
				}
				ms = append(ms, poolhost.Member{Key: m.Key, Idle: time.Duration(m.Idle), Pipes: m.Pipes, Age: time.Duration(m.Age)})
			}
			cancel, retire := poolhost.Plan(c.Dialing, ms, c.Decision.Count)
			// The dials are numbered from the oldest (0); the newest go first.
			var gotCancel []int
			for i := range cancel {
				gotCancel = append(gotCancel, c.Dialing-1-i)
			}
			gotClose := named
			for _, i := range retire {
				gotClose = append(gotClose, ms[i].Key)
			}
			if !slices.Equal(gotCancel, c.Expect.CancelDials) || !slices.Equal(gotClose, c.Expect.Close) {
				t.Fatalf("cancelled %v, closed %v; want %v, %v", gotCancel, gotClose, c.Expect.CancelDials, c.Expect.Close)
			}
		})
	}
}
