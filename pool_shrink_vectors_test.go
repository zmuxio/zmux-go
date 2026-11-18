package zmux

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/poolhost"
)

// TestPoolShrinkVectors runs the shrink order cases of testdata/pool-shrink
// shared by every implementation against a PipePool's order of closing. A
// PipePool runs its dials inside its looks and reports no growth dial, so
// only the cases without one apply.
func TestPoolShrinkVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/pool-shrink/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Cases []struct {
			Name    string `json:"name"`
			Members []struct {
				Key   PipeID `json:"key"`
				Idle  int64  `json:"idle"`
				Pipes int    `json:"pipes"`
				Age   int64  `json:"age"`
			} `json:"members"`
			Dialing  int `json:"dialing"`
			Decision struct {
				Count  int      `json:"count"`
				Retire []PipeID `json:"retire"`
			} `json:"decision"`
			Expect struct {
				Close []PipeID `json:"close"`
			} `json:"expect"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, c := range v.Cases {
		if c.Dialing != 0 {
			continue
		}
		ran++
		t.Run(c.Name, func(t *testing.T) {
			var ms []poolhost.Member
			for _, m := range c.Members {
				ms = append(ms, poolhost.Member{Retire: slices.Contains(c.Decision.Retire, m.Key), Idle: time.Duration(m.Idle),
					Pipes: m.Pipes, Age: time.Duration(m.Age), Key: uint64(m.Key)})
			}
			var closed []PipeID
			_, closing := poolhost.Plan(0, ms, c.Decision.Count)
			for _, i := range closing {
				closed = append(closed, PipeID(ms[i].Key))
			}
			if !slices.Equal(closed, c.Expect.Close) {
				t.Fatalf("closed %v, want %v", closed, c.Expect.Close)
			}
		})
	}
	if ran == 0 {
		t.Fatal("no case without growth dials")
	}
}
