package zmux

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/poolhost"
)

// TestPoolShrinkVectors checks physical pool close order against the
// shared cases in testdata/pool-shrink that have no growth dial.
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
			Expect struct {
				Close []PipeID `json:"close"`
			} `json:"expect"`
			Decision struct {
				Retire []PipeID `json:"retire"`
				Count  int      `json:"count"`
			} `json:"decision"`
			Dialing int `json:"dialing"`
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
