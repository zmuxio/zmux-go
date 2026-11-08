package zmux

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// TestReplayFilterVectors checks the shared vectors.
func TestReplayFilterVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/replay.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []struct {
			Name        string
			WindowMs    int64 `json:"window_ms"`
			Max         int
			NotBeforeMs int64 `json:"not_before_ms"`
			Steps       []struct {
				NowMs   int64  `json:"now_ms"`
				KeyHex  string `json:"key_hex"`
				TMs     int64  `json:"t_ms"`
				Verdict string
			}
		}
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		cfg := ReplayConfig{Window: time.Duration(c.WindowMs) * time.Millisecond, Max: c.Max}
		if c.NotBeforeMs != 0 {
			cfg.NotBefore = time.UnixMilli(c.NotBeforeMs)
		}
		f, err := NewReplayFilter(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i, s := range c.Steps {
			k, _ := hex.DecodeString(s.KeyHex)
			if v := f.checkAt(k, s.TMs, s.NowMs); v.String() != s.Verdict {
				t.Errorf("%s step %d: %v, want %s", c.Name, i, v, s.Verdict)
			}
		}
	}
	if len(doc.Cases) < 8 {
		t.Fatal("few cases")
	}
}

func TestReplayFilterConfig(t *testing.T) {
	for _, c := range []ReplayConfig{{}, {Window: time.Second}, {Max: 1}, {Window: time.Microsecond, Max: 1}} {
		if _, err := NewReplayFilter(c); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", c, err)
		}
	}
}

// Check uses the filter's clock, Len counts held keys, and concurrent
// checks of one key accept it once.
func TestReplayFilterClockConcurrent(t *testing.T) {
	f, err := NewReplayFilter(ReplayConfig{Window: time.Minute, Max: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	var wg sync.WaitGroup
	var mu sync.Mutex
	fresh := 0
	for range 16 {
		wg.Go(func() {
			if f.Check([]byte("k"), now) == ReplayFresh {
				mu.Lock()
				fresh++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if fresh != 1 || f.Len() != 1 {
		t.Fatalf("fresh %d len %d", fresh, f.Len())
	}
	if v := f.Check([]byte("x"), now-2*time.Minute.Milliseconds()); v != ReplayOutside {
		t.Fatalf("%v", v)
	}
}
