package bundle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping/machine"
)

// Synthetic fixtures written by hand from the format.
const (
	defA    = `[{"start":0,"states":[{"record":{"pad":32}}]}]`
	defB    = `[{"start":0,"states":[{"record":{"pad":{"t":[[1,0,64]]}}}]}]`
	session = `{"wire":{"initial":1000,"per_mb":100000},"max_delay":5000}`
	pool    = `{"count":2,"dial_after":[1000000,5000000],"dial_jitter":100000,` +
		`"retire":[{"metric":"age","above":600000000,"jitter":200000}],"probe_interval":15000000,"probe_jitter":250000}`
)

func bundleJSON(next string) string {
	n := "null"
	if next != "" {
		n = `"` + next + `"`
	}
	return fmt.Sprintf(`{"generated_at":"2026-01-01T00:00:00Z","next_change":%s,"kinds":{`+
		`"websocket":[{"machines":%s,"budget":%s,"pool":%s}],`+
		`"quic":[{"machines":%s}],`+
		`"split":[{}],`+
		`"default":[{"machines":%s,"budget":{"wire":{}}}]}}`, n, defA, session, pool, defB, defB)
}

func TestParseValid(t *testing.T) {
	b, err := Parse([]byte(bundleJSON("2026-01-02T00:00:00+01:00")), ParseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !b.GeneratedAt().Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("generated_at %v", b.GeneratedAt())
	}
	if !b.NextChange().Equal(time.Date(2026, 1, 1, 23, 0, 0, 0, time.UTC)) {
		t.Errorf("next_change %v", b.NextChange())
	}
	if got := strings.Join(b.Kinds(), ","); got != "default,websocket,split,quic" {
		t.Errorf("kinds %s", got)
	}
	ws := b.Entry(zmux.CarrierWebSocket)
	if ws == nil || !ws.Candidate.HasMachines() || !ws.Candidate.HasPool() {
		t.Fatalf("websocket entry %+v", ws)
	}
	if ws.Candidate.Compile(machine.Options{}).Budget() == nil {
		t.Errorf("no budget")
	}
	if e := b.Entry(zmux.CarrierSplit); e == nil || e.Candidate.HasMachines() || e.Candidate.HasPool() {
		t.Errorf("split entry %+v: a listed empty kind does not fall back", e)
	}
	def, _ := b.Default()
	if b.Entry(zmux.CarrierHTTP3) != def || b.Entry(0) != def || def == nil {
		t.Errorf("default fallback")
	}
	if def.Candidate.Compile(machine.Options{}).Budget() == nil {
		t.Errorf("default budget")
	}
	if _, ok := b.Lookup(zmux.CarrierHTTP3); ok {
		t.Errorf("Lookup falls back")
	}
	if !strings.Contains(string(b.Raw()), `"quic"`) {
		t.Errorf("raw")
	}
}

func TestKindNames(t *testing.T) {
	for _, n := range kindNames {
		k, ok := kindByName[n]
		if !ok || KindName(k) != n {
			t.Errorf("kind %s does not map to a zmux carrier kind", n)
		}
	}
	if KindName(0) != DefaultKind {
		t.Errorf("KindName(0)")
	}
}

func TestParseBounds(t *testing.T) {
	in := []byte(bundleJSON(""))
	if _, err := Parse(in, ParseOptions{MaxBytes: len(in) - 1}); err == nil {
		t.Error("MaxBytes not applied")
	}
	if _, err := Parse(in, ParseOptions{MaxBytes: -1}); err != nil {
		t.Error(err)
	}
	if _, err := Parse(in, ParseOptions{Size: machine.SizeLimits{MaxBytes: 10}}); err == nil {
		t.Error("candidate MaxBytes not applied")
	}
	big := `{"kinds":{"default":[{"pool":{"count":{"t":[[1,1,1],[1,2,2],[1,3,3]]}}}]}}`
	if _, err := Parse([]byte(big), ParseOptions{Size: machine.SizeLimits{MaxSegments: 2}}); err == nil {
		t.Error("MaxSegments not applied")
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(bundleJSON("2026-01-02T00:00:00Z")))
	f.Add([]byte(`{"generated_at":"2026-01-01T00:00:00Z","kinds":{"default":[{}]}}`))
	f.Add([]byte(`{"kinds":{"quic":null,"default":[{"budget":null}]}}`))
	f.Fuzz(func(t *testing.T, in []byte) {
		b, err := Parse(in, ParseOptions{})
		if err != nil {
			return
		}
		// A valid bundle parses again from its own bytes.
		if _, err := Parse(b.Raw(), ParseOptions{}); err != nil {
			t.Fatal(err)
		}
	})
}
