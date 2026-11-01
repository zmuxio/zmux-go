package machine

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// A length, delay, pause, rate, burst or padding that may be negative, or
// a bound that is not finite, fails the whole definition.
func TestDefinitionRejectsNegativeValues(t *testing.T) {
	for name, state := range map[string]string{
		"pad const":        `{"record": {"pad": {"const": -1}}}`,
		"max_len range":    `{"record": {"max_len": {"range": [-5, 10]}}}`,
		"pad_to max":       `{"record": {"pad_to": {"const": 5, "max": -1}}}`,
		"inject after":     `{"enter": [{"op": "inject", "after": {"const": -0.5}, "len": {"const": 100}}]}`,
		"inject len":       `{"enter": [{"op": "inject", "after": {"const": 1}, "len": {"const": -100}}]}`,
		"pause":            `{"enter": [{"op": "pause", "after": {"const": -1}}]}`,
		"rate":             `{"enter": [{"op": "rate", "rate": {"const": -1}}]}`,
		"burst":            `{"enter": [{"op": "rate", "rate": {"const": 1}, "burst": {"const": -1}}]}`,
		"timer":            `{"enter": [{"op": "timer", "after": {"const": -1}}]}`,
		"empirical points": `{"record": {"pad": {"dist": {"kind": "empirical", "points": [-3, 2], "weights": [1, 1]}}}}`,
		"normal no min":    `{"record": {"pad": {"dist": {"kind": "normal", "params": [1, 1]}}}}`,
		"uniform no min":   `{"enter": [{"op": "rate", "rate": {"dist": {"kind": "uniform", "params": [-1, 5]}}}]}`,
		"mixture no min":   `{"enter": [{"op": "pause", "after": {"dist": {"kind": "mixture", "of": [{"const": 1}, {"dist": {"kind": "normal", "params": [0, 1]}}], "weights": [1, 1]}}}]}`,
		"counter set":      `{"enter": [{"op": "set", "slot": 0, "value": {"const": -2}}]}`,
		"burst normal":     `{"enter": [{"op": "rate", "rate": {"const": 1}, "burst": {"dist": {"kind": "normal", "params": [5, 1]}}}]}`,
	} {
		js := `{"version": 1, "machines": [{"states": [` + state + `]}]}`
		if _, err := Parse([]byte(js)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A min that is not finite is caught on the Go side as well (JSON
	// carries no NaN).
	nan := func() *float64 { x := 0.0; x /= x; return &x }()
	one := 1.0
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{Pad: &Value{Const: &one, Min: nan}}}}}}}
	if err := d.Validate(); err == nil {
		t.Error("NaN min accepted")
	}
	// Weight values may still be negative, and a distribution that may
	// draw below 0 is accepted with a min of 0 or more.
	ok := `{"version": 1, "machines": [{"states": [{"record": {"pad": {"dist": {"kind": "normal", "params": [1, 1]}, "min": 0}},
		"enter": [{"op": "weight", "value": {"const": -2}},
		{"op": "rate", "rate": {"dist": {"kind": "normal", "params": [5, 1]}, "min": 1}}]}]}]}`
	if _, err := Parse([]byte(ok)); err != nil {
		t.Errorf("valid definition: %v", err)
	}
}

// The pool policy passes a probe interval below 1 s on as it is: the
// decision fails Validate (the pool rejects it) instead of being raised
// to 1 s.
func TestPoolProbeIntervalBelowOneRejected(t *testing.T) {
	for _, c := range []string{"0.2"} {
		p, err := PoolPolicy([]byte(`{"probe_interval": {"const": `+c+`}}`), Options{})
		if err != nil {
			t.Fatal(err)
		}
		var d zmux.PoolDecision
		p.Decide(&zmux.PoolView{Now: time.Now()}, &d)
		if err := d.Validate(); err == nil || !strings.Contains(err.Error(), "ProbeInterval") {
			t.Errorf("%s: decision %+v, Validate %v", c, d, err)
		}
	}
}

func TestSessionLimitVectors(t *testing.T) {
	b, err := os.ReadFile("../../testdata/shaping/session_limits.json")
	if err != nil {
		t.Fatal(err)
	}
	testSessionLimits(t, b)
}

// testSessionLimits runs session limit vectors: want holds the limits in
// seconds, null for no limit, 0 for none allowed.
func testSessionLimits(t *testing.T, b []byte) {
	var k struct {
		Cases []struct {
			Name   string
			Limits json.RawMessage
			Raw    string
			OK     bool
			Error  string
			Want   map[string]*float64
		}
	}
	if err := json.Unmarshal(b, &k); err != nil {
		t.Fatal(err)
	}
	if len(k.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range k.Cases {
		in := []byte(c.Raw)
		if c.Raw == "" {
			in = c.Limits
		}
		l, err := ParseLimits(in)
		if !c.OK {
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(c.Error)) {
				t.Errorf("%s: %v, want error %q", c.Name, err, c.Error)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.Name, err)
			continue
		}
		// What l allows, in the vector's terms: nil no limit.
		some := func(none, set bool, x float64) *float64 {
			switch {
			case none:
				return new(float64)
			case set:
				return &x
			}
			return nil
		}
		got := map[string]*float64{
			"max_padding_bytes": some(l.NoPadding, l.MaxPaddingBytes > 0, float64(l.MaxPaddingBytes)),
			"max_padding_ratio": some(l.NoPaddingRatio, l.MaxPaddingRatio > 0, l.MaxPaddingRatio),
			"max_injected":      some(l.NoInjection, l.MaxInjected > 0, float64(l.MaxInjected)),
			"max_pause_total":   some(l.NoPause && l.MaxPauseTotal == 0 && c.Want["max_pause_total"] != nil, l.MaxPauseTotal > 0, l.MaxPauseTotal.Seconds()),
			"max_delay":         some(l.NoPause && l.MaxDelay == 0 && c.Want["max_delay"] != nil, l.MaxDelay > 0, l.MaxDelay.Seconds()),
		}
		for f, w := range c.Want {
			if g := got[f]; (g == nil) != (w == nil) || g != nil && *g != *w {
				t.Errorf("%s: %s = %v, want %v (limits %+v)", c.Name, f, g, w, l)
			}
		}
		if w := c.Want; (w["max_padding_bytes"] != nil && *w["max_padding_bytes"] == 0) != l.NoPadding ||
			(w["max_padding_ratio"] != nil && *w["max_padding_ratio"] == 0) != l.NoPaddingRatio ||
			(w["max_pause_total"] != nil && *w["max_pause_total"] == 0 || w["max_delay"] != nil && *w["max_delay"] == 0) != l.NoPause {
			t.Errorf("%s: limits %+v", c.Name, l)
		}
	}
}
