package bundle

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/shaping/machine"
)

type vectorSource struct {
	state, n uint64
}

func (s *vectorSource) Uint64() uint64 {
	s.n++
	s.state += 0x9E3779B97F4A7C15
	z := s.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func TestFormatVectors(t *testing.T) {
	b, err := os.ReadFile("../../testdata/shaping/format.json")
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		Bundles []struct {
			Name     string          `json:"name"`
			Bundle   string          `json:"bundle"`
			Features []string        `json:"features"`
			Allow    bool            `json:"allow_unshaped"`
			Selected map[string]*int `json:"selected"`
			Unshaped []string        `json:"unshaped"`
			Error    string          `json:"error"`
		} `json:"bundles"`
		Handshake []struct {
			Name   string `json:"name"`
			Bundle string `json:"bundle"`
			Kind   string `json:"kind"`
			Source struct {
				Seed string `json:"seed"`
			} `json:"source"`
			Resolved string `json:"resolved"`
			Calls    []struct {
				Handshake string `json:"handshake"`
				Message   string `json:"message"`
				Plan      *struct {
					PadTo       int   `json:"pad_to"`
					PadMultiple int   `json:"pad_multiple"`
					Pad         int   `json:"pad"`
					Delay       int64 `json:"delay"`
				} `json:"plan"`
				Consumed uint64 `json:"consumed"`
			} `json:"calls"`
		} `json:"handshake"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	for _, c := range d.Bundles {
		bu, err := Parse([]byte(c.Bundle), ParseOptions{Features: c.Features, AllowUnshaped: c.Allow})
		got := ""
		if fe, ok := errors.AsType[*machine.FormatError](err); ok {
			got = fe.Class
		} else if err != nil {
			got = err.Error()
		}
		if got != c.Error {
			t.Errorf("%s: %q, want %q", c.Name, got, c.Error)
			continue
		}
		if err != nil {
			continue
		}
		sel := map[string]*int{}
		for _, n := range bu.names {
			e := bu.def
			if n != DefaultKind {
				e = bu.byKind[kindByName[n]]
			}
			i := e.Index
			sel[n] = &i
		}
		for _, n := range bu.unshaped {
			sel[n] = nil
		}
		if !reflect.DeepEqual(sel, c.Selected) {
			t.Errorf("%s: selected differs: %v", c.Name, bu.names)
		}
		un := bu.Unshaped()
		slices.Sort(un)
		w := slices.Clone(c.Unshaped)
		slices.Sort(w)
		if !slices.Equal(un, w) {
			t.Errorf("%s: unshaped %v, want %v", c.Name, un, w)
		}
	}
	for _, c := range d.Handshake {
		seed, err := strconv.ParseUint(c.Source.Seed, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		src := &vectorSource{state: seed}
		bu, err := Parse([]byte(c.Bundle), ParseOptions{})
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		e, resolved := bu.def, DefaultKind
		if k, ok := kindByName[c.Kind]; ok && bu.byKind[k] != nil {
			e, resolved = bu.byKind[k], c.Kind
		}
		if resolved != c.Resolved {
			t.Errorf("%s: resolved %s, want %s", c.Name, resolved, c.Resolved)
			continue
		}
		h := e.Candidate.Compile(machine.Options{NewSource: func() rand.Source { return src }}).Handshake()
		for i, call := range c.Calls {
			var u shaping.UnitPlan
			if h != nil {
				h(&shaping.HandshakeView{Kind: c.Kind, Handshake: call.Handshake, Message: call.Message}, &u)
			}
			var w shaping.UnitPlan
			if call.Plan != nil {
				w = shaping.UnitPlan{PadTo: call.Plan.PadTo, PadMultiple: call.Plan.PadMultiple, Pad: call.Plan.Pad}
				w.Delay = micros(call.Plan.Delay)
			}
			if u != w || src.n != call.Consumed {
				t.Errorf("%s call %d: %+v consumed %d, want %+v %d", c.Name, i, u, src.n, w, call.Consumed)
			}
		}
	}
}

func micros(x int64) time.Duration { return time.Duration(x) * time.Microsecond }
