// Package bundle reads shaping configuration documents.
//
// A bundle is one JSON document holding, per carrier kind, a list of
// candidates; the first whose requires are all known is chosen. A candidate
// holds handshake plans, record machines, session limits and a pool policy
// (package machine). Only what is chosen is looked into, so a document may
// hold candidates for readers that know more.
//
// This package takes bytes and does no I/O.
package bundle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping/machine"
)

// DefaultMaxBytes bounds a bundle when ParseOptions.MaxBytes is 0.
const DefaultMaxBytes = 1 << 20

// DefaultKind is the member of a bundle's kinds that applies to a carrier
// kind the bundle does not list, and to pipes of no listed kind (raw TCP
// or TLS, zmux.ShapedPipe.Carrier 0).
const DefaultKind = "default"

// kindNames are the carrier kind names a bundle may list besides
// DefaultKind.
var kindNames = [...]string{"websocket", "streaming", "split", "exchange", "quic", "http3", "webtransport"}

// kindByName maps each of kindNames to its zmux.CarrierKind.
var kindByName = func() map[string]zmux.CarrierKind {
	m := make(map[string]zmux.CarrierKind, len(kindNames))
	for k := 1; k < 256; k++ {
		m[zmux.CarrierKind(k).String()] = zmux.CarrierKind(k)
	}
	out := make(map[string]zmux.CarrierKind, len(kindNames))
	for _, n := range kindNames {
		if k, ok := m[n]; ok {
			out[n] = k
		}
	}
	return out
}()

// KindName returns the name under which a bundle lists k, or DefaultKind
// for a kind a bundle cannot list (0 included).
func KindName(k zmux.CarrierKind) string {
	for n, v := range kindByName {
		if v == k {
			return n
		}
	}
	return DefaultKind
}

// ParseOptions bound a bundle and say what its reader knows. The zero value is
// valid.
type ParseOptions struct {
	// MaxBytes bounds the bundle's bytes; 0 is DefaultMaxBytes and a
	// negative value is no bound.
	MaxBytes int
	// Size bounds each chosen candidate. Zero values: unbounded.
	Size machine.SizeLimits
	// Features are the names a candidate's requires may hold. A candidate
	// that requires another is skipped.
	Features []string
	// AllowUnshaped accepts a kind none of whose candidates can be chosen:
	// it has no entry, and Bundle.Unshaped reports it. Otherwise that fails
	// the bundle.
	AllowUnshaped bool
}

// ByteLimit returns the bound on a bundle's bytes, for a caller reading one
// from a file or a network; -1: none.
func (o ParseOptions) ByteLimit() int {
	switch {
	case o.MaxBytes == 0:
		return DefaultMaxBytes
	case o.MaxBytes < 0:
		return -1
	}
	return o.MaxBytes
}

// Entry is the candidate chosen for one kind.
type Entry struct {
	// Index is the candidate's place in the kind's list.
	Index     int
	Candidate *machine.Candidate
}

// Bundle is a parsed and validated bundle. It is immutable and safe for
// concurrent use.
type Bundle struct {
	raw         []byte
	generatedAt time.Time
	nextChange  time.Time
	byKind      map[zmux.CarrierKind]*Entry
	def         *Entry
	names       []string
	unshaped    []string
}

// Unshaped returns the kinds without an entry under ParseOptions.AllowUnshaped.
func (b *Bundle) Unshaped() []string { return append([]string(nil), b.unshaped...) }

// GeneratedAt returns the bundle's generated_at.
func (b *Bundle) GeneratedAt() time.Time { return b.generatedAt }

// NextChange returns the bundle's next_change; zero: no planned change.
func (b *Bundle) NextChange() time.Time { return b.nextChange }

// Raw returns a copy of the bytes the bundle was parsed from.
func (b *Bundle) Raw() []byte { return bytes.Clone(b.raw) }

// Kinds returns the names of the kinds with an entry, in a fixed order.
func (b *Bundle) Kinds() []string { return append([]string(nil), b.names...) }

// Lookup returns the entry the bundle lists for k, and whether it lists
// one, without falling back to DefaultKind.
func (b *Bundle) Lookup(k zmux.CarrierKind) (*Entry, bool) {
	e, ok := b.byKind[k]
	return e, ok
}

// Default returns the bundle's DefaultKind entry, and whether it lists
// one.
func (b *Bundle) Default() (*Entry, bool) { return b.def, b.def != nil }

// Entry returns the entry for k: the one the bundle lists for k, otherwise
// its DefaultKind entry; nil if neither is listed.
func (b *Bundle) Entry(k zmux.CarrierKind) *Entry {
	if e, ok := b.byKind[k]; ok {
		return e
	}
	return b.def
}

// Parse parses and validates a bundle. The whole document must be valid JSON
// without a repeated name in an object. An unknown kind, and every candidate
// but the chosen one, are not looked into; an error in a chosen candidate fails
// the bundle. Errors are *machine.FormatError where the format is at fault.
// Parse keeps a copy of b.
func Parse(b []byte, o ParseOptions) (*Bundle, error) {
	if limit := o.ByteLimit(); limit >= 0 && len(b) > limit {
		return nil, fmt.Errorf("bundle: %d bytes, limit %d", len(b), limit)
	}
	if !utf8.Valid(b) {
		return nil, &machine.FormatError{Class: "syntax", Msg: "not UTF-8"}
	}
	if err := machine.CheckSyntax(b); err != nil {
		return nil, err
	}
	bad := func(class, format string, a ...any) error {
		return &machine.FormatError{Class: class, Msg: fmt.Sprintf(format, a...)}
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil || top == nil {
		return nil, bad("type", "bundle")
	}
	bu := &Bundle{raw: bytes.Clone(b), byKind: map[zmux.CarrierKind]*Entry{}}
	for k, v := range top {
		switch k {
		case "generated_at", "next_change":
			if string(v) == "null" {
				continue
			}
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, bad("type", "%s", k)
			}
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				t = time.Time{} // the format asks for a string; a Fetcher compares only times it reads
			}
			if k == "generated_at" {
				bu.generatedAt = t
			} else {
				bu.nextChange = t
			}
		case "tables", "kinds":
		default:
			return nil, bad("unknown_member", "%s", k)
		}
	}
	var tables map[string]json.RawMessage
	if v, ok := top["tables"]; ok && string(v) != "null" {
		if err := json.Unmarshal(v, &tables); err != nil {
			return nil, bad("type", "tables")
		}
	}
	v, ok := top["kinds"]
	var kinds map[string]json.RawMessage
	if !ok || string(v) == "null" || json.Unmarshal(v, &kinds) != nil {
		return nil, bad("type", "kinds")
	}
	if d, ok := kinds[DefaultKind]; !ok || string(d) == "null" {
		return nil, bad("no_default", "kinds.default")
	}
	for _, name := range append([]string{DefaultKind}, kindNames[:]...) {
		raw, ok := kinds[name]
		if !ok || string(raw) == "null" {
			continue
		}
		e, err := chooseCandidate(raw, tables, &o)
		if err != nil {
			return nil, err
		}
		if e == nil {
			if !o.AllowUnshaped {
				return nil, bad("no_candidate", "%s", name)
			}
			bu.unshaped = append(bu.unshaped, name)
			continue
		}
		bu.names = append(bu.names, name)
		if name == DefaultKind {
			bu.def = e
		} else {
			bu.byKind[kindByName[name]] = e
		}
	}
	return bu, nil
}

// chooseCandidate parses the first candidate whose requires are all known; nil
// when there is none.
func chooseCandidate(raw json.RawMessage, tables map[string]json.RawMessage, o *ParseOptions) (*Entry, error) {
	bad := func(what string) error { return &machine.FormatError{Class: "type", Msg: what} }
	var cands []json.RawMessage
	if json.Unmarshal(raw, &cands) != nil || cands == nil {
		return nil, bad("candidates")
	}
next:
	for i, c := range cands {
		var m map[string]json.RawMessage
		if json.Unmarshal(c, &m) != nil || m == nil {
			return nil, bad("candidate")
		}
		if r, ok := m["requires"]; ok && string(r) != "null" {
			var req []json.RawMessage
			if json.Unmarshal(r, &req) != nil || req == nil {
				return nil, bad("requires")
			}
			known := true
			for _, x := range req {
				var s *string
				if json.Unmarshal(x, &s) != nil || s == nil {
					return nil, bad("requires")
				}
				known = known && slices.Contains(o.Features, *s)
			}
			if !known {
				continue next
			}
		}
		cand, err := machine.ParseCandidate(c, tables, o.Size)
		if err != nil {
			return nil, err
		}
		return &Entry{Index: i, Candidate: cand}, nil
	}
	return nil, nil
}
