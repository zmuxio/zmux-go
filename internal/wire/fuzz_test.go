package wire

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
)

// FuzzParsePreamble checks that parsing stays inside its input and limit,
// and that what it accepts survives a re-encoding: the encoder's output
// parses to the same fields.
func FuzzParsePreamble(f *testing.F) {
	for _, s := range []PreambleSpec{
		{},
		{HasOpenInfo: true, OpenInfo: []byte("info")},
		{HasPriority: true, Urgency: 7, Incr: true},
		{PipeCarrier: true},
		{HasOpenInfo: true, HasPriority: true, PipeCarrier: true, App: []TLV{{TLVAppFirst, []byte{1, 2}}}},
		{App: []TLV{{TLVAppFirst, nil}, {TLVAppFirst + 1, bytes.Repeat([]byte{9}, 70)}}, Grease: &TLV{0x21, []byte{0}}},
	} {
		f.Add(AppendPreamble(nil, &s), uint64(1<<16))
	}
	f.Add([]byte{0x05, 0x01}, uint64(8))
	f.Fuzz(func(t *testing.T, b []byte, limit uint64) {
		var p Preamble
		if ParsePreamble(b, limit, &p) != nil {
			return
		}
		if p.Size > len(b) || uint64(p.Size) > limit {
			t.Fatalf("size %d of %d bytes, limit %d", p.Size, len(b), limit)
		}
		s := PreambleSpec{OpenInfo: p.OpenInfo, App: p.App, HasOpenInfo: p.HasOpenInfo,
			HasPriority: p.HasPriority, Urgency: p.Urgency, Incr: p.Incr, PipeCarrier: p.PipeCarrier}
		enc := AppendPreamble(nil, &s)
		if len(enc) != s.Size() {
			t.Fatalf("encoded %d bytes, Size %d", len(enc), s.Size())
		}
		var q Preamble
		if err := ParsePreamble(enc, uint64(len(enc)), &q); err != nil {
			t.Fatalf("re-encoded preamble fails: %v", err)
		}
		if q.Size != len(enc) {
			t.Fatalf("re-encoded size %d of %d bytes", q.Size, len(enc))
		}
		// Empty and absent byte strings are the same field.
		for _, x := range []*Preamble{&p, &q} {
			x.Size = 0
			if len(x.OpenInfo) == 0 {
				x.OpenInfo = nil
			}
			if len(x.App) == 0 {
				x.App = nil
			}
			for i := range x.App {
				if len(x.App[i].Value) == 0 {
					x.App[i].Value = nil
				}
			}
		}
		if !reflect.DeepEqual(p, q) {
			t.Fatalf("preamble changed by re-encoding: %+v, %+v", p, q)
		}
	})
}

// FuzzDecodeParams checks that a parameter block that decodes encodes to
// one that decodes again, and that the encoding is then stable.
func FuzzDecodeParams(f *testing.F) {
	for _, e := range loadCorpus(f, "valid.ndjson") {
		var fr Frame
		if b, err := hex.DecodeString(e.Hex); err == nil {
			if _, werr := DecodeFrame(b, &fr); werr == nil && fr.Kind == KindTransportParams {
				f.Add(fr.Data)
			}
		}
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var p, q Params
		if DecodeParams(b, &p) != nil {
			return
		}
		enc, err := AppendParams(nil, &p)
		if err != nil {
			t.Fatalf("encoding decoded params: %v", err)
		}
		if err := DecodeParams(enc, &q); err != nil {
			t.Fatalf("re-encoded params fail: %v", err)
		}
		enc2, err := AppendParams(nil, &q)
		if err != nil || !bytes.Equal(enc, enc2) {
			t.Fatalf("encoding is not stable: % x, % x (%v)", enc, enc2, err)
		}
	})
}
