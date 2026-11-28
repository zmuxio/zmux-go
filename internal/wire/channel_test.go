package wire

import (
	"bytes"
	"testing"
)

func TestChannelDatagramRoundTrip(t *testing.T) {
	for _, id := range []uint64{0, 4, 63, 64, 16383, 16384, 1 << 40} {
		for _, data := range [][]byte{nil, {0}, bytes.Repeat([]byte{9}, 1200)} {
			b := AppendChannelDatagram(nil, true, id, data)
			if len(b) != ChannelDatagramLen(true, id, len(data)) {
				t.Fatalf("length %d, want %d", len(b), ChannelDatagramLen(true, id, len(data)))
			}
			gid, frame, ok := SplitChannelDatagram(b)
			if !ok || gid != id {
				t.Fatalf("split: %d %v", gid, ok)
			}
			var f Frame
			if err := DecodeChannelFrame(frame, &f, false); err != nil {
				t.Fatal(err)
			}
			if f.Type != TypeDatagram || !bytes.Equal(f.Data, data) {
				t.Fatalf("frame %#x %d bytes", f.Type, len(f.Data))
			}
		}
	}
	// Without the Pipe Stream ID (HTTP/3): the frame alone.
	b := AppendChannelDatagram(nil, false, 99, []byte("x"))
	if !bytes.Equal(b, []byte{0x30, 'x'}) || ChannelDatagramLen(false, 99, 1) != 2 {
		t.Fatalf("%x", b)
	}
}

func TestChannelDatagramMalformed(t *testing.T) {
	if _, _, ok := SplitChannelDatagram(nil); ok {
		t.Fatal("empty datagram split")
	}
	if _, _, ok := SplitChannelDatagram([]byte{0x40}); ok {
		t.Fatal("truncated Pipe Stream ID split")
	}
	bad := map[string][]byte{
		"empty":            {},
		"with Length":      {0x31, 0x01, 'x'},
		"Length too short": {0x31, 0x00, 'x'},
		"other frame":      {0x01},
		"padding first":    {0x00, 0x30, 'x'},
		"non-minimal type": {0x40, 0x30, 'x'},
		"stream frame":     {0x08, 0x00, 'x'},
	}
	for name, b := range bad {
		var f Frame
		err := DecodeChannelFrame(b, &f, false)
		if err == nil || err.Code != ProtocolViolation {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestCarrierDatagramsParam(t *testing.T) {
	p := Params{Has: HasDatagramFamily | HasCarrierDatagrams | HasDatagram, MaxDatagramFrameSize: 65535, MaxRecordSize: DefaultMaxRecordSize}
	b, _ := AppendParams(nil, &p)
	var q Params
	if err := DecodeParams(b, &q); err != nil {
		t.Fatal(err)
	}
	if q.Has&HasCarrierDatagrams == 0 || !q.IsZmux() {
		t.Fatalf("has %#x", q.Has)
	}
	// An empty flag: a value is an error.
	b = appendParam(nil, ParamCarrierDatagrams, []byte{0})
	if err := DecodeParams(b, &q); err == nil {
		t.Fatal("non-empty zmux_carrier_datagrams accepted")
	}
}

func TestPreambleParamBound(t *testing.T) {
	ok := func(v uint64) *Error {
		p := Params{Has: HasDatagramFamily | HasPreamble | HasResetAt | HasBidiRemote | HasUni, Preamble: v, MaxRecordSize: DefaultMaxRecordSize,
			MaxStreamDataBidiRem: 1 << 20, MaxStreamDataUni: 1 << 20}
		b, _ := AppendParams(nil, &p)
		var q Params
		return DecodeParams(b, &q)
	}
	if err := ok(MaxPreambleSize); err != nil {
		t.Fatalf("the bound itself: %v", err)
	}
	if err := ok(MaxPreambleSize + 1); err == nil {
		t.Fatal("zmux_preamble above 8160 accepted")
	}
}

func TestPaddedChannelFraming(t *testing.T) {
	encoded := AppendPaddedChannelDatagram(nil, true, 0, []byte{0, 0, 'a', 0}, 2)
	id, body, ok := SplitChannelDatagram(encoded)
	if !ok || id != 0 {
		t.Fatalf("split id=%d frame=%x", id, body)
	}
	var f Frame
	if e := DecodeChannelFrame(body, &f, false); e == nil {
		t.Fatal("accepted padding without negotiation")
	}
	if e := DecodeChannelFrame(body, &f, true); e != nil || !bytes.Equal(f.Data, []byte{0, 0, 'a', 0}) {
		t.Fatalf("body %x error %v", f.Data, e)
	}
	oversized := make([]byte, MaxRecordSize+1)
	oversized[MaxRecordSize-1] = byte(TypeDatagram)
	valid := oversized[:MaxRecordSize]
	for _, tc := range []struct {
		b     []byte
		valid bool
	}{{valid, true}, {oversized, false}, {valid[:len(valid)-1], false}, {[]byte{0, 0x31}, false}} {
		if ok := DecodeChannelFrame(tc.b, &f, true) == nil; ok != tc.valid {
			t.Fatalf("decoded %d bytes: valid=%t, want %t", len(tc.b), ok, tc.valid)
		}
	}
	if ParamCarrierDatagramPadding != 0x2071d4ee {
		t.Fatal("carrier padding parameter does not match the registry")
	}
	block, err := AppendParams(nil, &Params{
		Has:                  HasDatagramFamily | HasDatagram | HasCarrierDatagrams | HasCarrierDatagramPadding,
		MaxRecordSize:        DefaultMaxRecordSize,
		MaxDatagramFrameSize: 1200,
	})
	if err != nil {
		t.Fatal(err)
	}
	var p Params
	e := DecodeParams(block, &p)
	if e != nil || p.Has&(HasCarrierDatagrams|HasCarrierDatagramPadding) != HasCarrierDatagrams|HasCarrierDatagramPadding {
		t.Fatalf("parameters %+v, error %v", p, e)
	}
}
