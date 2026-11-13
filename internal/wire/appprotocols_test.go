package wire

import (
	"bytes"
	"strings"
	"testing"

	"github.com/zmuxio/zmux-go/varint"
)

// TestAppProtocolsVarintLength round-trips names of 64 bytes and more,
// whose length prefix takes two varint bytes.
func TestAppProtocolsVarintLength(t *testing.T) {
	long := []byte(strings.Repeat("a", 300))
	in := Params{Has: HasAppProtocols, AppProtocols: [][]byte{[]byte("x"), make([]byte, 64), long}}
	b, err := AppendParams(nil, &in)
	if err != nil {
		t.Fatal(err)
	}
	var out Params
	if err := DecodeParams(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Has&HasAppProtocols == 0 || len(out.AppProtocols) != 3 || !bytes.Equal(out.AppProtocols[2], long) ||
		len(out.AppProtocols[1]) != 64 {
		t.Fatalf("%+v", out.AppProtocols)
	}
}

// TestAppProtocolsMalformedIgnored: a malformed value is ignored like an
// unknown parameter, not a TRANSPORT_PARAMETER_ERROR.
func TestAppProtocolsMalformedIgnored(t *testing.T) {
	for _, v := range [][]byte{
		{},           // no name
		{5, 'a'},     // name past the end
		{0x40},       // truncated varint
		{0x41, 0x00}, // two-byte length 256 past the end
	} {
		b := varint.AppendVarint(nil, ParamAppProtocols)
		b = varint.AppendVarint(b, uint64(len(v)))
		b = append(b, v...)
		var p Params
		if err := DecodeParams(b, &p); err != nil {
			t.Fatalf("%x: %v", v, err)
		}
		if p.Has&HasAppProtocols != 0 || p.AppProtocols != nil {
			t.Fatalf("%x: kept %+v", v, p.AppProtocols)
		}
	}
}
