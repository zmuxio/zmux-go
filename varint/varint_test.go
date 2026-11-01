package varint

import (
	"bytes"
	"testing"
)

func TestVarintBoundaries(t *testing.T) {
	cases := []struct {
		v   uint64
		enc []byte
	}{
		{0, []byte{0x00}},
		{63, []byte{0x3f}},
		{64, []byte{0x40, 0x40}},
		{16383, []byte{0x7f, 0xff}},
		{16384, []byte{0x80, 0x00, 0x40, 0x00}},
		{1<<30 - 1, []byte{0xbf, 0xff, 0xff, 0xff}},
		{1 << 30, []byte{0xc0, 0, 0, 0, 0x40, 0, 0, 0}},
		{MaxVarint, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		// RFC 9000 Appendix A.1 examples.
		{151288809941952652, []byte{0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c}},
		{494878333, []byte{0x9d, 0x7f, 0x3e, 0x7d}},
		{15293, []byte{0x7b, 0xbd}},
		{37, []byte{0x25}},
	}
	for _, c := range cases {
		if got := AppendVarint(nil, c.v); !bytes.Equal(got, c.enc) {
			t.Errorf("AppendVarint(%d) = %x, want %x", c.v, got, c.enc)
		}
		if l := SizeVarint(c.v); l != len(c.enc) {
			t.Errorf("SizeVarint(%d) = %d, want %d", c.v, l, len(c.enc))
		}
		v, n := ConsumeVarint(append(c.enc, 0xaa))
		if v != c.v || n != len(c.enc) {
			t.Errorf("ConsumeVarint(%x) = %d, %d", c.enc, v, n)
		}
		if _, n := ConsumeVarint(c.enc[:len(c.enc)-1]); n != 0 {
			t.Errorf("ConsumeVarint of truncated %x took %d bytes", c.enc, n)
		}
	}
}

func TestVarintNonShortest(t *testing.T) {
	// RFC 9000 Appendix A.1: 0x4025 is 37 in two bytes.
	if v, n := ConsumeVarint([]byte{0x40, 0x25}); v != 37 || n != 2 {
		t.Fatalf("ConsumeVarint(4025) = %d, %d", v, n)
	}
	for _, n := range []int{1, 2, 4, 8} {
		b := AppendVarintN(nil, 37, n)
		if len(b) != n {
			t.Fatalf("AppendVarintN(37, %d) is %d bytes", n, len(b))
		}
		if v, m := ConsumeVarint(b); v != 37 || m != n {
			t.Fatalf("round trip of AppendVarintN(37, %d) = %d, %d", n, v, m)
		}
	}
	if _, n := ConsumeVarint(nil); n != 0 {
		t.Fatal("ConsumeVarint(nil) took bytes")
	}
}
