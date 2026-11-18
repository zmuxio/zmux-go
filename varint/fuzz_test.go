package varint

import (
	"bytes"
	"testing"
)

// FuzzConsumeVarint checks that decoding never reads past its length, that
// every value it returns re-encodes to the bytes it came from, and that the
// shortest encoding of the value decodes to the same value.
func FuzzConsumeVarint(f *testing.F) {
	for _, v := range []uint64{0, 1, 63, 64, 16383, 16384, 1<<30 - 1, 1 << 30, MaxVarint} {
		f.Add(AppendVarint(nil, v))
		f.Add(AppendVarintN(nil, v, 8))
	}
	f.Add([]byte{})
	f.Add([]byte{0x40})
	f.Add([]byte{0xc0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, n := ConsumeVarint(b)
		if n == 0 {
			if len(b) > 0 && len(b) >= 1<<(b[0]>>6) {
				t.Fatalf("% x: complete varint reported incomplete", b)
			}
			return
		}
		if n != 1<<(b[0]>>6) || n > len(b) || v > MaxVarint {
			t.Fatalf("% x: value %d, %d bytes", b, v, n)
		}
		if !bytes.Equal(AppendVarintN(nil, v, n), b[:n]) {
			t.Fatalf("% x: value %d does not re-encode in %d bytes", b[:n], v, n)
		}
		short := AppendVarint(nil, v)
		if len(short) != SizeVarint(v) || len(short) > n {
			t.Fatalf("value %d: shortest encoding %d bytes, SizeVarint %d, read from %d", v, len(short), SizeVarint(v), n)
		}
		if w, m := ConsumeVarint(short); w != v || m != len(short) {
			t.Fatalf("value %d: shortest encoding decodes to %d in %d bytes", v, w, m)
		}
	})
}
