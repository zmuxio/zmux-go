// Package varint encodes and decodes QUIC variable-length integers
// (RFC 9000 Section 16): 1, 2, 4 or 8 bytes, the two high bits of the first
// byte giving the length, holding values up to 2^62-1.
//
// The package depends on the standard library only.
package varint

// MaxVarint is the largest value a variable-length integer can hold (2^62-1).
const MaxVarint = 1<<62 - 1

// SizeVarint returns the length of the shortest encoding of v. v must not
// exceed MaxVarint.
func SizeVarint(v uint64) int {
	switch {
	case v < 1<<6:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<30:
		return 4
	default:
		return 8
	}
}

// AppendVarint appends the shortest encoding of v to b. v must not exceed
// MaxVarint.
func AppendVarint(b []byte, v uint64) []byte {
	return AppendVarintN(b, v, SizeVarint(v))
}

// AppendVarintN appends v in exactly n bytes (1, 2, 4 or 8, any other n
// meaning 8); v must fit in n bytes. A longer encoding than the shortest is
// valid QUIC and lets a field be written before its value is final.
func AppendVarintN(b []byte, v uint64, n int) []byte {
	switch n {
	case 1:
		return append(b, byte(v))
	case 2:
		return append(b, byte(v>>8)|0x40, byte(v))
	case 4:
		return append(b, byte(v>>24)|0x80, byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, byte(v>>56)|0xc0, byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// ConsumeVarint decodes the variable-length integer at the start of b and
// returns it with the number of bytes it took; n is 0 when b is too short
// to hold it. Non-shortest encodings are accepted, as QUIC requires.
func ConsumeVarint(b []byte) (v uint64, n int) {
	if len(b) == 0 {
		return 0, 0
	}
	l := 1 << (b[0] >> 6)
	if len(b) < l {
		return 0, 0
	}
	v = uint64(b[0] & 0x3f)
	for i := 1; i < l; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v, l
}
