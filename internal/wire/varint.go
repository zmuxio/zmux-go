// Package wire encodes and decodes the zmux wire format: QUIC variable-length
// integers, QMux records, frames, transport parameters and stream preambles.
// It allocates nothing on the decode path: decoded byte fields alias the input.
package wire

// MaxVarint is the largest value a variable-length integer can hold (2^62-1).
const MaxVarint = 1<<62 - 1

// VarintLen returns the length of the shortest encoding of v. v must not
// exceed MaxVarint.
func VarintLen(v uint64) int {
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

// AppendVarint appends the shortest encoding of v.
func AppendVarint(b []byte, v uint64) []byte {
	switch {
	case v < 1<<6:
		return append(b, byte(v))
	case v < 1<<14:
		return append(b, byte(v>>8)|0x40, byte(v))
	case v < 1<<30:
		return append(b, byte(v>>24)|0x80, byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, byte(v>>56)|0xc0, byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// AppendVarintN appends v in exactly n bytes (1, 2, 4 or 8); v must fit.
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

// ReadVarint decodes a varint from b. n is 0 when b is too short.
func ReadVarint(b []byte) (v uint64, n int) {
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
