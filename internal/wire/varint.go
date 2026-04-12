package wire

import (
	"fmt"
	"io"
)

func VarintLen(v uint64) (int, error) {
	switch {
	case v <= 63:
		return 1, nil
	case v <= 16383:
		return 2, nil
	case v <= 1073741823:
		return 4, nil
	case v <= MaxVarint62:
		return 8, nil
	default:
		return 0, ErrValueTooLarge
	}
}

func AppendVarint(dst []byte, v uint64) ([]byte, error) {
	n, err := VarintLen(v)
	if err != nil {
		return nil, err
	}
	switch n {
	case 1:
		return append(dst, byte(v)), nil
	case 2:
		return append(dst, byte((v>>8)&0x3f|0x40), byte(v)), nil
	case 4:
		return append(dst, byte((v>>24)&0x3f|0x80), byte(v>>16), byte(v>>8), byte(v)), nil
	case 8:
		return append(dst,
			byte((v>>56)&0x3f|0xc0),
			byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
		), nil
	default:
		return nil, fmt.Errorf("unsupported varint length %d", n)
	}
}

func PackVarint(v uint64) (uint64, uint8, error) {
	n, err := VarintLen(v)
	if err != nil {
		return 0, 0, err
	}
	switch n {
	case 1:
		return uint64(byte(v)), 1, nil
	case 2:
		return uint64(byte((v>>8)&0x3f|0x40)) |
			uint64(byte(v))<<8, 2, nil
	case 4:
		return uint64(byte((v>>24)&0x3f|0x80)) |
			uint64(byte(v>>16))<<8 |
			uint64(byte(v>>8))<<16 |
			uint64(byte(v))<<24, 4, nil
	case 8:
		return uint64(byte((v>>56)&0x3f|0xc0)) |
			uint64(byte(v>>48))<<8 |
			uint64(byte(v>>40))<<16 |
			uint64(byte(v>>32))<<24 |
			uint64(byte(v>>24))<<32 |
			uint64(byte(v>>16))<<40 |
			uint64(byte(v>>8))<<48 |
			uint64(byte(v))<<56, 8, nil
	default:
		return 0, 0, fmt.Errorf("unsupported varint length %d", n)
	}
}

func AppendPackedVarint(dst []byte, packed uint64, n uint8) ([]byte, error) {
	switch n {
	case 1:
		return append(dst, byte(packed)), nil
	case 2:
		return append(dst, byte(packed), byte(packed>>8)), nil
	case 4:
		return append(dst,
			byte(packed),
			byte(packed>>8),
			byte(packed>>16),
			byte(packed>>24),
		), nil
	case 8:
		return append(dst,
			byte(packed),
			byte(packed>>8),
			byte(packed>>16),
			byte(packed>>24),
			byte(packed>>32),
			byte(packed>>40),
			byte(packed>>48),
			byte(packed>>56),
		), nil
	default:
		return nil, fmt.Errorf("unsupported packed varint length %d", n)
	}
}

func EncodeVarint(v uint64) ([]byte, error) { return AppendVarint(nil, v) }

func ParseVarint(src []byte) (uint64, int, error) {
	if len(src) == 0 {
		return 0, 0, ErrTruncatedVarint
	}
	first := src[0]
	prefix := first >> 6
	n := 1 << prefix
	if len(src) < n {
		return 0, 0, ErrTruncatedVarint
	}

	var v uint64
	switch n {
	case 1:
		v = uint64(first & 0x3f)
	case 2:
		v = uint64(first&0x3f)<<8 | uint64(src[1])
	case 4:
		v = uint64(first&0x3f)<<24 | uint64(src[1])<<16 | uint64(src[2])<<8 | uint64(src[3])
	case 8:
		v = uint64(first&0x3f)<<56 |
			uint64(src[1])<<48 | uint64(src[2])<<40 | uint64(src[3])<<32 |
			uint64(src[4])<<24 | uint64(src[5])<<16 | uint64(src[6])<<8 | uint64(src[7])
	default:
		return 0, 0, ErrTruncatedVarint
	}
	if v > MaxVarint62 {
		return 0, 0, ErrValueTooLarge
	}
	want, err := VarintLen(v)
	if err != nil {
		return 0, 0, err
	}
	if want != n {
		return 0, 0, ErrNonCanonicalVarint
	}
	return v, n, nil
}

func ReadVarint(r io.ByteReader) (uint64, int, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	prefix := first >> 6
	n := 1 << prefix
	var buf [8]byte
	buf[0] = first
	for i := 1; i < n; i++ {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return 0, 0, ErrTruncatedVarint
			}
			return 0, 0, err
		}
		buf[i] = b
	}

	var v uint64
	switch n {
	case 1:
		v = uint64(buf[0] & 0x3f)
	case 2:
		v = uint64(buf[0]&0x3f)<<8 | uint64(buf[1])
	case 4:
		v = uint64(buf[0]&0x3f)<<24 | uint64(buf[1])<<16 | uint64(buf[2])<<8 | uint64(buf[3])
	case 8:
		v = uint64(buf[0]&0x3f)<<56 |
			uint64(buf[1])<<48 | uint64(buf[2])<<40 | uint64(buf[3])<<32 |
			uint64(buf[4])<<24 | uint64(buf[5])<<16 | uint64(buf[6])<<8 | uint64(buf[7])
	default:
		return 0, 0, ErrTruncatedVarint
	}
	if v > MaxVarint62 {
		return 0, 0, ErrValueTooLarge
	}
	want, err := VarintLen(v)
	if err != nil {
		return 0, 0, err
	}
	if want != n {
		return 0, 0, ErrNonCanonicalVarint
	}
	return v, n, nil
}
