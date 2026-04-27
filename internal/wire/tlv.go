package wire

import "errors"

type TLV struct {
	Type  uint64
	Value []byte
}

type tlvVisitor func(typ uint64, value []byte) error

func AppendTLV(dst []byte, typ uint64, value []byte) ([]byte, error) {
	var err error
	dst, err = AppendVarint(dst, typ)
	if err != nil {
		return nil, err
	}
	dst, err = AppendVarint(dst, uint64(len(value)))
	if err != nil {
		return nil, err
	}
	dst = append(dst, value...)
	return dst, nil
}

func ParseTLVs(src []byte) ([]TLV, error) {
	return parseTLVs(src, true)
}

func ParseTLVsView(src []byte) ([]TLV, error) {
	return parseTLVs(src, false)
}

func parseTLVsView(src []byte) ([]TLV, error) {
	return parseTLVs(src, false)
}

func walkTLVs(src []byte, visit tlvVisitor) error {
	for len(src) > 0 {
		typ, nType, err := ParseVarint(src)
		if err != nil {
			if errors.Is(err, ErrTruncatedVarint) {
				return ErrTruncatedTLV
			}
			return err
		}
		src = src[nType:]
		length, nLen, err := ParseVarint(src)
		if err != nil {
			if errors.Is(err, ErrTruncatedVarint) {
				return ErrTruncatedTLV
			}
			return err
		}
		src = src[nLen:]
		if uint64(len(src)) < length {
			return ErrTLVValueOverrun
		}
		value := src[:length]
		src = src[length:]
		if visit != nil {
			if err := visit(typ, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTLVs(src []byte) error {
	return walkTLVs(src, nil)
}

const maxTLVParseCapacityHint = 64

func tlvParseCapacityHint(srcLen int) int {
	if srcLen <= 0 {
		return 0
	}
	hint := srcLen / 2 // The smallest valid TLV is one-byte type + one-byte length.
	if hint > maxTLVParseCapacityHint {
		return maxTLVParseCapacityHint
	}
	return hint
}

func parseTLVs(src []byte, cloneValues bool) ([]TLV, error) {
	var out []TLV
	if hint := tlvParseCapacityHint(len(src)); hint > 0 {
		out = make([]TLV, 0, hint)
	}
	err := walkTLVs(src, func(typ uint64, value []byte) error {
		if cloneValues {
			value = append([]byte(nil), value...)
		}
		out = append(out, TLV{Type: typ, Value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
