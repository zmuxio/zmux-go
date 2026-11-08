package join

import "github.com/zmuxio/zmux-go/varint"

// AppendItemHeader appends the type and length of a payload item.
func AppendItemHeader(b []byte, typ uint64, n int) []byte {
	return varint.AppendVarint(varint.AppendVarint(b, typ), uint64(n))
}

// ConsumeMinimal reads a shortest-form varint.
func ConsumeMinimal(b []byte) (v uint64, n int, err error) {
	v, n = varint.ConsumeVarint(b)
	if n == 0 {
		return 0, 0, ErrTLVTruncated
	}
	if n != varint.SizeVarint(v) {
		return 0, 0, ErrTLVNonMinimal
	}
	return v, n, nil
}

// ConsumeItem reads one type, length and value.
func ConsumeItem(b []byte) (typ uint64, val, rest []byte, err error) {
	typ, n, err := ConsumeMinimal(b)
	if err != nil {
		return 0, nil, nil, err
	}
	b = b[n:]
	l, n, err := ConsumeMinimal(b)
	if err != nil {
		return 0, nil, nil, err
	}
	b = b[n:]
	if l > uint64(len(b)) {
		return 0, nil, nil, ErrTLVTruncated
	}
	return typ, b[:l], b[l:], nil
}

// ParseItems applies the payload rules in byte order: the first violation met
// is the result. visit takes each item in turn and reports whether the type is
// known in this message, after validating its value; an unknown type with low
// bit 1 fails and one with low bit 0 is skipped. Values alias b.
func ParseItems(b []byte, visit func(typ uint64, val []byte) (known bool, err error)) error {
	first := true
	var last uint64
	for len(b) > 0 {
		typ, val, rest, err := ConsumeItem(b)
		if err != nil {
			return err
		}
		if !first && typ <= last {
			return ErrTLVOrder
		}
		first, last, b = false, typ, rest
		known, err := visit(typ, val)
		if err != nil {
			return err
		}
		if !known && typ&1 == 1 {
			return ErrTLVUnknownCritical
		}
	}
	return nil
}
