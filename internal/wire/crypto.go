package wire

import "github.com/zmuxio/zmux-go/varint"

// Suite and Group values that the secure carrier requires in every set.
const (
	RequiredSuite = 1
	RequiredGroup = 2
)

// SessionCrypto is the value of zmux_session_crypto, the Suites and Groups an end declares:
// the Join Suites and the key exchange Groups an endpoint implements. It is the
// one encoding of these sets: the parameter, session_crypto and the local
// configuration all use it.
type SessionCrypto struct {
	Suites []uint64
	Groups []uint64
}

// Valid reports whether both sets are strictly ascending, without zero, and hold
// Suite 1 and Group 2.
func (c SessionCrypto) Valid() bool {
	return validSet(c.Suites, RequiredSuite) && validSet(c.Groups, RequiredGroup)
}

func validSet(set []uint64, required uint64) bool {
	has := false
	for i, v := range set {
		if v == 0 || v > varint.MaxVarint || i > 0 && v <= set[i-1] {
			return false
		}
		has = has || v == required
	}
	return has
}

// Append appends SuiteSet ‖ GroupSet. The caller checks Valid.
func (c SessionCrypto) Append(b []byte) []byte {
	for _, set := range [][]uint64{c.Suites, c.Groups} {
		b = varint.AppendVarint(b, uint64(len(set)))
		for _, v := range set {
			b = varint.AppendVarint(b, v)
		}
	}
	return b
}

// DecodeSessionCrypto parses a whole value: shortest varints, nothing after the
// GroupSet, and sets that are Valid.
func DecodeSessionCrypto(v []byte) (SessionCrypto, bool) {
	var c SessionCrypto
	for _, set := range []*[]uint64{&c.Suites, &c.Groups} {
		count, n := varint.ConsumeVarint(v)
		if n == 0 || n != varint.SizeVarint(count) || count > uint64(len(v)) {
			return SessionCrypto{}, false
		}
		v = v[n:]
		out := make([]uint64, 0, count)
		for range count {
			x, m := varint.ConsumeVarint(v)
			if m == 0 || m != varint.SizeVarint(x) {
				return SessionCrypto{}, false
			}
			out = append(out, x)
			v = v[m:]
		}
		*set = out
	}
	if len(v) != 0 || !c.Valid() {
		return SessionCrypto{}, false
	}
	return c, true
}

// Intersect returns the values of a that b also holds, ascending.
func Intersect(a, b []uint64) []uint64 {
	var out []uint64
	for _, x := range a {
		for _, y := range b {
			if x == y {
				out = append(out, x)
				break
			}
		}
	}
	return out
}
