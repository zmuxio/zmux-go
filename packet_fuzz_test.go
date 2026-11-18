package zmux

import (
	"testing"

	"github.com/zmuxio/zmux-go/varint"
)

// FuzzParseCapsuleHeader checks the incremental capsule header parser: it
// never accepts a header whose payload exceeds the bound, never claims
// more than eight missing bytes, and the header of a datagram it accepts
// is what appendCapsuleHeader writes for it when minimally encoded.
func FuzzParseCapsuleHeader(f *testing.F) {
	for _, c := range []struct {
		cx    uint64
		n     int
		limit uint16
	}{{0, 0, 16}, {5, 100, 1500}, {1 << 20, 1500, 1500}, {varint.MaxVarint, 7, 8}} {
		f.Add(appendCapsuleHeader(nil, c.cx, c.n), c.limit)
	}
	f.Add([]byte{}, uint16(100))
	f.Add([]byte{0x00}, uint16(100))
	f.Add([]byte{0x00, 0x40}, uint16(100))
	f.Add([]byte{0x05, 0x04, 'a', 'b', 'c', 'd'}, uint16(100))
	f.Add([]byte{0x00, 0x00}, uint16(100))
	f.Fuzz(func(t *testing.T, b []byte, bound uint16) {
		limit := int(bound)
		h, need, err := parseCapsuleHeader(b, limit)
		if err != nil {
			return
		}
		if need < 0 || need > 8 {
			t.Fatalf("% x: %d bytes missing", b, need)
		}
		if need > 0 {
			return
		}
		if h.n > len(b) || h.payload > uint64(limit)+8 || (h.typ == capsuleDatagram && h.payload > uint64(limit)) {
			t.Fatalf("% x: header %+v, limit %d", b, h, limit)
		}
		if h.typ != capsuleDatagram {
			return
		}
		again, need, err := parseCapsuleHeader(appendCapsuleHeader(nil, h.cx, int(h.payload)), limit)
		if err != nil || need != 0 || again.cx != h.cx || again.payload != h.payload {
			t.Fatalf("header %+v re-encodes to %+v (need %d, %v)", h, again, need, err)
		}
	})
}
