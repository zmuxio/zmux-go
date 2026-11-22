package carrierstats

import (
	"encoding/binary"
	"testing"
)

func TestNativeCarrierPairs(t *testing.T) {
	var linux [160]byte
	binary.NativeEndian.PutUint32(linux[100:], 3)
	binary.NativeEndian.PutUint32(linux[156:], 17)
	var darwin [112]byte
	binary.NativeEndian.PutUint64(darwin[56:], 1<<35)
	binary.NativeEndian.PutUint64(darwin[104:], 2)
	for _, tc := range []struct {
		name       string
		decode     func([]byte) Readout
		buf        []byte
		sent, lost uint64
	}{
		{"linux", decodeLinuxPair, linux[:], 17, 3},
		{"darwin", decodeDarwinPair, darwin[:], 1 << 35, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.decode(tc.buf); got != (Readout{Sent: tc.sent, Lost: tc.lost, Known: true}) {
				t.Fatalf("pair = %+v", got)
			}
			if tc.decode(tc.buf[:len(tc.buf)-1]).Known {
				t.Fatal("short native reply produced a known pair")
			}
			clear(tc.buf)
			if got := tc.decode(tc.buf); got != (Readout{Known: true}) {
				t.Fatalf("zero pair = %+v", got)
			}
		})
	}
}
