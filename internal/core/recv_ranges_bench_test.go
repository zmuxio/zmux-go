package core

import (
	"strconv"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// BenchmarkRecvReorderAtCap: a peer fills a stream with one-byte pieces at
// every other offset, highest first, up to the bound on its disjoint
// ranges (each insert the general, linear case of a sorted insert); the
// time is per piece.
func BenchmarkRecvReorderAtCap(b *testing.B) {
	for _, window := range []uint64{1 << 20, 256 << 20} {
		lim := maxRecvRanges(window)
		b.Run(strconv.Itoa(lim), func(b *testing.B) {
			b.ReportAllocs()
			data := []byte{1}
			f := &wire.Frame{Kind: wire.KindStream, StreamID: 1, Data: data}
			for b.Loop() {
				b.StopTimer()
				s, p := statsMP()
				s.feat.Multipath = true
				s.rxPipe = p
				s.conn.adv = 1 << 40
				st := s.newStream(true, false)
				st.id, st.idSet = 1, true
				st.rcv.adv, st.rcv.window = window, window
				s.streams[1] = st
				b.StartTimer()
				for off := uint64(2 * lim); off > 0; off -= 2 {
					f.Offset = off
					if err := onStreamTest(s, f); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(lim), "ns/piece")
		})
	}
}
