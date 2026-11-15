package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// BenchmarkRecvRefPieces: one stream receives a read buffer of eight
// referenced record-sized pieces and reads them.
func BenchmarkRecvRefPieces(b *testing.B) {
	s, _ := statsMP()
	s.cfg.ConnWindowCap = 1 << 62
	s.conn.adv = 1 << 62
	st := s.newStream(true, false)
	st.id, st.idSet = 1, true
	st.rcv.adv, st.rcv.window = 1<<62, 1<<62
	s.streams[1] = st
	buf := &Buf{B: make([]byte, 128<<10)}
	const piece = 16 << 10
	dst := make([]byte, piece)
	var off uint64
	b.ReportAllocs()
	b.SetBytes(int64(len(buf.B)))
	for b.Loop() {
		buf.refs.Store(1)
		for i := 0; i < len(buf.B); i += piece {
			f := &wire.Frame{Kind: wire.KindStream, StreamID: 1, Offset: off, Data: buf.B[i : i+piece]}
			if _, err := s.onStream(f, buf); err != nil {
				b.Fatal(err)
			}
			off += piece
		}
		buf.Unref()
		for range len(buf.B) / piece {
			if n, _, err := s.Read(st, dst); err != nil || n != piece {
				b.Fatal(n, err)
			}
		}
	}
}
