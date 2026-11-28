package core

import (
	"io"
	"testing"

	"github.com/zmuxio/zmux-go/internal/carrierio"
)

// BenchmarkPollSmallWrites: sixteen streams each write a small piece that
// one poll copies into its batch.
func BenchmarkPollSmallWrites(b *testing.B) {
	s, p := channelSession(b, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 62
	s.loc[0].limit = 100
	s.conn.sendMax = 1 << 62
	var sts []*Stream
	for range 16 {
		st, _ := s.OpenStream(true)
		sts = append(sts, st)
	}
	data := make([]byte, 100)
	now := Time(Second)
	b.ReportAllocs()
	for b.Loop() {
		for _, st := range sts {
			st.snd.maxData = st.snd.sent + 1<<20
			if _, err := s.Write(st, data, false); err != nil {
				b.Fatal(err)
			}
		}
		bt := GetBatch()
		s.PollTransmit(p.id, 0, bt, DefaultBatch, now)
		s.WriteDone(bt, bt.Len(), nil, now)
		PutBatch(bt)
	}
}

// BenchmarkBatchFlatten isolates preparation of a contiguous carrier write.
func BenchmarkBatchFlatten(b *testing.B) {
	const chunk = 16 << 10
	data := make([]byte, 4*chunk)
	for i := range data {
		data[i] = byte(i)
	}
	var batch Batch
	var w recWriter
	w.init(&batch, FramingBytes, 2*chunk, DefaultBatch)
	for off := 0; off < len(data); off += chunk {
		if !w.stream(0, uint64(off), data[off:off+chunk], pin{}, false, chunk) {
			b.Fatal("stream frame did not fit")
		}
		w.end()
	}
	b.Run("Pooled", func(b *testing.B) {
		b.SetBytes(int64(batch.Len()))
		b.ReportAllocs()
		for b.Loop() {
			_, _ = carrierio.WriteFlat(io.Discard, batch.Bufs())
		}
	})
	b.Run("Batch", func(b *testing.B) {
		b.SetBytes(int64(batch.Len()))
		b.ReportAllocs()
		for b.Loop() {
			_, _ = io.Discard.Write(batch.FlatView())
		}
	})
}
