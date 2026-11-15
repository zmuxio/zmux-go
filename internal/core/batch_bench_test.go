package core

import "testing"

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
		s.WriteDone(bt, nil, now)
		PutBatch(bt)
	}
}
