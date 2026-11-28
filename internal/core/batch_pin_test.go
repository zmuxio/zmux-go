package core

import (
	"bytes"
	"testing"
)

// Small stream pieces copied into the batch arena do not keep their send
// blocks pinned until WriteDone: once polled, only the referenced pieces'
// blocks are pinned, and every stream's block went back once its bytes
// were sent.
func TestBatchPinsOnlyReferencedPieces(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 100
	s.conn.sendMax = 1 << 20
	const streams = 16
	var blks []*block
	for range streams {
		st, _ := s.OpenStream(true)
		if _, err := s.Write(st, bytes.Repeat([]byte{7}, 100), false); err != nil {
			t.Fatal(err)
		}
		blks = append(blks, st.snd.q.segs[0].blk)
	}
	big, _ := s.OpenStream(true)
	if _, err := s.Write(big, bytes.Repeat([]byte{8}, 2*inlineSendLimit), false); err != nil {
		t.Fatal(err)
	}
	b := GetBatch()
	defer PutBatch(b)
	now := Time(Second)
	s.PollTransmit(p.id, 0, b, DefaultBatch, now)
	if len(b.pins) != 1 {
		t.Fatalf("%d pins after the poll, want the large piece's only", len(b.pins))
	}
	for i, k := range blks {
		if n := k.refs.Load(); n > 1 {
			t.Fatalf("stream %d block keeps %d references", i, n)
		}
	}
	s.WriteDone(b, b.Len(), nil, now)
}
