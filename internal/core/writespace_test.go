package core

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/zmuxio/zmux-go/internal/israce"
)

// lentSession is a session with one stream whose credit never runs out.
func lentSession(t *testing.T) (*Session, *Pipe, *Stream) {
	t.Helper()
	s, p := channelSession(t, nil, false)
	s.peerMaxRecord = 16 << 10
	s.peer.MaxStreamDataBidiRem = 1 << 62
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 62
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	return s, p, st
}

// fillLent writes total bytes of val through WriteSpace and CommitWrite in
// steps of at most step bytes, as ReadFrom does with a reader.
func fillLent(t *testing.T, s *Session, st *Stream, total, step int, val byte) {
	for total > 0 {
		buf, err := s.WriteSpace(st, min(step, total))
		if err != nil || len(buf) == 0 {
			t.Fatalf("WriteSpace: %d, %v", len(buf), err)
		}
		for i := range buf {
			buf[i] = val
		}
		if err := s.CommitWrite(st, len(buf)); err != nil {
			t.Fatal(err)
		}
		total -= len(buf)
	}
}

// drainLent sends everything queued and returns the stream bytes.
func drainLent(t *testing.T, s *Session, p *Pipe, b *Batch, out []byte) []byte {
	now := Time(Second)
	for s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
		for _, r := range recordData(t, b) {
			out = append(out, make([]byte, r)...)
		}
		s.WriteDone(b, nil, now)
	}
	return out
}

func TestWriteSpaceData(t *testing.T) {
	s, p, st := lentSession(t)
	// Partial steps leave tail room both above and below minLend.
	fillLent(t, s, st, 3000, 1000, 'a')
	fillLent(t, s, st, 40000, 7777, 'b')
	if _, err := s.Write(st, bytes.Repeat([]byte{'c'}, 500), false); err != nil {
		t.Fatal(err)
	}
	fillLent(t, s, st, 20000, 16<<10, 'd')
	want := append(append(append(bytes.Repeat([]byte{'a'}, 3000), bytes.Repeat([]byte{'b'}, 40000)...),
		bytes.Repeat([]byte{'c'}, 500)...), bytes.Repeat([]byte{'d'}, 20000)...)
	q := &st.snd.q
	if q.end-q.base != uint64(len(want)) {
		t.Fatalf("queued %d, want %d", q.end-q.base, len(want))
	}
	var got []byte
	for off := q.base; off < q.end; {
		b, _ := q.peek(off, 1<<20)
		got = append(got, b...)
		off += uint64(len(b))
	}
	if !bytes.Equal(got, want) {
		t.Fatal("queued bytes differ")
	}
	if a := s.stats.AppBytesWritten; a != uint64(len(want)) {
		t.Fatalf("AppBytesWritten %d", a)
	}
	b := GetBatch()
	defer PutBatch(b)
	drainLent(t, s, p, b, nil)
}

// A loan whose tail is released meanwhile still lands after it.
func TestWriteSpaceTailReleased(t *testing.T) {
	s, p, st := lentSession(t)
	fillLent(t, s, st, 100, 100, 'a')
	buf, err := s.WriteSpace(st, 1<<20)
	if err != nil || len(buf) != blockSize-100 {
		t.Fatalf("WriteSpace: %d, %v", len(buf), err)
	}
	b := GetBatch()
	defer PutBatch(b)
	drainLent(t, s, p, b, nil) // sends and releases the 100 bytes
	if st.snd.q.tail != nil {
		t.Fatal("tail kept after release")
	}
	copy(buf, "xyz")
	if err := s.CommitWrite(st, 3); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.snd.q.peek(st.snd.q.base, 10); string(got) != "xyz" {
		t.Fatalf("queued %q", got)
	}
}

// A reset during a loan discards its bytes and returns the block.
func TestWriteSpaceReset(t *testing.T) {
	s, _, st := lentSession(t)
	fillLent(t, s, st, 100, 100, 'a')
	if _, err := s.WriteSpace(st, 1<<20); err != nil {
		t.Fatal(err)
	}
	blk := st.snd.res
	s.Reset(st, 1)
	if err := s.CommitWrite(st, 10); err == nil {
		t.Fatal("CommitWrite after reset succeeded")
	}
	if r := blk.refs.Load(); r > 1 {
		t.Fatalf("block refs %d after the loan", r)
	}
}

// The send buffer bounds a loan, and a full buffer lends nothing.
func TestWriteSpaceRoom(t *testing.T) {
	s, _, st := lentSession(t)
	s.SetSendBuffer(st, 20000)
	fillLent(t, s, st, 20000, 1<<20, 'a')
	buf, err := s.WriteSpace(st, 1<<20)
	if err != nil || len(buf) != 0 {
		t.Fatalf("WriteSpace on a full buffer: %d, %v", len(buf), err)
	}
}

// Filling in place allocates nothing once warm, nor does Write: the
// queue reuses its segment array. The bound leaves room for the block
// pool's own bookkeeping (sync.Pool grows its per-P lists now and then).
func TestWriteSpaceNoAlloc(t *testing.T) {
	if israce.On {
		t.Skip("the race detector allocates")
	}
	s, p, st := lentSession(t)
	b := GetBatch()
	defer PutBatch(b)
	out := make([]byte, 0, 1<<20)
	const total = 256 << 10
	src := make([]byte, 32<<10)
	mallocs := func(fill func()) uint64 {
		var n uint64
		var ms runtime.MemStats
		for range 20 {
			out = drainLent(t, s, p, b, out[:0])
			runtime.ReadMemStats(&ms)
			before := ms.Mallocs
			fill()
			runtime.ReadMemStats(&ms)
			n += ms.Mallocs - before
		}
		return n
	}
	lent := func() { fillLent(t, s, st, total, 32<<10, 0) }
	write := func() {
		for left := total; left > 0; left -= len(src) {
			if _, err := s.Write(st, src, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	mallocs(lent) // warm the pools
	mallocs(write)
	l, w := mallocs(lent), mallocs(write)
	t.Logf("256 KiB x 20: WriteSpace %d, Write %d allocations", l, w)
	if l >= 20 || w >= 20 {
		t.Fatalf("256 KiB x 20: WriteSpace %d, Write %d allocations", l, w)
	}
}
