package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// TestRecvRangesBounded: a peer that sends one byte at every other offset,
// highest first (each insert the general, linear case), is stopped at the
// bound on a stream's disjoint ranges instead of growing pieces without
// end.
func TestRecvRangesBounded(t *testing.T) {
	s, p := statsMP()
	s.feat.Multipath = true
	s.rxPipe = p
	st := s.newStream(true, false)
	st.id, st.idSet = 1, true
	st.rcv.adv, st.rcv.window = 1<<20, 1<<20
	s.conn.adv = 1 << 20
	s.streams[1] = st
	lim := maxRecvRanges(1 << 20)
	var err *wire.Error
	sent := 0
	for off := uint64(2*lim + 2); off > 0 && err == nil; off -= 2 {
		f := &wire.Frame{Kind: wire.KindStream, StreamID: 1, Offset: off, Data: []byte{1}}
		_, err = s.onStream(f, nil)
		sent++
	}
	if err == nil || err.Code != wire.ExcessiveLoad {
		t.Fatalf("after %d pieces: %v", sent, err)
	}
	if n := st.rcv.arrived.len(); n != lim+1 {
		t.Fatalf("stopped at %d ranges, bound %d", n, lim)
	}
}

const rbSize = 128 << 10 // the root package's read buffer

// TestRecvPinnedCompaction: streams whose data each arrived as one 4 KiB
// piece of its own 128 KiB read buffer (a slow reader, carrier reads that
// return one record) would charge 32 times the bytes they hold; the
// sparse buffers are copied out instead, so the charge stays within a few
// times the bytes plus the slack, and every stream still reads its data.
func TestRecvPinnedCompaction(t *testing.T) {
	s, _ := statsMP()
	s.cfg.ConnWindowCap = 1 << 30
	s.conn.adv = 1 << 30
	const streams, size = 512, refThreshold
	var bufs []*Buf
	for i := range streams {
		id := uint64(4*i + 1)
		st := s.newStream(true, false)
		st.id, st.idSet = id, true
		st.rcv.adv, st.rcv.window = 1<<20, 1<<20
		s.streams[id] = st
		b := &Buf{B: make([]byte, rbSize)}
		b.refs.Store(1)
		for j := range size {
			b.B[j] = byte(i + j)
		}
		f := &wire.Frame{Kind: wire.KindStream, StreamID: id, Data: b.B[:size]}
		if _, err := s.onStream(f, b); err != nil {
			t.Fatal(err)
		}
		b.Unref() // the driver lets go of it
		bufs = append(bufs, b)
	}
	c := &s.conn
	if c.buffered != streams*size {
		t.Fatalf("buffered %d", c.buffered)
	}
	charge := c.buffered + c.pins.extra - streams*pieceCost
	if limit := (pinSparse+1)*c.buffered + pinSlack + rbSize; charge > limit {
		t.Fatalf("charged %d for %d bytes, limit %d", charge, c.buffered, limit)
	}
	t.Logf("charged %d for %d bytes (%.1fx); without compaction %d",
		charge, c.buffered, float64(charge)/float64(c.buffered), streams*rbSize)
	for i := range streams {
		st := s.streams[uint64(4*i+1)]
		got := make([]byte, size+1)
		n, _, err := s.Read(st, got)
		if err != nil || n != size {
			t.Fatalf("stream %d: %d %v", i, n, err)
		}
		for j := range size {
			if got[j] != byte(i+j) {
				t.Fatalf("stream %d byte %d", i, j)
			}
		}
	}
	if c.buffered != 0 || c.pins.extra != 0 || len(c.pins.bufs) != 0 {
		t.Fatalf("after the reads: buffered %d, extra %d, %d buffers", c.buffered, c.pins.extra, len(c.pins.bufs))
	}
	for i, b := range bufs {
		if b.Refs() != 0 {
			t.Fatalf("buffer %d keeps %d references", i, b.Refs())
		}
	}
}

// TestRecvPinnedBuffer: a piece that references a read buffer charges the
// whole buffer while it is held, each piece its fixed cost, and nothing is
// left once they were read or taken.
func TestRecvPinnedBuffer(t *testing.T) {
	var ps pinSet
	rb := recvBuf{pin: &ps}
	b := &Buf{B: make([]byte, 128<<10)}
	b.refs.Store(1)
	rb.insert(0, b.B[:refThreshold], b)
	rb.insert(refThreshold, b.B[refThreshold:2*refThreshold], b)
	if want := uint64(128<<10-2*refThreshold) + 2*pieceCost; ps.extra != want {
		t.Fatalf("pinned %d beyond the pieces, want %d", ps.extra, want)
	}
	rb.read(0, make([]byte, refThreshold+10))
	if want := uint64(128<<10-refThreshold+10) + pieceCost; ps.extra != want {
		t.Fatalf("pinned %d after a partial read, want %d", ps.extra, want)
	}
	d, held := rb.take(refThreshold+10, 1<<20)
	if len(d) != refThreshold-10 || held != b || ps.extra != 0 || len(ps.bufs) != 0 {
		t.Fatalf("after the take: %d bytes, pinned %d", len(d), ps.extra)
	}
	held.Unref()
}
