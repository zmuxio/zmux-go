package core

import (
	"errors"
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
		err = onStreamTest(s, f)
		sent++
	}
	if err == nil || err.Code != wire.ExcessiveLoad {
		t.Fatalf("after %d pieces: %v", sent, err)
	}
	if n := st.rcv.arrived.len(); n != lim+1 {
		t.Fatalf("stopped at %d ranges, bound %d", n, lim)
	}
}

func TestRecvCompactionKeepsSparseStreamsReadable(t *testing.T) {
	s, _ := statsMP()
	s.cfg.ConnWindowCap = 1 << 30
	s.conn.adv = 1 << 30
	const streams, capacity = 64, 128 << 10
	roots := make([]*Buf, streams)
	for i := range streams {
		id := uint64(4*i + 1)
		st := s.newStream(true, false)
		st.id, st.idSet = id, true
		st.rcv.adv, st.rcv.window = 1<<20, 1<<20
		s.streams[id] = st
		b := NewBuf(make([]byte, capacity))
		b.B[0] = byte(i)
		f := &wire.Frame{Kind: wire.KindStream, StreamID: id, Data: b.B[:1]}
		if _, err := s.onStream(f, Input{First: b.Span(0, 1)}); err != nil {
			t.Fatal(err)
		}
		b.Unref()
		roots[i] = b
	}
	m := &s.conn.rx
	if m.live != streams || m.queued-m.live > 1<<20+capacity {
		t.Fatalf("retained %d bytes for %d byte views", m.queued, m.live)
	}
	var got [1]byte
	for i := range streams {
		st := s.streams[uint64(4*i+1)]
		n, _, err := s.Read(st, got[:])
		if err != nil || n != 1 || got[0] != byte(i) {
			t.Fatalf("stream %d: %d %v %x", i, n, err, got)
		}
		if roots[i].Refs() != 0 {
			t.Fatalf("root %d: %d references", i, roots[i].Refs())
		}
	}
	if m.queued != 0 || m.account.bytes.Load() != 0 || m.nodes != 0 {
		t.Fatalf("after reads: %d queued, %d resident, %d pieces", m.queued, m.account.bytes.Load(), m.nodes)
	}
}

func TestReceiveLoansShareBackingAndFailBeforeConsume(t *testing.T) {
	m := recvMemory{account: &recvAccount{loanCap: 256}}
	a, b := NewBuf(make([]byte, 256)), NewBuf(make([]byte, 256))
	rb := recvBuf{memory: &m}
	rb.insert(0, a, 0, 16)
	rb.insert(16, a, 24, 16)
	rb.insert(32, b, 0, 16)
	a.Unref()
	b.Unref()

	if a.Refs() != 1 || b.Refs() != 1 || m.queued != 512 {
		t.Fatalf("receive ownership: %d + %d references, %d bytes", a.Refs(), b.Refs(), m.queued)
	}
	for i := range 2 {
		n, view, err := rb.consume(uint64(i*16), nil, 16, true)
		if err != nil {
			t.Fatal(err)
		}
		d, root := view.Bytes(), view.Buf
		if root != a || n != 16 || cap(d) != n {
			t.Fatalf("unexpected view %d: %d bytes", i, n)
		}
	}
	if m.account.loaned.Load() != 256 || a.Refs() != 2 {
		t.Fatalf("shared loan charged %d with %d references", m.account.loaned.Load(), a.Refs())
	}

	a.ReturnView()
	if n, _, err := rb.consume(32, nil, 16, true); n != 0 || !errors.Is(err, ErrReceiveLoanLimit) || rb.bytes != 16 {
		t.Fatalf("loan consumed data: %d %v, %d remaining", n, err, rb.bytes)
	}
	var dst [16]byte
	if n, _, err := rb.consume(32, dst[:], len(dst), false); n != len(dst) || err != nil {
		t.Fatalf("ordinary read: %d %v", n, err)
	}
	a.ReturnView()
	if m.account.loaned.Load() != 0 || m.account.bytes.Load() != 0 {
		t.Fatalf("after release: %d loaned, %d resident", m.account.loaned.Load(), m.account.bytes.Load())
	}
}

func TestReceiveQueueAllocations(t *testing.T) {
	for _, size := range []int{1, 4095, 4096, 4097, 16 << 10} {
		m := recvMemory{}
		rb := recvBuf{memory: &m}
		b := NewBuf(make([]byte, size))
		dst := make([]byte, size)
		n := testing.AllocsPerRun(100, func() {
			const offset = 0
			rb.insert(offset, b, 0, size)
			if n, _, err := rb.consume(offset, dst, len(dst), false); err != nil || n != size {
				panic("failed to consume receive interval")
			}
		})
		b.Unref()
		if n != 0 {
			t.Errorf("payload %d: %g allocations per record", size, n)
		}
	}
}

func TestCompactPagesShareCapacityAndMoveOnce(t *testing.T) {
	var m recvMemory
	var queues [8]recvBuf
	for i := range queues {
		q := &queues[i]
		q.memory = &m
		source := NewBuf(make([]byte, 4096))
		source.B[0] = byte(i)
		q.insert(0, source, 0, 1)
		source.Unref()
	}
	m.compact(nil)
	if m.queued != compactSlot || m.live != uint64(len(queues)) || m.compacted != m.live || len(m.roots) != 1 {
		t.Fatalf("%d bytes in %d roots, %d live bytes, %d bytes copied", m.queued, len(m.roots), m.live, m.compacted)
	}
	m.compact(nil)
	if m.compacted != uint64(len(queues)) {
		t.Fatalf("copied %d bytes after a second pressure pass", m.compacted)
	}
	var dst [1]byte
	for i := range queues {
		n, _, err := queues[i].consume(0, dst[:], 1, false)
		if n != 1 || err != nil || dst[0] != byte(i) {
			t.Fatalf("queue %d returned %v (%d, %v)", i, dst, n, err)
		}
	}
	if m.queued != 0 || m.account.bytes.Load() != 0 {
		t.Fatal("a compact page stayed charged after its last range")
	}
}
