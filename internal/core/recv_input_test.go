package core

import (
	"bytes"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

func receiveTest(s *Session, pipe, lane uint64, data []byte, now Time) int {
	buf := NewBuf(data)
	defer buf.Unref()
	return s.Receive(pipe, lane, Input{First: buf.Span(0, len(data))}, now)
}

func receiveMessageTest(s *Session, pipe, lane uint64, data []byte, now Time) {
	buf := NewBuf(data)
	defer buf.Unref()
	s.ReceiveMessage(pipe, lane, buf.Span(0, len(data)), now)
}

func onStreamTest(s *Session, f *wire.Frame) *wire.Error {
	buf := NewBuf(f.Data)
	defer buf.Unref()
	_, err := s.onStream(f, Input{First: buf.Span(0, len(f.Data))})
	return err
}

func TestReceiveAcrossEveryBlockBoundary(t *testing.T) {
	payload := bytes.Repeat([]byte("zmux"), 40)
	body := []byte{0, 0, 0}
	body = wire.AppendStreamHeader(body, 1, 0, len(payload), true, true)
	payloadAt := len(body)
	body = append(body, payload...)
	record := varint.AppendVarint(nil, uint64(len(body)))
	payloadAt += len(record)
	record = append(record, body...)

	for split := 1; split < len(record); split++ {
		s, p := channelSession(t, nil, false)
		s.role, s.roleKnown = RoleInitiator, true
		p.in.lanes[0].expectTP = false
		st := s.newStream(true, false)
		st.id, st.idSet = 1, true
		st.rcv.adv = uint64(len(payload))
		s.streams[1] = st
		first := NewBuf(bytes.Clone(record[:split]))
		second := NewBuf(bytes.Clone(record[split:]))
		released := 0
		free := func(b *Buf) {
			released++
			clear(b.B)
		}
		first.Free, second.Free = free, free
		input := Input{First: first.Span(0, split)}
		if n := s.Receive(p.id, 0, input, s.now); n != 0 || st.rcv.buf.bytes != 0 {
			t.Fatalf("split %d: consumed an incomplete record", split)
		}
		input.Second = second.Span(0, len(record)-split)
		if n := s.Receive(p.id, 0, input, s.now); n != len(record) || p.in.lanes[0].pos != uint64(n) {
			t.Fatalf("split %d: consumed %d, position %d, error %v", split, n, p.in.lanes[0].pos, s.Err())
		}
		first.Unref()
		second.Unref()
		off := 0
		var last *Buf
		for off < len(payload) {
			view, _, err := s.ReadChunk(st, len(payload))
			data, root := view.Bytes(), view.Buf
			if err != nil || len(data) == 0 {
				t.Fatalf("split %d at %d: %v", split, off, err)
			}
			index := payloadAt + off
			want := second
			if index < split {
				want = first
			}
			if root != want || !bytes.Equal(data, payload[off:off+len(data)]) || cap(data) != len(data) {
				t.Fatalf("split %d at %d: loan does not reference the validated range", split, off)
			}
			if last != nil {
				last.ReturnView()
			}
			last = root
			off += len(data)
		}
		if _, fin, err := s.ReadChunk(st, 1); err != nil || !fin {
			t.Fatalf("split %d: fin %v, %v", split, fin, err)
		}
		if released != 1 {
			t.Fatalf("split %d: released %d roots before final loan ended", split, released)
		}
		last.ReturnView()
		if released != 2 || s.conn.rx.account.bytes.Load() != 0 {
			t.Fatalf("split %d: released %d roots, resident %d", split, released, s.conn.rx.account.bytes.Load())
		}
	}
}

func TestNestedReceiveAcrossForeignBackings(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.role, s.roleKnown = RoleInitiator, true
	p.in.lanes[0].expectTP = false
	st := s.newStream(true, false)
	st.id, st.idSet, st.rcv.adv = 1, true, 4096
	s.streams[1] = st

	// Retain disjoint bytes of the same allocations in the outer session
	// while the inner session receives a complete record one byte at a time.
	var outer recvMemory
	first, sibling := recvBuf{memory: &outer}, recvBuf{memory: &outer}
	payload := []byte("foreign")
	body := wire.AppendStreamHeader(nil, 1, 0, len(payload), false, true)
	body = append(body, payload...)
	record := append(varint.AppendVarint(nil, uint64(len(body))), body...)
	var parts ReceiveSpans
	defer parts.Release()
	releases := 0
	roots := make([]*Buf, len(record))
	for i, x := range record {
		b := NewBuf([]byte{x, 0xa5})
		b.Free = func(b *Buf) { releases++; clear(b.B) }
		first.insert(uint64(i), b, 0, 1)
		sibling.insert(uint64(i), b, 1, 1)
		b.Unref()
		_, v, err := first.consume(uint64(i), nil, 1, true)
		if err != nil {
			t.Fatal(err)
		}
		input := parts.Append(v, true)
		n := s.Receive(p.id, 0, input, s.now)
		if i+1 != len(record) && n != 0 {
			t.Fatalf("prefix %d published a partial record", i)
		}
		parts.Advance(n)
		roots[i] = b
	}
	if releases != 0 || s.conn.rx.account.bytes.Load() != 0 || outer.account.bytes.Load() != int64(2*len(record)) {
		t.Fatal("physical backing was charged twice or released early")
	}
	for i, want := range payload {
		v, _, err := s.ReadChunk(st, 1)
		b := roots[len(record)-len(payload)+i]
		if err != nil || v.Buf != b || len(v.Bytes()) != 1 || v.Bytes()[0] != want {
			t.Fatalf("byte %d lost its backing: %v", i, err)
		}
		v.Buf.ReturnView()
	}
	if releases != 0 || s.conn.rx.foreign != 0 || outer.account.loaned.Load() != 0 {
		t.Fatal("foreign queue did not release independently")
	}
	sibling.clear()
	if releases != len(record) || outer.account.bytes.Load() != 0 {
		t.Fatalf("released %d of %d", releases, len(record))
	}
}
