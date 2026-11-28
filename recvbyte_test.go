package zmux

import (
	"bytes"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
)

func TestByteReceiverReusesUnpublishedBulkTail(t *testing.T) {
	var r byteReceiver
	defer r.release()

	read := func(b []byte) core.Input {
		t.Helper()
		n, input, err := r.read(bytes.NewReader(b))
		if n != len(b) || err != nil {
			t.Fatalf("read %d bytes, want %d: %v", n, len(b), err)
		}
		return input
	}

	idle := make([]byte, core.RecvIdleSize)
	read(idle)
	r.advance(0) // The record needs more than the idle block.

	body := bytes.Repeat([]byte{0x53}, 2*core.RecvIdleSize)
	input := read(body)
	if input.Second.Buf == nil {
		t.Fatal("read did not complete across the carry and bulk block")
	}
	published := input.Second
	published.Buf.Ref()
	defer published.Buf.Unref()
	r.advance(input.Len())

	next := read([]byte{0xab})
	if next.First.Buf != published.Buf || next.First.Start != published.End {
		t.Fatal("next read did not append to the unpublished tail")
	}
	if !bytes.Equal(published.Bytes(), body) {
		t.Fatal("next read overwrote published bytes")
	}
	r.advance(next.Len())
	if next := read([]byte{0x07}); len(next.First.Buf.B) != core.RecvIdleSize {
		t.Fatal("short record did not return the receiver to the idle class")
	}
}

func TestByteReceiverRewindsExclusiveBulkBacking(t *testing.T) {
	var r byteReceiver
	defer r.release()

	b := core.GetRecvBuf(core.RecvBlockSize)
	r.current = b.Span(0, core.RecvBlockSize)
	r.advance(core.RecvBlockSize)
	if r.current.Buf != b || r.current.End != 0 || r.current.Start != 0 {
		t.Fatal("receiver did not rewind its exclusive backing")
	}

	n, input, err := r.read(bytes.NewReader([]byte{0x44}))
	if n != 1 || err != nil || input.First.Buf != b || input.First.Start != 0 {
		t.Fatalf("read after rewind returned %d bytes, %v", n, err)
	}
}
