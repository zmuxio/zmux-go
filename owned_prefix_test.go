package zmux

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

func TestClassifiedPrefixTransfersBacking(t *testing.T) {
	body := wire.AppendTransportParams(nil, nil)
	record := varint.AppendVarint(nil, uint64(len(body)))
	record = append(record, body...)
	const cut = 5
	a := core.NewBuf(bytes.Clone(record[:cut]))
	b := core.NewBuf(append(bytes.Clone(record[cut:]), 2, 3, 4))
	released := 0
	a.Free = func(*core.Buf) { released++ }
	b.Free = a.Free
	p := PipeIO{R: io.NopCloser(bytes.NewReader(nil))}
	p = withOwnedPrefix(p, &bytePrefix{view: a.Span(0, len(a.B)), next: &bytePrefix{view: b.Span(0, len(b.B)), end: io.EOF}})
	f, class, err := readFirstRecord(p)
	if err != nil || class != core.ClassSession {
		t.Fatalf("class = %v, err = %v", class, err)
	}
	defer f.release()
	p = f.replay(p)
	var r byteReadState
	if _, input, err := r.read(p.R); err != nil || input.First.Buf != a || input.Len() != cut {
		t.Fatalf("first span = %+v, err = %v", input, err)
	}
	if _, input, err := r.read(p.R); !errors.Is(err, io.EOF) || input.First.Buf != a || input.Second.Buf != b ||
		!bytes.Equal(input.Slice(0, len(record)).Bytes().AppendTo(nil), record) {
		t.Fatalf("second span = %+v, err = %v", input, err)
	}
	r.advance(len(record))
	if released != 1 {
		t.Fatalf("released %d roots after consuming one record", released)
	}
	r.release()
	if released != 2 {
		t.Fatalf("released %d roots after closing", released)
	}
}
