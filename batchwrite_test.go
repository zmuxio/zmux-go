package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

type contiguousTestWriter struct {
	t     *testing.T
	want  []byte
	first *byte
	calls int
}

func (*contiguousTestWriter) PreferFlatWrite() {}

func (w *contiguousTestWriter) WriteBuffers([][]byte) (int, error) {
	w.t.Error("carrier was given scatter buffers")
	return 0, io.ErrUnexpectedEOF
}

func (w *contiguousTestWriter) Write(p []byte) (int, error) {
	w.calls++
	if !bytes.Equal(p, w.want) {
		w.t.Errorf("written bytes %x, want %x", p, w.want)
	}
	if len(p) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if w.first == nil {
		w.first = &p[0]
	} else if w.first != &p[0] {
		w.t.Error("second write did not reuse batch capacity")
	}
	return len(p) - 1, io.ErrClosedPipe
}

func TestContiguousBatchWriteKeepsPartialResult(t *testing.T) {
	s := core.New(core.DefaultConfig(), 0)
	s.AttachBootstrap(core.CarrierInfo{}, core.CarrierInfo{}, nil)
	var b core.Batch
	defer b.Discard()
	if !s.PollTransmit(0, 0, &b, core.DefaultBatch, 0) || len(b.Bufs()) < 2 {
		t.Fatal("initial batch needs multiple buffers")
	}
	w := &contiguousTestWriter{t: t, want: b.Flat()}
	for range 2 {
		n, err := writeBatch(w, FramingByteStream, &b, nil, nil)
		if n != b.Len()-1 || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("write result = (%d, %v), want (%d, %v)", n, err, b.Len()-1, io.ErrClosedPipe)
		}
	}
	if w.calls != 2 {
		t.Fatalf("%d carrier writes for two batches", w.calls)
	}
}

// batchCarrier is a message carrier with WriteMessages, counting calls.
type batchCarrier struct {
	*msgCarrier
	first                 atomic.Pointer[[]byte] // their concatenation
	wrote                 chan struct{}          // messages in the first call
	batches, msgs, single atomic.Int64
	firstBatch            atomic.Int64
}

func (b *batchCarrier) WriteMessages(msgs [][]byte) error {
	if b.batches.Add(1) == 1 {
		b.firstBatch.Store(int64(len(msgs)))
		cat := bytes.Join(msgs, nil)
		b.first.Store(&cat)
		if b.wrote != nil {
			close(b.wrote)
		}
	}
	b.msgs.Add(int64(len(msgs)))
	for _, m := range msgs {
		if err := b.msgCarrier.WriteMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (b *batchCarrier) WriteMessage(p []byte) error {
	b.single.Add(1)
	return b.msgCarrier.WriteMessage(p)
}

// A message-framed half with WriteMessages gets every batch in one call.
func TestMessageBatchWriter(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	x, y := msgCarriers()
	bx, by := &batchCarrier{msgCarrier: x}, &batchCarrier{msgCarrier: y}
	type res struct {
		c   *Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := startServerNoListener(ctx, PipeIO{R: by, W: by, Framing: FramingRecords}, mpConfig())
		ch <- res{c, err}
	}()
	c, err := Start(ctx, PipeIO{R: bx, W: bx, Framing: FramingRecords}, RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	s := r.c
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	for _, b := range []*batchCarrier{bx, by} {
		if b.batches.Load() == 0 || b.single.Load() != 0 || b.msgs.Load() < b.batches.Load() {
			t.Fatalf("batches %d, messages %d, single writes %d", b.batches.Load(), b.msgs.Load(), b.single.Load())
		}
	}
}

// msgCarrier is one end of an in-memory message-framed carrier.
type msgCarrier struct {
	in, out chan []byte
	once    *sync.Once
	done    chan struct{}
}

func msgCarriers() (*msgCarrier, *msgCarrier) {
	x, y, once, done := make(chan []byte, 8), make(chan []byte, 8), new(sync.Once), make(chan struct{})
	return &msgCarrier{in: x, out: y, once: once, done: done}, &msgCarrier{in: y, out: x, once: once, done: done}
}

func (m *msgCarrier) ReadMessage(limit int) (ReceivedMessage, error) {
	var b []byte
	select {
	case b = <-m.in:
	case <-m.done:
		select {
		case b = <-m.in:
		default:
			return ReceivedMessage{}, io.EOF
		}
	}
	if len(b) > limit {
		return ReceivedMessage{}, ErrMessageTooLarge
	}
	return OwnMessage(b), nil
}

func (m *msgCarrier) WriteMessage(p []byte) error {
	select {
	case <-m.done:
		return net.ErrClosed
	default:
	}
	m.out <- bytes.Clone(p)
	return nil
}

func (m *msgCarrier) Read([]byte) (int, error)  { return 0, errors.New("message carrier") }
func (m *msgCarrier) Write([]byte) (int, error) { return 0, errors.New("message carrier") }
func (m *msgCarrier) Close() error              { m.once.Do(func() { close(m.done) }); return nil }

// TestOversizedMessageFailsPipe: a message above MaxRecordSize fails the
// pipe that carried it; the session reads no more of it.
func TestOversizedMessageFailsPipe(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	x, y := msgCarriers()
	ch := make(chan *Conn, 1)
	go func() {
		s, err := startServerNoListener(ctx, PipeIO{R: y, W: y, Framing: FramingRecords}, nil)
		if err != nil {
			s = nil
		}
		ch <- s
	}()
	c, err := Start(ctx, PipeIO{R: x, W: x, Framing: FramingRecords}, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := <-ch
	if s == nil {
		t.Fatal("no server session")
	}
	defer func() { _ = c.Close(); waitDone(t, c); waitDone(t, s) }()
	x.out <- make([]byte, MaxRecordSize+1)
	waitDone(t, s)
	if err := s.Wait(); err == nil || !errors.Is(err, ErrMessageTooLarge) && !strings.Contains(err.Error(), "lost") {
		t.Fatalf("the session ended with %v", err)
	}
}
