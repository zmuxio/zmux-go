package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// batchCarrier is a message carrier with WriteMessages, counting calls.
type batchCarrier struct {
	*msgCarrier
	first                 atomic.Pointer[[]byte] // their concatenation
	batches, msgs, single atomic.Int64
	firstBatch            atomic.Int64 // messages in the first call
}

func (b *batchCarrier) WriteMessages(msgs [][]byte) error {
	if b.batches.Add(1) == 1 {
		b.firstBatch.Store(int64(len(msgs)))
		cat := bytes.Join(msgs, nil)
		b.first.Store(&cat)
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

func (m *msgCarrier) ReadMessage() ([]byte, error) {
	select {
	case b := <-m.in:
		return b, nil
	case <-m.done:
		select {
		case b := <-m.in:
			return b, nil
		default:
			return nil, io.EOF
		}
	}
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
