package zmux

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// batchCarrier is a message carrier with WriteMessages, counting calls.
type batchCarrier struct {
	*msgCarrier
	batches, msgs, single atomic.Int64
	firstBatch            atomic.Int64           // messages in the first call
	first                 atomic.Pointer[[]byte] // their concatenation
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
		c, err := Start(ctx, PipeIO{R: by, W: by, Framing: FramingRecords}, RoleServer, mpConfig())
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
