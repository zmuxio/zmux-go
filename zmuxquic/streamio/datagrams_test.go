package streamio

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/carrier"
)

type datagramFixture struct {
	ctx     context.Context
	input   chan []byte
	reading chan struct{}
}

func (f *datagramFixture) Context() context.Context { return f.ctx }

func (f *datagramFixture) SendDatagram(b []byte) error {
	if len(b) > 1000 {
		return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 1200}
	}
	return nil
}

func (f *datagramFixture) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case f.reading <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case b := <-f.input:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestDatagramMTUSeparatesCapacityAndRoom(t *testing.T) {
	var mtu DatagramMTU
	current := int64(1200)
	send := func([]byte) error { return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: current} }
	if limit, room := mtu.Capacity(send, 0, nil); limit != 1200 || room != 1200 {
		t.Fatalf("limit=%d room=%d", limit, room)
	}
	changed := mtu.Changed()
	current = 900
	_ = mtu.Send(send, 0, nil)
	select {
	case <-changed:
	default:
		t.Fatal("room change did not notify")
	}
	if limit, room := mtu.Capacity(send, 0, nil); limit != 1200 || room != 900 {
		t.Fatalf("after shrink, limit=%d room=%d", limit, room)
	}
	current = 1350
	_ = mtu.Send(send, 0, nil)
	if limit, room := mtu.Capacity(send, 0, nil); limit != 1200 || room != 1200 {
		t.Fatalf("after growth, limit=%d room=%d", limit, room)
	}
}

func TestNextBatchTransportSharedDatagramRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	f := &datagramFixture{ctx: ctx, input: make(chan []byte, 2), reading: make(chan struct{}, 3)}
	registry := NewDatagramRegistry(1, "test-datagram", func(*datagramFixture) bool { return true })
	c := registry.Channel(f)
	if limit, room := c.Capacity(); registry.Channel(f) != c || limit != 1199 || room != limit {
		t.Fatal("a single endpoint must share its receiver and payload limit")
	}
	delivered := make(chan []byte, 1)
	stop := c.Register(4, func(b []byte) { delivered <- b })
	defer stop()
	stopOther := c.Register(8, func([]byte) { t.Error("wrong recipient") })
	defer stopOther()
	f.input <- carrier.AppendChannelDatagram(nil, true, 12, []byte("unknown"))
	f.input <- carrier.AppendChannelDatagram(nil, true, 4, []byte("known"))
	for range 3 {
		select {
		case <-f.reading:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if b := <-delivered; len(b) < 5 || string(b[len(b)-5:]) != "known" || registry.Dropped(f) != 1 {
		t.Fatalf("delivered %q, dropped %d", b, registry.Dropped(f))
	}
	err := c.Send(make([]byte, 1200))
	if _, ok := errors.AsType[*quic.DatagramTooLargeError](err); !ok {
		t.Fatalf("refused send: %v", err)
	}
}
