package streamio

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// DatagramEndpoint is a QUIC-based connection, session or request stream.
type DatagramEndpoint interface {
	Context() context.Context
	SendDatagram([]byte) error
	ReceiveDatagram(context.Context) ([]byte, error)
}

// A successful probe would send a datagram. Use a length no QUIC packet can
// carry so quic-go always rejects it before queuing anything.
var datagramProbe = make([]byte, 1<<16)

// DatagramMTU caches the observed payload limit for one endpoint.
type DatagramMTU struct {
	size  carrierio.DatagramSize
	limit atomic.Int64 // declared once for the endpoint
}

func datagramLimit(err error, overhead int) (int64, bool) {
	tooLarge, ok := errors.AsType[*quic.DatagramTooLargeError](err)
	if !ok {
		return 0, false
	}
	return tooLarge.MaxDatagramPayloadSize - int64(overhead), true
}

// Capacity returns the fixed service capacity and current room. quic-go's
// size error combines peer and path bounds, so the adapter declares the first
// usable room as its service capacity. Later probes update room only.
func (m *DatagramMTU) Capacity(send func([]byte) error, overhead int, enabled func() bool) (limit, room int) {
	room = m.size.Get(func() (int64, bool) {
		if enabled != nil && !enabled() {
			return 0, false
		}
		return datagramLimit(send(datagramProbe), overhead)
	})
	if room > 0 && m.limit.Load() == 0 {
		m.limit.CompareAndSwap(0, int64(room))
	}
	limit = int(m.limit.Load())
	return limit, min(limit, room)
}

// Changed signals an observed change to the cached MTU.
func (m *DatagramMTU) Changed() <-chan struct{} { return m.size.Changed() }

// Send forwards a datagram and corrects the cached limit on a size rejection.
func (m *DatagramMTU) Send(send func([]byte) error, overhead int, payload []byte) error {
	err := send(payload)
	if n, ok := datagramLimit(err, overhead); ok {
		m.size.Set(n)
	}
	return err
}

// ReceiveDatagrams runs a single receive loop until its endpoint ends.
func ReceiveDatagrams(ctx context.Context, endpoint DatagramEndpoint, deliver func([]byte)) {
	for {
		b, err := endpoint.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		deliver(b)
	}
}

// DatagramRegistry shares one cached MTU and one receiver per live endpoint.
type DatagramRegistry[T interface {
	comparable
	DatagramEndpoint
}] struct {
	enabled  func(T) bool
	states   sync.Map // T -> *DatagramChannel
	label    string
	overhead int
}

// NewDatagramRegistry wires a carrier's framing overhead and negotiated support.
func NewDatagramRegistry[T interface {
	comparable
	DatagramEndpoint
}](overhead int, label string, enabled func(T) bool) *DatagramRegistry[T] {
	return &DatagramRegistry[T]{overhead: overhead, label: label, enabled: enabled}
}

// Channel resolves the channel of endpoint, created on first use.
func (r *DatagramRegistry[T]) Channel(endpoint T) *DatagramChannel {
	if c, ok := r.states.Load(endpoint); ok {
		return c.(*DatagramChannel)
	}
	c, loaded := r.states.LoadOrStore(endpoint, &DatagramChannel{
		endpoint: endpoint, label: r.label, overhead: r.overhead, enabled: func() bool { return r.enabled(endpoint) },
	})
	if !loaded {
		context.AfterFunc(endpoint.Context(), func() { r.states.CompareAndDelete(endpoint, c) })
	}
	return c.(*DatagramChannel)
}

// Dropped counts received datagrams that named no registered pipe.
func (r *DatagramRegistry[T]) Dropped(endpoint T) uint64 {
	if c, ok := r.states.Load(endpoint); ok {
		return c.(*DatagramChannel).demux.Dropped()
	}
	return 0
}

// DatagramChannel routes the datagrams of an endpoint to its pipes.
type DatagramChannel struct {
	endpoint DatagramEndpoint
	enabled  func() bool
	label    string
	demux    carrier.ChannelDemux
	mtu      DatagramMTU
	overhead int
	recvOnce sync.Once
}

// Capacity returns the endpoint's declared capacity and current room.
func (c *DatagramChannel) Capacity() (limit, room int) {
	return c.mtu.Capacity(c.endpoint.SendDatagram, c.overhead, c.enabled)
}

// Changed signals an observed change to the endpoint's payload limit.
func (c *DatagramChannel) Changed() <-chan struct{} { return c.mtu.Changed() }

// Send sends a datagram through the shared endpoint.
func (c *DatagramChannel) Send(payload []byte) error {
	return c.mtu.Send(c.endpoint.SendDatagram, c.overhead, payload)
}

// Register routes datagrams for one pipe and starts the receiver on first use.
func (c *DatagramChannel) Register(id uint64, deliver func([]byte)) func() {
	stop := c.demux.Register(id, deliver)
	c.recvOnce.Do(func() {
		if c.enabled() {
			labels.Go(context.Background(), c.label, func() {
				ReceiveDatagrams(c.endpoint.Context(), c.endpoint, c.demux.Deliver)
			})
		}
	})
	return stop
}

// SupportsDatagrams reports whether both QUIC endpoints enabled datagrams.
func SupportsDatagrams(st quic.ConnectionState) bool {
	return st.SupportsDatagrams.Local && st.SupportsDatagrams.Remote
}
