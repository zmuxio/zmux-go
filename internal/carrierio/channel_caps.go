package carrierio

import (
	"net"
	"time"
)

// DatagramDeadlineSender permits a channel to honor expiry while waiting for room.
type DatagramDeadlineSender interface {
	SendChannelDatagramUntil(b []byte, deadline time.Time) error
}

// DatagramCapacityNotifier signals when to read a fresh channel snapshot.
type DatagramCapacityNotifier interface {
	ChannelCapacityChanged() <-chan struct{}
}

// ChannelCapacityChanged forwards snapshot invalidation when the channel provides it.
func ChannelCapacityChanged(ch any) <-chan struct{} {
	if n, ok := ch.(DatagramCapacityNotifier); ok {
		return n.ChannelCapacityChanged()
	}
	return nil
}

// SendChannelDatagram forwards a datagram to the optional timed sender.
func SendChannelDatagram(ch interface{ SendChannelDatagram([]byte) error }, b []byte, expiry time.Time) error {
	if ch == nil {
		return net.ErrClosed
	}
	if d, ok := ch.(DatagramDeadlineSender); ok && !expiry.IsZero() {
		return d.SendChannelDatagramUntil(b, expiry)
	}
	return ch.SendChannelDatagram(b)
}
