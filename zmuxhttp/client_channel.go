package zmuxhttp

import (
	"context"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// httpClientChanWriter exposes the request stream's channel once the final
// response is accepted. A redirect discards its old request and its channel.
type httpClientChanWriter struct{ *httpReqWriter }

func (w *httpClientChanWriter) channel() zmux.DatagramChannel {
	select {
	case <-w.call.done:
		if w.call.resp != nil {
			if p, ok := w.call.resp.Body.(interface{ DatagramChannel() zmux.DatagramChannel }); ok {
				return p.DatagramChannel()
			}
		}
	default:
	}
	return nil
}

func (w *httpClientChanWriter) ChannelStreamID() (uint64, bool) { return 0, false }

func (w *httpClientChanWriter) ChannelDatagramCapacity() (limit, room int) {
	return channelCapacity(w.call.done, w.channel)
}

func channelCapacity(open <-chan struct{}, current func() zmux.DatagramChannel) (limit, room int) {
	select {
	case <-open:
		if ch := current(); ch != nil {
			return ch.ChannelDatagramCapacity()
		}
		return 0, 0
	default:
		return -1, 0
	}
}

func (w *httpClientChanWriter) SendChannelDatagram(b []byte) error {
	return w.SendChannelDatagramUntil(b, time.Time{})
}

func (w *httpClientChanWriter) SendChannelDatagramUntil(b []byte, deadline time.Time) error {
	return carrierio.SendChannelDatagram(w.channel(), b, deadline)
}

func (w *httpClientChanWriter) ChannelCapacityChanged() <-chan struct{} {
	return channelChange(w.call.done, w.channel)
}

func channelChange(open <-chan struct{}, current func() zmux.DatagramChannel) <-chan struct{} {
	select {
	case <-open:
		return carrierio.ChannelCapacityChanged(current())
	default:
		return open
	}
}

func (w *httpClientChanWriter) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	labels.Go(ctx, "http-client-channel", func() {
		defer close(done)
		select {
		case <-w.call.done:
		case <-ctx.Done():
			return
		}
		if ch := w.channel(); ch != nil {
			stop := ch.ReceiveChannelDatagrams(deliver)
			defer stop()
			<-ctx.Done()
		}
	})
	return func() { cancel(); <-done }
}
