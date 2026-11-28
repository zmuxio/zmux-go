package zmuxh3

import (
	"net/http"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxquic/streamio"
)

// HTTP3Handler publishes the connection's carrier statistics for streaming
// requests. The server must set ConnContext to QUICConnContext. The
// classification uses the options of the wrapped zmuxhttp handler.
func HTTP3Handler(h http.Handler, o *zmuxhttp.Options) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := quicInContext(r.Context())
		if class := o.Classify(r); state.qc != nil &&
			(class == zmuxhttp.ClassDownlink || class == zmuxhttp.ClassUplink || class == zmuxhttp.ClassBidirectional) {
			w = &h3CarrierWriter{ResponseWriter: w, state: state}
		}
		h.ServeHTTP(w, r)
	})
}

type h3CarrierWriter struct {
	http.ResponseWriter
	state quicState
}

func (w *h3CarrierWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *h3CarrierWriter) PhysicalCarrier() any {
	if w.state.qc != nil {
		return w.state.qc
	}
	return nil
}

// CarrierSampler is for internal use by zmux modules.
func (w *h3CarrierWriter) CarrierSampler() *carrierstats.Sampler { return w.state.CarrierSampler() }

// ---- WebTransport ----

// Session.SendDatagram reports capacity after HTTP/3 framing overhead.
var wtChannels = streamio.NewDatagramRegistry(0, "wt-channel", func(sess *Session) bool {
	return sess.Context().Err() == nil && streamio.SupportsDatagrams(sess.state)
})

// ChannelStreamID is the stream's ID within its QUIC connection.
func (w *wtStream) ChannelStreamID() (uint64, bool) { return uint64(w.s.StreamID()), true }

// ChannelDatagramCapacity returns the session's limit and current room.
func (w *wtStream) ChannelDatagramCapacity() (limit, room int) {
	return wtChannels.Channel(w.sess).Capacity()
}

// ChannelCapacityChanged wakes writers sharing a WebTransport MTU.
func (w *wtStream) ChannelCapacityChanged() <-chan struct{} {
	return wtChannels.Channel(w.sess).Changed()
}

// SendChannelDatagram sends a datagram of the session.
func (w *wtStream) SendChannelDatagram(b []byte) error { return wtChannels.Channel(w.sess).Send(b) }

// ReceiveChannelDatagrams receives the session's datagrams for the pipe.
func (w *wtStream) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	return wtChannels.Channel(w.sess).Register(uint64(w.s.StreamID()), deliver)
}

// ChannelDropped returns the datagrams received on sess that
// named no pipe attached to a session.
func ChannelDropped(sess *Session) uint64 {
	return wtChannels.Dropped(sess)
}
