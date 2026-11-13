package zmuxh3

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// Datagram channels: the HTTP Datagrams (RFC 9297) of
// an HTTP/3 request stream that is a pipe, the frame alone; and the
// datagrams of a WebTransport session, each starting with the Pipe Stream
// ID of a stream of the session, routed by one receive loop per session.

// sizeProbe is larger than any QUIC datagram: sending it is always refused
// with the connection's current maximum, and nothing is sent.
var sizeProbe = make([]byte, 1<<16)

// sizeRefresh bounds how stale a channel's maximum may be.
const sizeRefresh = time.Second

var epoch = time.Now()

// dgramSize is a channel's current maximum, learned from refused sends.
type dgramSize struct {
	max      atomic.Int64
	probedAt atomic.Int64
}

// get returns the maximum less overhead, probing at most once per
// sizeRefresh; probe sends sizeProbe, which a connection without datagrams
// refuses for another reason.
func (d *dgramSize) get(overhead int, probe func() error) int {
	now := int64(time.Since(epoch))
	if last := d.probedAt.Load(); last != 0 && now-last < int64(sizeRefresh) {
		return int(d.max.Load())
	}
	if !d.learn(probe(), overhead) {
		d.max.Store(0)
	}
	// After the result: a concurrent caller that sees a recent probe reads it.
	d.probedAt.Store(now)
	return int(d.max.Load())
}

// learn takes the maximum from a send refused for size.
func (d *dgramSize) learn(err error, overhead int) bool {
	tl, ok := errors.AsType[*quic.DatagramTooLargeError](err)
	if ok {
		d.max.Store(max(tl.MaxDatagramPayloadSize-int64(overhead), 0))
	}
	return ok
}

// ---- HTTP/3 request streams ----

// h3Stream is an HTTP/3 request stream with HTTP Datagrams (a server's
// *http3.Stream or a client's *http3.RequestStream).
type h3Stream interface {
	SendDatagram(b []byte) error
	ReceiveDatagram(ctx context.Context) ([]byte, error)
	StreamID() quic.StreamID
	Context() context.Context
}

// h3Channel is the datagram channel of an HTTP/3 request stream: usable
// once the peer's SETTINGS enabled HTTP Datagrams.
type h3Channel struct {
	str      h3Stream
	settings http3.Settingser
	size     dgramSize
}

// newH3Channel returns the channel of str, whose HTTP/3 connection's
// settings are s.
func newH3Channel(str h3Stream, s http3.Settingser) *h3Channel {
	return &h3Channel{str: str, settings: s}
}

// ChannelStreamID reports no stream ID: the HTTP Datagram names the
// request stream itself.
func (c *h3Channel) ChannelStreamID() (uint64, bool) { return 0, false }

// MaxChannelDatagram is the largest HTTP Datagram payload the stream takes.
func (c *h3Channel) MaxChannelDatagram() int {
	select {
	case <-c.settings.ReceivedSettings():
	default:
		return 0
	}
	if !c.settings.Settings().EnableDatagrams {
		return 0
	}
	return c.size.get(c.overhead(), func() error { return c.str.SendDatagram(sizeProbe) })
}

// overhead is the Quarter Stream ID before the payload.
func (c *h3Channel) overhead() int { return quicvarint.Len(uint64(c.str.StreamID()) / 4) }

// SendChannelDatagram sends an HTTP Datagram of the stream.
func (c *h3Channel) SendChannelDatagram(b []byte) error {
	err := c.str.SendDatagram(b)
	c.size.learn(err, c.overhead())
	return err
}

// ReceiveChannelDatagrams receives the stream's HTTP Datagrams.
func (c *h3Channel) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	ctx, cancel := context.WithCancel(c.str.Context())
	labels.Go(context.Background(), "h3-channel", func() {
		for {
			b, err := c.str.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			deliver(b)
		}
	})
	return cancel
}

// HTTP3Handler wraps h, typically the handler of zmuxhttp.NewHandler, for
// an HTTP/3 server with HTTP Datagrams enabled (http3.Server's
// EnableDatagrams, as NewServer sets): a bidirectional carrier's
// request stream offers its HTTP Datagrams as the pipe's datagram channel
// (zmuxhttp.DatagramChannel). Other requests pass unchanged. o (nil:
// defaults) are the zmuxhttp.Options of the zmux handler inside h, whose pairing
// field decides which requests are bidirectional carriers.
func HTTP3Handler(h http.Handler, o *zmuxhttp.Options) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hs, ok := w.(http3.HTTPStreamer)
		st, sok := w.(http3.Settingser)
		if !ok || !sok || o.Classify(r) != zmuxhttp.ClassBidirectional {
			h.ServeHTTP(w, r)
			return
		}
		cw := &h3ChanWriter{ResponseWriter: w, hs: hs, st: st}
		h.ServeHTTP(cw, r)
		if str := cw.str; str != nil {
			// Taken over by HTTPStream: end it as the server would.
			str.CancelRead(quic.StreamErrorCode(http3.ErrCodeNoError))
			_ = str.Close()
		}
	})
}

// h3ChanWriter is a response writer that provides the request stream's
// datagram channel.
type h3ChanWriter struct {
	http.ResponseWriter
	hs  http3.HTTPStreamer
	st  http3.Settingser
	mu  sync.Mutex
	str *http3.Stream
}

func (w *h3ChanWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// DatagramChannel takes over the stream (the response header went out) and
// returns its channel.
func (w *h3ChanWriter) DatagramChannel() zmux.DatagramChannel {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.str == nil {
		w.str = w.hs.HTTPStream()
	}
	return newH3Channel(w.str, w.st)
}

// ---- WebTransport ----

// wtChannel is the datagram channel of one WebTransport session.
type wtChannel struct {
	sess     *webtransport.Session
	demux    carrier.ChannelDemux
	recvOnce sync.Once
	size     dgramSize
}

var wtChannels sync.Map // *webtransport.Session -> *wtChannel

func wtChannelOf(sess *webtransport.Session) *wtChannel {
	if c, ok := wtChannels.Load(sess); ok {
		return c.(*wtChannel)
	}
	c, loaded := wtChannels.LoadOrStore(sess, &wtChannel{sess: sess})
	if !loaded {
		context.AfterFunc(sess.Context(), func() { wtChannels.Delete(sess) })
	}
	return c.(*wtChannel)
}

// wtOverhead bounds the Quarter Stream ID of the session's CONNECT stream
// that precedes each of its datagrams.
const wtOverhead = 8

func (c *wtChannel) maxDatagram() int {
	st := c.sess.SessionState().ConnectionState
	if !st.SupportsDatagrams.Local || !st.SupportsDatagrams.Remote {
		return 0
	}
	return c.size.get(wtOverhead, func() error { return c.sess.SendDatagram(sizeProbe) })
}

func (c *wtChannel) send(b []byte) error {
	err := c.sess.SendDatagram(b)
	c.size.learn(err, wtOverhead)
	return err
}

func (c *wtChannel) register(id uint64, deliver func([]byte)) func() {
	c.recvOnce.Do(func() {
		labels.Go(context.Background(), "wt-channel", func() {
			for {
				b, err := c.sess.ReceiveDatagram(c.sess.Context())
				if err != nil {
					return
				}
				c.demux.Deliver(b)
			}
		})
	})
	return c.demux.Register(id, deliver)
}

// ChannelStreamID is the stream's ID within its QUIC connection.
func (w *wtStream) ChannelStreamID() (uint64, bool) { return uint64(w.s.StreamID()), true }

// MaxChannelDatagram is the session's current largest datagram.
func (w *wtStream) MaxChannelDatagram() int { return wtChannelOf(w.sess).maxDatagram() }

// SendChannelDatagram sends a datagram of the session.
func (w *wtStream) SendChannelDatagram(b []byte) error { return wtChannelOf(w.sess).send(b) }

// ReceiveChannelDatagrams receives the session's datagrams for the pipe.
func (w *wtStream) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	return wtChannelOf(w.sess).register(uint64(w.s.StreamID()), deliver)
}

// ChannelDropped returns the datagrams received on sess that
// named no pipe attached to a session.
func ChannelDropped(sess *webtransport.Session) uint64 {
	if c, ok := wtChannels.Load(sess); ok {
		return c.(*wtChannel).demux.Dropped()
	}
	return 0
}
