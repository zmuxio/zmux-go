package zmuxh3

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
)

func dgCfg() *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, PipeAuth: zmux.PipeAuth{PSK: make([]byte, 32), EndToEnd: true}}
}

// warmUp sends datagrams from a to b until one goes on the channel (the
// first ones may go in records while the carrier negotiates its
// datagrams).
func warmUp(t *testing.T, a, b *zmux.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for a.ConnectionStats().DatagramsSentChannel == 0 {
		if err := a.SendDatagram([]byte("warm")); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ReceiveDatagram(ctx); err != nil {
			t.Fatalf("channel not ready: %v\nsender %+v\nreceiver %+v", err, a.ConnectionStats(), b.ConnectionStats())
		}
	}
}

// roundTrip sends n datagrams each way and checks that they arrived, all
// on the channel.
func roundTrip(t *testing.T, c, s *zmux.Conn, tag string) {
	t.Helper()
	warmUp(t, c, s)
	warmUp(t, s, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs0, ss0 := c.ConnectionStats(), s.ConnectionStats()
	const n = 20
	for i := range n {
		if err := c.SendDatagramWait(ctx, fmt.Appendf(nil, "%s up %d", tag, i)); err != nil {
			t.Fatal(err)
		}
		if err := s.SendDatagramWait(ctx, fmt.Appendf(nil, "%s down %d", tag, i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []struct {
		r   *zmux.Conn
		dir string
	}{{s, "up"}, {c, "down"}} {
		seen := map[string]bool{}
		for len(seen) < n {
			p, err := d.r.ReceiveDatagram(ctx)
			if err != nil {
				t.Fatalf("%s %s: %d of %d: %v", tag, d.dir, len(seen), n, err)
			}
			if string(p) == "warm" {
				continue
			}
			seen[string(p)] = true
		}
		for i := range n {
			if !seen[fmt.Sprintf("%s %s %d", tag, d.dir, i)] {
				t.Fatalf("%s %s %d missing", tag, d.dir, i)
			}
		}
	}
	cs, ss := c.ConnectionStats(), s.ConnectionStats()
	if cs.DatagramsSentChannel-cs0.DatagramsSentChannel != n || cs.DatagramsReceivedChannel-cs0.DatagramsReceivedChannel != n ||
		ss.DatagramsSentChannel-ss0.DatagramsSentChannel != n || ss.DatagramsReceivedChannel-ss0.DatagramsReceivedChannel != n {
		t.Fatalf("%s: client %+v\nserver %+v", tag, cs, ss)
	}
}

func TestWebTransportChannel(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: dgCfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: testQUIC()}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	wh, err := Handler(ws, lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	h3.Handler = wh
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	url := "https://" + pc.LocalAddr().String() + "/zmux"
	c, sess, err := Dial(ctx, tr, url, nil, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A second zmux session on another stream of the same WebTransport
	// session: the session's datagrams are routed by Pipe Stream ID.
	p2, err := OpenPipe(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := zmux.Start(ctx, p2, zmux.RoleClient, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	s2, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, s, "first")
	roundTrip(t, c2, s2, "second")
	if WebTransportChannelDropped(sess) != 0 {
		t.Fatal("datagrams dropped")
	}
	for _, x := range []*zmux.Conn{c, c2, s, s2} {
		_ = x.Close()
	}
}

// rawH3 is a bidirectional carrier over an HTTP/3 POST written directly on
// a QUIC connection, with the request stream's HTTP Datagrams as its
// datagram channel: quic-go's HTTP/3 client offers no streaming POST on a
// stream whose datagrams it exposes, and consumes the connection's QUIC
// datagrams itself.
type rawH3 struct {
	qc     *quic.Conn
	str    *quic.Stream
	rest   uint64 // bytes left in the current DATA frame
	header bool   // the response HEADERS frame was read
}

// dialRawH3 sends SETTINGS with SETTINGS_H3_DATAGRAM and a POST to
// authority without a Content-Length (a bidirectional carrier).
func dialRawH3(ctx context.Context, qc *quic.Conn, authority string) (*rawH3, error) {
	ctl, err := qc.OpenUniStream()
	if err != nil {
		return nil, err
	}
	settings := []byte{0x33, 0x01}
	if _, err := ctl.Write(append([]byte{0x00, 0x04, byte(len(settings))}, settings...)); err != nil {
		return nil, err
	}
	str, err := qc.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	// A QPACK field section of literal field lines with literal names
	// (RFC 9204 4.5.6), no dynamic table: prefix 0, 0.
	block := []byte{0x00, 0x00}
	for _, f := range [][2]string{{":method", "POST"}, {":scheme", "https"}, {":authority", authority}, {":path", "/"}} {
		block = appendPrefixInt(block, 0x20, 3, len(f[0]))
		block = append(block, f[0]...)
		block = appendPrefixInt(block, 0x00, 7, len(f[1]))
		block = append(block, f[1]...)
	}
	h := quicvarint.Append([]byte{0x01}, uint64(len(block)))
	if _, err := str.Write(append(h, block...)); err != nil {
		return nil, err
	}
	return &rawH3{qc: qc, str: str}, nil
}

// appendPrefixInt appends v as an integer with an n-bit prefix (RFC 7541
// 5.1) after the bits of first.
func appendPrefixInt(b []byte, first byte, n uint, v int) []byte {
	m := 1<<n - 1
	if v < m {
		return append(b, first|byte(v))
	}
	b = append(b, first|byte(m))
	for v -= m; v >= 128; v >>= 7 {
		b = append(b, byte(v%128+128))
	}
	return append(b, byte(v))
}

// Write sends p as a DATA frame.
func (r *rawH3) Write(p []byte) (int, error) {
	f := quicvarint.Append([]byte{0x00}, uint64(len(p)))
	if _, err := r.str.Write(append(f, p...)); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (r *rawH3) Close() error { return r.str.Close() }

// Read returns the response's DATA, skipping its HEADERS frame.
func (r *rawH3) Read(p []byte) (int, error) {
	br := quicvarint.NewReader(r.str)
	for r.rest == 0 {
		t, err := quicvarint.Read(br)
		if err != nil {
			return 0, err
		}
		l, err := quicvarint.Read(br)
		if err != nil {
			return 0, err
		}
		if t == 0x00 {
			r.rest = l
			continue
		}
		if t == 0x01 {
			r.header = true
		}
		if _, err := io.CopyN(io.Discard, r.str, int64(l)); err != nil {
			return 0, err
		}
	}
	n, err := r.str.Read(p[:min(uint64(len(p)), r.rest)])
	r.rest -= uint64(n)
	return n, err
}

// The request stream's HTTP Datagrams: the Quarter Stream ID, then the
// channel datagram.
func (r *rawH3) qsid() byte                      { return byte(r.str.StreamID() / 4) }
func (r *rawH3) ChannelStreamID() (uint64, bool) { return 0, false }
func (r *rawH3) MaxChannelDatagram() int         { return 1000 }
func (r *rawH3) SendChannelDatagram(b []byte) error {
	return r.qc.SendDatagram(append([]byte{r.qsid()}, b...))
}
func (r *rawH3) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	ctx, cancel := context.WithCancel(r.qc.Context())
	go func() {
		for {
			b, err := r.qc.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			if len(b) > 0 && b[0] == r.qsid() {
				deliver(b[1:])
			}
		}
	}()
	return cancel
}

func TestHTTP3Channel(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: dgCfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	hh, err := zmux.NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewHTTP3Server("", srvTLS, testQUIC(), hh, nil)
	go func() { _ = srv.Serve(pc) }()
	defer func() { _ = srv.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, pc.LocalAddr().String(), cliTLS, testQUIC())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	rh, err := dialRawH3(ctx, qc, pc.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, zmux.PipeIO{R: rh, W: rh}, zmux.RoleClient, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, s, "h3")
	echo(t, c, s)
	if !rh.header {
		t.Fatal("no response header")
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

// A client without the channel gets datagrams in records.
func TestHTTP3ChannelFallback(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: dgCfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	hh, err := zmux.NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewHTTP3Server("", srvTLS, testQUIC(), hh, nil)
	go func() { _ = srv.Serve(pc) }()
	defer func() { _ = srv.Close() }()
	client, tr := HTTPClient(cliTLS, &quic.Config{HandshakeIdleTimeout: 30 * time.Second})
	defer func() { _ = tr.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := zmux.HTTPPipe(ctx, client, "https://"+pc.LocalAddr().String()+"/", nil, zmux.DirBidirectional, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := s.SendDatagramWait(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if p, err := c.ReceiveDatagram(ctx); err != nil || len(p) != 1 || p[0] != byte(i) {
			t.Fatalf("%v %v", p, err)
		}
	}
	if st := s.ConnectionStats(); st.DatagramsSentChannel != 0 || st.DatagramsSent != 5 {
		t.Fatalf("%+v", st)
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

// testQUIC is a QUIC configuration with a handshake timeout that tolerates
// a loaded test host: the default 5 s fails handshakes under -race with
// many packages running at once.
func testQUIC() *quic.Config {
	return &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true,
		HandshakeIdleTimeout: 30 * time.Second}
}
