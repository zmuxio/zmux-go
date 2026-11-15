package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/testscale"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// chanConn is a net.Conn with a datagram channel to its peer, as a QUIC
// stream has on its connection: channel datagrams start with the Pipe
// Stream ID id and go to the peer's demultiplexer.
type chanConn struct {
	net.Conn
	peer  *chanConn
	demux carrierio.ChannelDemux
	id    uint64
	max   atomic.Int64
	sent  atomic.Int64
	drop  atomic.Bool // lose every channel datagram
}

func (c *chanConn) ChannelStreamID() (uint64, bool) { return c.id, true }
func (c *chanConn) MaxChannelDatagram() int         { return int(c.max.Load()) }
func (c *chanConn) SendChannelDatagram(b []byte) error {
	if len(b) > c.MaxChannelDatagram() {
		return errors.New("too large")
	}
	c.sent.Add(1)
	if !c.drop.Load() {
		c.peer.demux.Deliver(bytes.Clone(b))
	}
	return nil
}
func (c *chanConn) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	return c.demux.Register(c.id, deliver)
}

// chanPair returns two connected chanConns of channel size 1200.
func chanPair(t *testing.T) (*chanConn, *chanConn) {
	t.Helper()
	const maxSize = 1200
	a, b := tcpPair(t)
	ca, cb := &chanConn{Conn: a, id: 4}, &chanConn{Conn: b, id: 4}
	ca.peer, cb.peer = cb, ca
	ca.max.Store(int64(maxSize))
	cb.max.Store(int64(maxSize))
	return ca, cb
}

func dgramCfg() *Config { return &Config{EnableDatagrams: true} }

// exchangeDatagrams sends n datagrams of size bytes from c to s and
// returns how many arrived.
func exchangeDatagrams(t *testing.T, c, s *Conn, n, size int) int {
	t.Helper()
	for i := range n {
		p := bytes.Repeat([]byte{byte(i)}, size)
		if err := c.SendDatagramWait(t.Context(), p); err != nil {
			t.Fatal(err)
		}
	}
	got := 0
	for got < n {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		p, err := s.ReceiveDatagram(ctx)
		cancel()
		if err != nil {
			break
		}
		if len(p) != size {
			t.Fatalf("datagram of %d bytes, want %d", len(p), size)
		}
		got++
	}
	return got
}

// sentStats waits until c has counted sent datagrams in all and returns
// its statistics. The driver counts a channel datagram once the carrier
// took it, outside the session's lock and so possibly after the peer
// already received it; a datagram the peer received is counted on its
// sender eventually, not before the peer sees it.
func sentStats(t *testing.T, c *Conn, sent uint64) ConnectionStats {
	t.Helper()
	for {
		cs := c.ConnectionStats()
		if cs.DatagramsSent >= sent {
			return cs
		}
		select {
		case <-t.Context().Done():
			t.Fatalf("sent datagrams not counted: %+v", cs)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestChannelDatagrams(t *testing.T) {
	a, b := chanPair(t)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	if got := exchangeDatagrams(t, c, s, 50, 100); got != 50 {
		t.Fatalf("%d of 50 datagrams", got)
	}
	cs, ss := sentStats(t, c, 50), s.ConnectionStats()
	if cs.DatagramsSentChannel != 50 || cs.DatagramsSent != 50 || ss.DatagramsReceivedChannel != 50 || ss.DatagramsReceived != 50 {
		t.Fatalf("client %+v\nserver %+v", cs, ss)
	}
	if a.sent.Load() != 50 {
		t.Fatalf("%d channel datagrams sent", a.sent.Load())
	}
	// Both directions.
	if got := exchangeDatagrams(t, s, c, 10, 1000); got != 10 || sentStats(t, s, 10).DatagramsSentChannel != 10 {
		t.Fatalf("server to client: %d", got)
	}
	// Above the channel's maximum: in records.
	if got := exchangeDatagrams(t, c, s, 5, 1500); got != 5 {
		t.Fatalf("%d of 5 large datagrams", got)
	}
	if cs := sentStats(t, c, 55); cs.DatagramsSentChannel != 50 || cs.DatagramsSent != 55 {
		t.Fatalf("large datagrams: %+v", cs)
	}
	// Stream datagrams use the channel too.
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("open")); err != nil {
		t.Fatal(err)
	}
	in, err := s.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SendDatagram(7, []byte("flow")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cx, p, err := in.ReceiveDatagram(ctx)
	if err != nil || cx != 7 || string(p) != "flow" {
		t.Fatalf("stream datagram %d %q %v", cx, p, err)
	}
	if cs := sentStats(t, c, 56); cs.DatagramsSentChannel != 51 || cs.DatagramsSent != 56 {
		t.Fatalf("stream datagram not on the channel: %+v", cs)
	}
}

// A lost channel datagram is lost: it is not resent in a record.
func TestChannelDatagramsLost(t *testing.T) {
	a, b := chanPair(t)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	a.drop.Store(true)
	if got := exchangeDatagrams(t, c, s, 5, 100); got != 0 {
		t.Fatalf("%d lost datagrams arrived", got)
	}
}

// Without the parameter on one side the channel is not used.
func TestChannelDatagramsNegotiation(t *testing.T) {
	for _, side := range []string{"client", "server"} {
		t.Run("plain "+side, func(t *testing.T) {
			a, b := chanPair(t)
			var ca, cb net.Conn = a, b
			if side == "client" {
				ca = a.Conn
			} else {
				cb = b.Conn
			}
			c, s := pair(t, ca, cb, dgramCfg(), dgramCfg())
			if got := exchangeDatagrams(t, c, s, 10, 100); got != 10 {
				t.Fatalf("%d of 10", got)
			}
			if got := exchangeDatagrams(t, s, c, 10, 100); got != 10 {
				t.Fatalf("%d of 10 back", got)
			}
			if a.sent.Load()+b.sent.Load() != 0 || c.ConnectionStats().DatagramsSentChannel != 0 {
				t.Fatal("channel used without negotiation")
			}
		})
	}
	t.Run("datagrams disabled on one side", func(t *testing.T) {
		a, b := chanPair(t)
		c, s := pair(t, a, b, dgramCfg(), nil)
		if err := c.SendDatagram([]byte("x")); err == nil {
			t.Fatal("datagram sent to a peer without datagrams")
		}
		_ = s
		if a.sent.Load() != 0 {
			t.Fatal("channel used")
		}
	})
}

func TestChannelDatagramsShaped(t *testing.T) {
	a, b := chanPair(t)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	c.SetShaper(planShaper(func(p *shaping.RecordPlan) { p.Pad = 1 }), nil)
	if got := exchangeDatagrams(t, c, s, 5, 100); got != 5 {
		t.Fatalf("%d of 5", got)
	}
	if a.sent.Load() != 0 {
		t.Fatal("shaped pipe used its channel")
	}
}

func TestChannelDatagramsReceiveErrors(t *testing.T) {
	t.Run("unknown pipe", func(t *testing.T) {
		a, b := chanPair(t)
		c, s := pair(t, a, b, dgramCfg(), dgramCfg())
		b.demux.Deliver(wire.AppendChannelDatagram(nil, true, 8, []byte{0, 'x'}))
		b.demux.Deliver([]byte{0x40}) // truncated Pipe Stream ID
		if b.demux.Dropped() != 2 {
			t.Fatalf("dropped %d", b.demux.Dropped())
		}
		if got := exchangeDatagrams(t, c, s, 1, 10); got != 1 {
			t.Fatal("session broken")
		}
	})
	for name, frame := range map[string][]byte{
		"with Length": {0x31, 0x02, 0, 'x'},
		"padding":     {0x00, 0x30, 0, 'x'},
		"empty":       {},
	} {
		t.Run(name, func(t *testing.T) {
			a, b := chanPair(t)
			_, s := pair(t, a, b, dgramCfg(), dgramCfg())
			b.demux.Deliver(append([]byte{4}, frame...))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			select {
			case <-s.Context().Done():
			case <-ctx.Done():
				t.Fatal("session did not fail")
			}
			var te *TransportError
			if err := s.Wait(); !errors.As(err, &te) || te.ErrorCode != TransportProtocolViolation || te.Remote {
				t.Fatalf("error %v", err)
			}
		})
	}
}

func TestChannelDemuxSessions(t *testing.T) {
	var d carrierio.ChannelDemux
	var got [3][]string
	var stops [3]func()
	for i := range 3 {
		stops[i] = d.Register(uint64(i*4), func(f []byte) { got[i] = append(got[i], string(f)) })
	}
	for i := range 3 {
		d.Deliver(wire.AppendChannelDatagram(nil, true, uint64(i*4), []byte(strconv.Itoa(i))))
	}
	stops[1]()
	d.Deliver(wire.AppendChannelDatagram(nil, true, 4, []byte("late")))
	for i, g := range got {
		if len(g) != 1 || g[0] != "\x30"+strconv.Itoa(i) {
			t.Fatalf("pipe %d got %q", i, g)
		}
	}
	if d.Dropped() != 1 {
		t.Fatalf("dropped %d", d.Dropped())
	}
	// A stale stop does not remove a newer registration.
	s1 := d.Register(0, func([]byte) {})
	n := 0
	_ = d.Register(0, func([]byte) { n++ })
	s1()
	d.Deliver(wire.AppendChannelDatagram(nil, true, 0, nil))
	if n != 1 {
		t.Fatal("stale stop removed the new registration")
	}
	stops[0]()
	stops[2]()
}

// A stream datagram sent on the channel right after the stream's opening
// frame may overtake it; the receiver holds it until the stream opens, for
// at most one smoothed RTT (EXTENSIONS 6.8): on a loaded host the opening
// may come later than that, and the datagram is then dropped and counted,
// never lost unaccounted.
func TestChannelStreamDatagramOvertakesOpening(t *testing.T) {
	a, b := chanPair(t)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	ctx, cancel := context.WithTimeout(t.Context(), testscale.D(10*time.Second))
	defer cancel()
	const n = 20
	delivered := 0
	for i := range n {
		st, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if st.SendDatagram(0, []byte{byte(i)}) != nil {
			t.Fatal("stream datagram refused")
		}
		in, err := s.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// The datagram arrives, or its drop is counted.
		for dropped := uint64(i - delivered); ; {
			wctx, wcancel := context.WithTimeout(ctx, 10*time.Millisecond)
			_, p, err := in.ReceiveDatagram(wctx)
			wcancel()
			if err == nil {
				if len(p) != 1 || p[0] != byte(i) {
					t.Fatalf("stream %d: datagram %v", i, p)
				}
				delivered++
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("stream %d: datagram neither delivered nor dropped", i)
			}
			if s.ConnectionStats().DatagramsDroppedRecv > dropped {
				break
			}
		}
		_ = st.Close()
	}
	st := s.ConnectionStats()
	if delivered == 0 || st.DatagramsReceivedChannel != uint64(delivered) || st.DatagramsDroppedRecv != uint64(n-delivered) ||
		st.DatagramsReceivedChannel > st.DatagramsReceived {
		t.Fatalf("delivered %d: %+v", delivered, st)
	}
}

// TestChannelHandOff hands a received channel datagram to a session
// without waiting for its lock, so that a connection's receive loop shared
// by several sessions is not held up by one of them.
func TestChannelHandOff(t *testing.T) {
	a, b := chanPair(t)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	// Capture what c sends on the channel instead of delivering it.
	capture := &chanConn{}
	got := make(chan []byte, 1)
	capture.demux.Register(4, func(f []byte) { got <- append([]byte{4}, f...) })
	a.peer = capture
	if err := c.SendDatagram([]byte("held up")); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	select {
	case raw = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing sent on the channel")
	}
	s.lock()
	done := make(chan struct{})
	go func() { b.demux.Deliver(raw); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.unlock()
		<-done
		t.Fatal("delivery waited for the session's lock")
	}
	s.unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := s.ReceiveDatagram(ctx)
	if err != nil || string(p) != "held up" {
		t.Fatalf("%q, %v", p, err)
	}
	// The hand-off is bounded: beyond it, datagrams are dropped and
	// counted. A drainer may take the queue (and its drop count) once
	// before it blocks on the lock, so of 3 x chInMax datagrams at least
	// chInMax are dropped; the count reaches the session's statistics
	// once the drainer gets the lock.
	s.lock()
	for range 3 * s.chInMax {
		b.demux.Deliver(bytes.Clone(raw))
	}
	s.chIn.mu.Lock()
	n := len(s.chIn.q)
	s.chIn.mu.Unlock()
	s.unlock()
	if n > s.chInMax {
		t.Fatalf("%d queued, over %d", n, s.chInMax)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.ConnectionStats().DatagramsDroppedRecv < uint64(s.chInMax) {
		if time.Now().After(deadline) {
			t.Fatalf("%d dropped, want at least %d", s.ConnectionStats().DatagramsDroppedRecv, s.chInMax)
		}
		time.Sleep(time.Millisecond)
	}
}

// The channel of a read half stays visible behind the bytes classification
// read back; the write half need not offer it.
func TestChannelOfBehindPrefix(t *testing.T) {
	a, _ := chanPair(t)
	w := struct{ io.WriteCloser }{a}
	for _, framing := range []Framing{FramingByteStream, FramingRecords} {
		p := withPrefix(PipeIO{R: a, W: w, Framing: framing}, []byte{1})
		if channelOf(p) == nil {
			t.Fatalf("framing %v: channel lost", framing)
		}
	}
}

// panicChanConn is a chanConn whose receiver registration panics.
type panicChanConn struct{ *chanConn }

func (panicChanConn) ReceiveChannelDatagrams(func([]byte)) func() { panic("register") }

// A channel whose receiver registration panics fails its pipe, outside
// the session's lock: the session ends and nothing deadlocks.
func TestChannelRegistrationPanicFailsPipe(t *testing.T) {
	a, b := chanPair(t)
	ch := make(chan *Conn, 1)
	go func() {
		s, err := startServerNoListener(t.Context(), NetPipe(b), dgramCfg())
		if err != nil {
			s = nil
		}
		ch <- s
	}()
	c, err := Start(t.Context(), PipeIO{R: panicChanConn{a}, W: panicChanConn{a}}, RoleClient, dgramCfg())
	if err == nil {
		waitDone(t, c)
	}
	if s := <-ch; s != nil {
		_ = s.Close()
		waitDone(t, s)
	}
}

// readGate is a chanConn whose reliable stream reads, once they have read,
// wait while its gate is closed before they return; its channel datagrams
// do not.
type readGate struct {
	*chanConn
	gate atomic.Pointer[chan struct{}] // nil: reads return at once
}

func (g *readGate) Read(p []byte) (int, error) {
	n, err := g.chanConn.Read(p)
	if ch := g.gate.Load(); ch != nil {
		<-*ch
	}
	return n, err
}

// TestChannelHeldDatagramExpires forces the ordering that drops a held
// stream datagram: the stream's opening record reaches the receiver later
// than one smoothed RTT after the datagram that overtook it. The datagram
// is dropped and counted (EXTENSIONS 6.8); the stream itself opens.
func TestChannelHeldDatagramExpires(t *testing.T) {
	a, b := chanPair(t)
	gb := &readGate{chanConn: b}
	c, s := pair(t, a, gb, dgramCfg(), dgramCfg())
	if got := exchangeDatagrams(t, c, s, 1, 10); got != 1 { // a measured, small RTT
		t.Fatal("warm-up datagram lost")
	}
	gate := make(chan struct{})
	gb.gate.Store(&gate)
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SendDatagram(0, []byte{1}); err != nil {
		t.Fatal(err)
	}
	// The record, read, waits behind the gate while the datagram that
	// overtook it is held, until the hold (one smoothed RTT, the initial
	// RTT before one is measured) expires and drops it.
	for s.ConnectionStats().DatagramsDroppedRecv == 0 {
		select {
		case <-t.Context().Done():
			t.Fatal("the held datagram never expired")
		case <-time.After(time.Millisecond):
		}
	}
	gb.gate.Store(nil)
	close(gate)
	ctx, cancel := context.WithTimeout(t.Context(), testscale.D(5*time.Second))
	defer cancel()
	in, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer wcancel()
	if _, p, err := in.ReceiveDatagram(wctx); err == nil {
		t.Fatalf("a datagram held past its RTT was delivered: %v", p)
	}
	if d := s.ConnectionStats().DatagramsDroppedRecv; d != 1 {
		t.Fatalf("dropped %d, want 1", d)
	}
}
