package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// chanConn is a net.Conn with a datagram channel to its peer, as a QUIC
// stream has on its connection: channel datagrams start with the Pipe
// Stream ID id and go to the peer's demultiplexer.
type chanConn struct {
	net.Conn
	id    uint64
	max   atomic.Int64
	demux ChannelDemux
	peer  *chanConn
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

// chanPair returns two connected chanConns of channel size max.
func chanPair(t *testing.T, max int) (*chanConn, *chanConn) {
	a, b := tcpPair(t)
	ca, cb := &chanConn{Conn: a, id: 4}, &chanConn{Conn: b, id: 4}
	ca.peer, cb.peer = cb, ca
	ca.max.Store(int64(max))
	cb.max.Store(int64(max))
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

func TestChannelDatagrams(t *testing.T) {
	a, b := chanPair(t, 1200)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	if got := exchangeDatagrams(t, c, s, 50, 100); got != 50 {
		t.Fatalf("%d of 50 datagrams", got)
	}
	cs, ss := c.ConnectionStats(), s.ConnectionStats()
	if cs.DatagramsSentChannel != 50 || cs.DatagramsSent != 50 || ss.DatagramsReceivedChannel != 50 || ss.DatagramsReceived != 50 {
		t.Fatalf("client %+v\nserver %+v", cs, ss)
	}
	if a.sent.Load() != 50 {
		t.Fatalf("%d channel datagrams sent", a.sent.Load())
	}
	// Both directions.
	if got := exchangeDatagrams(t, s, c, 10, 1000); got != 10 || s.ConnectionStats().DatagramsSentChannel != 10 {
		t.Fatalf("server to client: %d", got)
	}
	// Above the channel's maximum: in records.
	if got := exchangeDatagrams(t, c, s, 5, 1500); got != 5 {
		t.Fatalf("%d of 5 large datagrams", got)
	}
	if cs := c.ConnectionStats(); cs.DatagramsSentChannel != 50 || cs.DatagramsSent != 55 {
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
	if cs := c.ConnectionStats(); cs.DatagramsSentChannel != 51 {
		t.Fatalf("stream datagram not on the channel: %+v", cs)
	}
}

// A lost channel datagram is lost: it is not resent in a record.
func TestChannelDatagramsLost(t *testing.T) {
	a, b := chanPair(t, 1200)
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
			a, b := chanPair(t, 1200)
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
		a, b := chanPair(t, 1200)
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
	a, b := chanPair(t, 1200)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	c.SetShaper(planShaper(func(p *shaping.RecordPlan) { p.Pad = 1 }))
	if got := exchangeDatagrams(t, c, s, 5, 100); got != 5 {
		t.Fatalf("%d of 5", got)
	}
	if a.sent.Load() != 0 {
		t.Fatal("shaped pipe used its channel")
	}
}

func TestChannelDatagramsReceiveErrors(t *testing.T) {
	t.Run("unknown pipe", func(t *testing.T) {
		a, b := chanPair(t, 1200)
		c, s := pair(t, a, b, dgramCfg(), dgramCfg())
		b.demux.Deliver(AppendChannelDatagram(nil, true, 8, []byte{0, 'x'}))
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
			a, b := chanPair(t, 1200)
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
	var d ChannelDemux
	var got [3][]string
	var stops [3]func()
	for i := range 3 {
		stops[i] = d.Register(uint64(i*4), func(f []byte) { got[i] = append(got[i], string(f)) })
	}
	for i := range 3 {
		d.Deliver(AppendChannelDatagram(nil, true, uint64(i*4), []byte(fmt.Sprint(i))))
	}
	stops[1]()
	d.Deliver(AppendChannelDatagram(nil, true, 4, []byte("late")))
	for i, g := range got {
		if len(g) != 1 || g[0] != "\x30"+fmt.Sprint(i) {
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
	d.Deliver(AppendChannelDatagram(nil, true, 0, nil))
	if n != 1 {
		t.Fatal("stale stop removed the new registration")
	}
	stops[0]()
	stops[2]()
}

// A stream datagram sent on the channel right after the stream's opening
// frame may overtake it; the receiver holds it until the stream opens.
func TestChannelStreamDatagramOvertakesOpening(t *testing.T) {
	a, b := chanPair(t, 1200)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for i := range 20 {
		st, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		for st.SendDatagram(0, []byte{byte(i)}) != nil {
			t.Fatal("stream datagram refused")
		}
		in, err := s.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, p, err := in.ReceiveDatagram(ctx)
		if err != nil || len(p) != 1 || p[0] != byte(i) {
			t.Fatalf("stream %d: %v %v", i, p, err)
		}
		_ = st.Close()
	}
	if st := s.ConnectionStats(); st.DatagramsDroppedRecv != 0 || st.DatagramsReceivedChannel != 20 ||
		st.DatagramsReceivedChannel > st.DatagramsReceived {
		t.Fatalf("%+v", st)
	}
}

// TestChannelHandOff hands a received channel datagram to a session
// without waiting for its lock, so that a connection's receive loop shared
// by several sessions is not held up by one of them.
func TestChannelHandOff(t *testing.T) {
	a, b := chanPair(t, 1200)
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
