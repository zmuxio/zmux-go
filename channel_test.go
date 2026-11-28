package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/carrierio"
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
	limit int
	max   atomic.Int64
	sent  atomic.Int64
	drop  atomic.Bool // lose every channel datagram
}

func (c *chanConn) ChannelStreamID() (uint64, bool) { return c.id, true }
func (c *chanConn) ChannelDatagramCapacity() (limit, room int) {
	return c.limit, int(c.max.Load())
}
func (c *chanConn) SendChannelDatagram(b []byte) error {
	if len(b) > int(c.max.Load()) {
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
	a, b := tcpPair(t)
	return newChanPair(a, b)
}

func newChanPair(a, b net.Conn) (*chanConn, *chanConn) {
	const maxSize = 1200
	ca, cb := &chanConn{Conn: a, id: 4, limit: maxSize}, &chanConn{Conn: b, id: 4, limit: maxSize}
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

func TestChannelDatagrams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		ca, cb := newChanPair(a, b)
		cfg := dgramCfg()
		cfg.ClockDomain = NewClockDomain(timeshim.Real)
		c, s := pair(t, ca, cb, cfg, cfg)
		if got := exchangeDatagrams(t, c, s, 50, 100); got != 50 {
			t.Fatalf("%d of 50 datagrams", got)
		}
		synctest.Wait()
		cs, ss := c.ConnectionStats(), s.ConnectionStats()
		if cs.DatagramsSentChannel != 50 || cs.DatagramsSent != 50 || ss.DatagramsReceivedChannel != 50 || ss.DatagramsReceived != 50 {
			t.Fatalf("client %+v\nserver %+v", cs, ss)
		}
		if ca.sent.Load() != 50 {
			t.Fatalf("%d channel datagrams sent", ca.sent.Load())
		}
		// Both directions.
		if got := exchangeDatagrams(t, s, c, 10, 1000); got != 10 {
			t.Fatalf("server to client: %d", got)
		}
		synctest.Wait()
		if n := s.ConnectionStats().DatagramsSentChannel; n != 10 {
			t.Fatalf("server sent %d native datagrams, want 10", n)
		}
		// Above the channel's maximum: in records.
		if got := exchangeDatagrams(t, c, s, 5, 1500); got != 5 {
			t.Fatalf("%d of 5 large datagrams", got)
		}
		synctest.Wait()
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
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cx, p, err := in.ReceiveDatagram(ctx)
		if err != nil || cx != 7 || string(p) != "flow" {
			t.Fatalf("stream datagram %d %q %v", cx, p, err)
		}
		synctest.Wait()
		if cs := c.ConnectionStats(); cs.DatagramsSentChannel != 51 || cs.DatagramsSent != 56 {
			t.Fatalf("stream datagram not on the channel: %+v", cs)
		}
	})
}

// A lost channel datagram is lost: it is not resent in a record.
func TestChannelDatagramsLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		ca, cb := newChanPair(a, b)
		cfg := dgramCfg()
		cfg.ClockDomain = NewClockDomain(timeshim.Real)
		c, s := pair(t, ca, cb, cfg, cfg)
		ca.drop.Store(true)
		for range 5 {
			if err := c.SendDatagramWait(t.Context(), []byte("lost")); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if n := ca.sent.Load(); n != 5 {
			t.Fatalf("carrier took %d datagrams, want 5", n)
		}
		if got := s.ConnectionStats().DatagramsReceived; got != 0 {
			t.Fatalf("%d lost datagrams arrived", got)
		}
	})
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
	mustSetShaper(t, c, planShaper(func(p *shaping.RecordPlan) { p.Pad = 1 }), nil)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	want := []byte{'x', 0}
	for range 5 {
		if err := c.SendDatagramWaitWith(ctx, want, DatagramSendOptions{Carrier: DatagramRequireNative}); err != nil {
			t.Fatal(err)
		}
		got, err := s.ReceiveDatagram(ctx)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("datagram %x, error %v", got, err)
		}
	}
	if a.sent.Load() != 5 {
		t.Fatal("shaped datagrams did not use the native channel")
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
		"with Length":  {0x31, 0x02, 0, 'x'},
		"padding only": {0x00, 0x00},
		"empty":        {},
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

// A channel datagram that overtakes the opening record is delivered once
// the stream opens within the hold period.
func TestChannelStreamDatagramOvertakesOpening(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { testChannelDatagramBeforeOpen(t, false) })
}

// TestChannelHandOff hands a received channel datagram to a session
// without waiting for its lock, so that a connection's receive loop shared
// by several sessions is not held up by one of them.
func TestChannelHandOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		x, y := net.Pipe()
		a, b := newChanPair(x, y)
		// Capture what c sends on the channel instead of delivering it.
		capture := &chanConn{}
		got := make(chan []byte, 1)
		capture.demux.Register(4, func(f []byte) { got <- append([]byte{4}, f...) })
		a.peer = capture
		cfg := dgramCfg()
		cfg.ClockDomain = NewClockDomain(timeshim.Real)
		c, s := pair(t, a, b, cfg, cfg)
		if err := c.SendDatagram([]byte("held up")); err != nil {
			t.Fatal(err)
		}
		var raw []byte
		select {
		case raw = <-got:
		case <-time.After(time.Minute):
			t.Fatal("nothing sent on the channel")
		}
		s.lock()
		done := make(chan struct{})
		go func() { b.demux.Deliver(raw); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Minute):
			s.unlock()
			<-done
			t.Fatal("delivery waited for the session's lock")
		}
		s.unlock()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
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
		synctest.Wait()
		if got := s.ConnectionStats().DatagramsDroppedRecv; got < uint64(s.chInMax) {
			t.Fatalf("%d dropped, want at least %d", got, s.chInMax)
		}
	})
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

// openingWriteGate takes reliable writes while holding, without delivering
// them to the peer. Native datagrams still go directly to the peer.
type openingWriteGate struct {
	*chanConn
	pending [][]byte
	mu      sync.Mutex
	holding bool
}

func (g *openingWriteGate) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.holding {
		g.pending = append(g.pending, bytes.Clone(p))
		return len(p), nil
	}
	return g.chanConn.Write(p)
}

func (g *openingWriteGate) release() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.holding = false
	for _, p := range g.pending {
		if _, err := g.chanConn.Write(p); err != nil {
			return err
		}
	}
	g.pending = nil
	return nil
}

// A held datagram is dropped if the opening record arrives after the hold
// period. The stream itself still opens.
func TestChannelHeldDatagramExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { testChannelDatagramBeforeOpen(t, true) })
}

func testChannelDatagramBeforeOpen(t *testing.T, expire bool) {
	t.Helper()
	x, y := net.Pipe()
	a, b := newChanPair(x, y)
	gate := &openingWriteGate{chanConn: a}
	cfg := dgramCfg()
	cfg.ClockDomain = NewClockDomain(timeshim.Real)
	c, s := pair(t, gate, b, cfg, cfg)
	gate.mu.Lock()
	gate.holding = true
	gate.mu.Unlock()
	t.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SendDatagram(0, []byte("early")); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	if sent := a.sent.Load(); sent != 1 {
		t.Fatalf("%d native sends while the opening record was held", sent)
	}
	if got := s.ConnectionStats(); got.DatagramsReceived != 0 || got.DatagramsDroppedRecv != 0 {
		t.Fatalf("datagram settled before the opening record: %+v", got)
	}
	if expire {
		// With no elapsed virtual time before the datagram, one second
		// exceeds the initial 333 ms RTT and any measured RTT.
		synctest.Sleep(time.Second)
		synctest.Wait()
		if got := s.ConnectionStats().DatagramsDroppedRecv; got != 1 {
			t.Fatalf("dropped %d datagrams before the stream opened, want 1", got)
		}
	}
	if err := gate.release(); err != nil {
		t.Fatal(err)
	}
	in, err := s.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	var payload []byte
	var ok bool
	s.locked(func() { _, payload, ok = s.s.RecvStreamDatagram(in.h.st) })
	stats := s.ConnectionStats()
	if expire {
		if ok || stats.DatagramsDroppedRecv != 1 || stats.DatagramsReceivedChannel != 0 {
			t.Fatalf("expired datagram received %q, stats %+v", payload, stats)
		}
	} else if !ok || string(payload) != "early" || stats.DatagramsDroppedRecv != 0 || stats.DatagramsReceivedChannel != 1 {
		t.Fatalf("held datagram received %q, stats %+v", payload, stats)
	}
}

// PathDatagramPayload is what fits one channel datagram: a datagram of
// it goes on the channel, one byte more in a record; a smaller channel
// lowers it and EventDatagramPathChanged reports it.
func TestNextBatchE2NativePathDatagramPayload(t *testing.T) {
	a, b := chanPair(t)
	c, s := pair(t, a, b, dgramCfg(), dgramCfg())
	exchange := func(n int) int {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		if err := c.SendDatagramWait(ctx, make([]byte, n)); err != nil {
			t.Fatal(err)
		}
		p, err := s.ReceiveDatagram(ctx)
		if err != nil || len(p) != n {
			t.Fatalf("received %d bytes, wanted %d: %v", len(p), n, err)
		}
		return 1
	}
	if got := exchange(10); got != 1 {
		t.Fatal("no datagram")
	}
	st := c.ConnectionState()
	if st.NativeDatagramPayload <= 0 || st.NativeDatagramPayload >= st.MaxDatagramPayload || st.NativeDatagramPayload > 1200 {
		t.Fatalf("path %d, max %d", st.NativeDatagramPayload, st.MaxDatagramPayload)
	}
	path := st.NativeDatagramPayload
	if got := exchange(path); got != 1 {
		t.Fatal("no datagram of the path size")
	}
	if sent := a.sent.Load(); sent != 2 {
		t.Fatalf("channel sends = %d, want 2", sent)
	}
	if got := exchange(path + 1); got != 1 {
		t.Fatal("no datagram above the path size")
	}
	if sent := a.sent.Load(); sent != 2 {
		t.Fatalf("oversized packet reached channel, sends = %d", sent)
	}
	a.max.Store(800)
	if got := exchange(10); got != 1 {
		t.Fatal("no datagram after the change")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	for {
		ev, err := c.NextEvent(ctx)
		if err != nil {
			t.Fatalf("no EventDatagramPathChanged: %v", err)
		}
		if ev.Kind == EventDatagramPathChanged && ev.NativeDatagramPayload == path-400 {
			break
		}
	}
	if got := c.ConnectionState().NativeDatagramPayload; got != path-400 {
		t.Fatalf("path %d after the change, want %d", got, path-400)
	}
}
