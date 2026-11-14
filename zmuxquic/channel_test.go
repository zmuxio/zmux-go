package zmuxquic

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// lossyRelay forwards UDP between one client and a server with a one-way
// delay, and drops the next client-to-server packets of at least
// dropMin bytes when armed.
type lossyRelay struct {
	ln, up  *net.UDPConn
	client  atomic.Pointer[net.UDPAddr]
	delay   time.Duration
	dropMin int
	drop    atomic.Int32 // packets still to drop
	dropped atomic.Int32
}

func newLossyRelay(t *testing.T, server string, delay time.Duration) *lossyRelay {
	t.Helper()
	ln, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	sa, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	up, err := net.DialUDP("udp", nil, sa)
	if err != nil {
		t.Fatal(err)
	}
	r := &lossyRelay{ln: ln, up: up, delay: delay, dropMin: 1000}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.pump(true) }()
	go func() { defer wg.Done(); r.pump(false) }()
	t.Cleanup(func() {
		_ = ln.Close()
		_ = up.Close()
		wg.Wait()
	})
	return r
}

func (r *lossyRelay) addr() string { return r.ln.LocalAddr().String() }

// pump forwards one direction, each packet delay after it arrived, in order.
func (r *lossyRelay) pump(toServer bool) {
	type pkt struct {
		b   []byte
		due time.Time
	}
	q := make(chan pkt, 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for p := range q {
			time.Sleep(time.Until(p.due))
			if toServer {
				_, _ = r.up.Write(p.b)
			} else if a := r.client.Load(); a != nil {
				_, _ = r.ln.WriteToUDP(p.b, a)
			}
		}
	}()
	defer func() { close(q); <-done }()
	buf := make([]byte, 65536)
	for {
		var n int
		var err error
		if toServer {
			var a *net.UDPAddr
			n, a, err = r.ln.ReadFromUDP(buf)
			if err == nil {
				r.client.Store(a)
			}
		} else {
			n, err = r.up.Read(buf)
		}
		if err != nil {
			return
		}
		if toServer && n >= r.dropMin && r.drop.Load() > 0 && r.drop.Add(-1) >= 0 {
			r.dropped.Add(1)
			continue
		}
		select {
		case q <- pkt{bytes.Clone(buf[:n]), time.Now().Add(r.delay)}:
		default: // queue overflow: lost
		}
	}
}

// stamp is a datagram carrying its send time.
func stamp(seq int) []byte {
	b := make([]byte, 100)
	binary.BigEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	b[8] = byte(seq)
	return b
}

func latencyOf(b []byte) time.Duration {
	return time.Since(time.Unix(0, int64(binary.BigEndian.Uint64(b))))
}

// lossLatencies runs a session through a relay with one-way delay d,
// writes a burst of stream data whose first large packets are lost, sends
// datagrams right after it, and returns their latencies and the client's
// statistics. qconf configures the client's connection; lanes is the
// endpoints' MaxLanes (0: the default).
func lossLatencies(t *testing.T, d time.Duration, qconf *quic.Config, lanes int) ([]time.Duration, zmux.ConnectionStats) {
	t.Helper()
	cfg := &zmux.Config{EnableDatagrams: true, MaxLanes: lanes}
	e := newEnv(t, cfg)
	r := newLossyRelay(t, e.ln.Addr().String(), d)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, r.addr(), e.cli, qconf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qc.CloseWithError(0, "") })
	c, err := Client(ctx, qc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("warm")); err != nil {
		t.Fatal(err)
	}
	in, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := make(chan int64, 1)
	go func() {
		var n int64
		buf := make([]byte, 64<<10)
		for {
			m, err := in.Read(buf)
			n += int64(m)
			if err != nil {
				read <- n
				return
			}
		}
	}()
	// Warm up the RTT estimates and the channel's size.
	for i := range 5 {
		_ = c.SendDatagram(stamp(i))
		if _, err := s.ReceiveDatagram(ctx); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(sizeRefresh + 4*d)
	r.drop.Store(2)
	if _, err := st.Write(make([]byte, 32<<10)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond) // the burst is on the wire, its head lost
	const n = 10
	for i := range n {
		if err := c.SendDatagram(stamp(i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	var lat []time.Duration
	for len(lat) < n {
		rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
		p, err := s.ReceiveDatagram(rctx)
		rcancel()
		if err != nil {
			break
		}
		lat = append(lat, latencyOf(p))
	}
	if r.dropped.Load() == 0 {
		t.Fatal("no packet lost")
	}
	_ = st.Close()
	if got := <-read; got != 32<<10+4 {
		t.Fatalf("stream delivered %d bytes", got)
	}
	return lat, c.ConnectionStats()
}

// Datagrams on the QUIC connection's datagram channel are not held behind
// a lost packet of the pipe's stream; datagrams in records on the same QUIC
// stream are.
func TestChannelNotHeadOfLineBlocked(t *testing.T) {
	const d = 40 * time.Millisecond
	t.Run("channel", func(t *testing.T) {
		lat, st := lossLatencies(t, d, testQUIC(nil), 0)
		if st.DatagramsSentChannel < 10 {
			t.Fatalf("datagrams not on the channel: %+v", st)
		}
		if len(lat) < 5 {
			t.Fatalf("%d datagrams arrived", len(lat))
		}
		for _, l := range lat {
			if l > 2*d {
				t.Fatalf("channel datagram took %v (one-way delay %v): %v", l, d, lat)
			}
		}
		t.Logf("channel: %v", lat)
	})
	t.Run("records", func(t *testing.T) {
		qconf := testQUIC(nil)
		qconf.EnableDatagrams = false // no channel: records
		// Without lanes the stream data and the records share one QUIC
		// stream; with them the data goes on a lane of its own once the
		// peer has acknowledged that lane, and the records on lane 0 are
		// not held.
		lat, st := lossLatencies(t, d, qconf, 1)
		if st.DatagramsSentChannel != 0 {
			t.Fatalf("channel used: %+v", st)
		}
		if len(lat) != 10 {
			t.Fatalf("%d of 10 datagrams in records arrived", len(lat))
		}
		// Each waits for the retransmission of the stream's lost packet:
		// at least a round trip (2d) more than the one-way delay.
		for _, l := range lat {
			if l < 2*d {
				t.Fatalf("a datagram in records was not held behind the lost packet: %v", lat)
			}
		}
		t.Logf("records: %v", lat)
	})
}

func dialClient(t *testing.T, e *env, qc *quic.Conn, cfg *zmux.Config) (*zmux.Conn, *zmux.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Client(ctx, qc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return c, s
}

// Several sessions on one QUIC connection each receive their own
// datagrams.
func TestChannelSessionsShareConnection(t *testing.T) {
	cfg := &zmux.Config{EnableDatagrams: true}
	e := newEnv(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qc.CloseWithError(0, "") })
	type pair struct{ c, s *zmux.Conn }
	var ps []pair
	for range 3 {
		c, s := dialClient(t, e, qc, cfg)
		ps = append(ps, pair{c, s})
	}
	// Each server session must know its client: tag by the first datagram.
	for i, p := range ps {
		for j := range 20 {
			if err := p.c.SendDatagramWait(ctx, fmt.Appendf(nil, "%d/%d", i, j)); err != nil {
				t.Fatal(err)
			}
			if err := p.s.SendDatagramWait(ctx, fmt.Appendf(nil, "back %d/%d", i, j)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, p := range ps {
		for j := range 20 {
			want := fmt.Sprintf("%d/%d", i, j)
			got, err := p.s.ReceiveDatagram(ctx)
			if err != nil || string(got) != want {
				t.Fatalf("session %d: %q %v, want %q", i, got, err, want)
			}
			want = fmt.Sprintf("back %d/%d", i, j)
			got, err = p.c.ReceiveDatagram(ctx)
			if err != nil || string(got) != want {
				t.Fatalf("session %d back: %q %v, want %q", i, got, err, want)
			}
		}
		if cs, ss := p.c.ConnectionStats(), p.s.ConnectionStats(); cs.DatagramsSentChannel != 20 ||
			ss.DatagramsReceivedChannel != 20 || ss.DatagramsSentChannel != 20 || cs.DatagramsReceivedChannel != 20 {
			t.Fatalf("session %d: client %+v server %+v", i, cs, ss)
		}
	}
	if ChannelDropped(qc) != 0 {
		t.Fatalf("%d dropped", ChannelDropped(qc))
	}
	// A datagram naming no pipe is dropped and counted.
	if err := qc.SendDatagram(carrier.AppendChannelDatagram(nil, true, 1<<20, []byte{0, 'x'})); err != nil {
		t.Fatal(err)
	}
	// The server side counts it; the client's connection object is ours.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n uint64
		connChannels.Range(func(k, v any) bool {
			if k.(*quic.Conn) != qc {
				n += v.(*connChannel).demux.Dropped()
			}
			return true
		})
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dropped %d", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Datagrams larger than the connection's QUIC datagrams go in records;
// a peer without QUIC datagrams gets them in records.
func TestChannelFallback(t *testing.T) {
	cfg := &zmux.Config{EnableDatagrams: true}
	for name, qconf := range map[string]*quic.Config{
		"too large":    testQUIC(nil),
		"no datagrams": {EnableDatagrams: false, HandshakeIdleTimeout: 30 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, qconf)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = qc.CloseWithError(0, "") })
			c, s := dialClient(t, e, qc, cfg)
			size := 4000
			if name == "no datagrams" {
				size = 100
			}
			for i := range 5 {
				if err := c.SendDatagramWait(ctx, bytes.Repeat([]byte{byte(i)}, size)); err != nil {
					t.Fatal(err)
				}
			}
			for range 5 {
				p, err := s.ReceiveDatagram(ctx)
				if err != nil || len(p) != size {
					t.Fatalf("%d bytes, %v", len(p), err)
				}
			}
			if st := c.ConnectionStats(); st.DatagramsSentChannel != 0 || st.DatagramsSent != 5 {
				t.Fatalf("%+v", st)
			}
		})
	}
}
