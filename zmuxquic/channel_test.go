package zmuxquic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// streamWriteGate holds the QUIC stream write carrying the test payload.
// Its datagram channel still writes to the QUIC connection independently.
type streamWriteGate struct {
	*qStream
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

const gatedStreamPayload = "ordered stream bytes"

func (w *streamWriteGate) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(gatedStreamPayload)) {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return w.qStream.Write(p)
}

func (w *streamWriteGate) WriteBuffers(p [][]byte) (int, error) { return carrier.WriteFlat(w, p) }

// dropWarmup drops the warm-up datagram after it reaches the peer even when
// delivered in a reliable QUIC stream. A zmux datagram in a record is still
// subject to datagram queue and policy drops.
type dropWarmup struct {
	dropped chan struct{}
	once    sync.Once
}

const warmupSize = 100

func (*dropWarmup) Acquire(_ context.Context, _, n int64) (int64, error) { return n, nil }
func (d *dropWarmup) TryAcquire(_, n int64) (int64, error) {
	if n == warmupSize {
		d.once.Do(func() { close(d.dropped) })
		return 0, errors.New("drop warm-up datagram")
	}
	return n, nil
}
func (*dropWarmup) Refund(int64) {}

// Datagrams on the QUIC connection's datagram channel can pass a blocked
// stream write. Record datagrams on that same ordered stream follow it.
func TestChannelNotHeadOfLineBlocked(t *testing.T) {
	for _, mode := range []string{"channel", "records"} {
		t.Run(mode, func(t *testing.T) {
			useChannel := mode == "channel"
			ccfg := &zmux.Config{EnableDatagrams: true, MaxLanes: 1, DatagramQueueDelay: -1}
			scfg := ccfg.Clone()
			limiter := &dropWarmup{dropped: make(chan struct{})}
			scfg.DatagramLimiter.Receive = limiter
			e := newEnv(t, scfg)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			qconf := testQUIC(nil)
			qconf.EnableDatagrams = useChannel
			qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, qconf)
			if err != nil {
				t.Fatalf("dial QUIC: %v", err)
			}
			t.Cleanup(func() { _ = qc.CloseWithError(0, "") })
			p, err := ConnPipe(ctx, qc)
			if err != nil {
				t.Fatalf("open carrier: %v", err)
			}
			gate := &streamWriteGate{qStream: p.W.(*qStream), entered: make(chan struct{}), release: make(chan struct{})}
			p.W = gate
			release := sync.OnceFunc(func() { close(gate.release) })
			defer release()
			c, err := zmux.Start(ctx, p, zmux.RoleClient, ccfg)
			if err != nil {
				t.Fatalf("start client: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			server, err := e.lst.Accept(ctx)
			if err != nil {
				t.Fatalf("accept server: %v", err)
			}
			t.Cleanup(func() { _ = server.Close() })

			if err := c.SendDatagram(make([]byte, warmupSize)); err != nil {
				t.Fatalf("queue warm-up: %v", err)
			}
			select {
			case <-limiter.dropped:
			case <-ctx.Done():
				t.Fatalf("warm-up never reached the receive limiter: %v", ctx.Err())
			}
			warmStats := server.ConnectionStats()

			st, err := c.OpenStream()
			if err != nil {
				t.Fatalf("open application stream: %v", err)
			}
			if _, err := st.Write([]byte(gatedStreamPayload)); err != nil {
				t.Fatalf("queue application stream data: %v", err)
			}
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatalf("application data never reached the write gate: %v", ctx.Err())
			}

			const probe = "datagram probe"
			if err := c.SendDatagram([]byte(probe)); err != nil {
				t.Fatalf("queue probe: %v", err)
			}
			if !useChannel {
				if got := c.ConnectionStats().DatagramsSent; got != 1 {
					t.Fatalf("record probe passed a blocked writer: sent %d, want only the warm-up", got)
				}
				release()
			}
			d, err := server.ReceiveDatagram(ctx)
			if err != nil || string(d) != probe {
				t.Fatalf("receive probe: %q, %v", d, err)
			}
			release()

			in, err := server.AcceptStream(ctx)
			if err != nil {
				t.Fatalf("accept application stream: %v", err)
			}
			deadline, _ := ctx.Deadline()
			if err := in.SetReadDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			var payload [len(gatedStreamPayload)]byte
			if _, err := io.ReadFull(in, payload[:]); err != nil || string(payload[:]) != gatedStreamPayload {
				t.Fatalf("receive application data: %q, %v", payload, err)
			}
			wantChannel := warmStats.DatagramsReceivedChannel
			if useChannel {
				wantChannel++
			}
			if stats := server.ConnectionStats(); stats.DatagramsReceived != 2 || stats.DatagramsReceivedChannel != wantChannel || stats.DatagramsDroppedLimit != 1 {
				t.Fatalf("datagrams received %d, on channel %d, dropped by limiter %d; want 2, %d, 1", stats.DatagramsReceived, stats.DatagramsReceivedChannel, stats.DatagramsDroppedLimit, wantChannel)
			}
		})
	}
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
	ps := make([]pair, 0, 3)
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
