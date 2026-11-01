package zmux

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestStreamAndSessionStats checks the application bytes, the stream
// snapshot (also once the stream is released), the datagram payload
// counts and the session gauges.
func TestStreamAndSessionStats(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	cfg := &Config{EnableDatagrams: true}
	c, s := pair(t, a, b, cfg, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cs, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SetPreamble([]byte("open info")); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 10000)
	if _, err := cs.Write(payload); err != nil {
		t.Fatal(err)
	}
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	half := make([]byte, 4000)
	if _, err := io.ReadFull(ss, half); err != nil {
		t.Fatal(err)
	}
	if err := cs.SendDatagram(9, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if _, p, err := ss.ReceiveDatagram(ctx); err != nil || string(p) != "abcdef" {
		t.Fatalf("stream datagram %q %v", p, err)
	}
	if err := c.SendDatagram([]byte("1234")); err != nil {
		t.Fatal(err)
	}
	if p, err := s.ReceiveDatagram(ctx); err != nil || string(p) != "1234" {
		t.Fatalf("session datagram %q %v", p, err)
	}
	waitUntil(t, 5*time.Second, "the rest of the data", func() bool { return ss.Stats().BytesReceived == 10000 })

	// The preamble is not payload: every count is in payload bytes.
	got := ss.Stats()
	if got.BytesRead != 4000 || got.RecvBuffered != 6000 || got.RecvWindow == 0 || got.BytesWritten != 0 ||
		got.DatagramsReceived != 1 || got.DatagramBytesReceived != 6 || got.BytesSent != 0 {
		t.Fatalf("receiver %+v", got)
	}
	if sent := cs.Stats(); sent.BytesWritten != 10000 || sent.BytesSent != 10000 || sent.BytesAcknowledged != 10000 ||
		sent.SendBuffered != 0 || sent.DatagramsSent != 1 || sent.DatagramBytesSent != 6 {
		t.Fatalf("sender %+v", sent)
	}
	st := s.ConnectionStats()
	if st.StreamsActive != 1 || st.RecvBuffered != 6000 || st.BytesRead != 4000 ||
		st.DatagramBytesReceived != 10 || st.DatagramsReceived != 2 {
		t.Fatalf("server session %+v", st)
	}
	if st := c.ConnectionStats(); st.DatagramBytesSent != 10 || st.DatagramsSent != 2 || st.BytesWritten != 10000 ||
		st.PipesActive != 1 || st.PipesAdded != 1 || st.SendBuffered != 0 {
		t.Fatalf("client session %+v", st)
	}

	// Released on both sides, the snapshots keep their final values.
	if _, err := io.ReadAll(io.LimitReader(ss, 6000)); err != nil {
		t.Fatal(err)
	}
	_ = cs.Close()
	cs.CancelRead(0)
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	_ = ss.Close()
	ss.CancelRead(0)
	waitUntil(t, 5*time.Second, "the stream's release", func() bool { return s.ConnectionStats().StreamsActive == 0 })
	if got := ss.Stats(); got.BytesRead != 10000 || got.BytesReceived != 10000 || got.RecvBuffered != 0 {
		t.Fatalf("released receiver %+v", got)
	}
	if got := cs.Stats(); got.BytesWritten != 10000 || got.BytesSent != 10000 {
		t.Fatalf("released sender %+v", got)
	}

	// An RTT sample from a QX_PING.
	if _, err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.SmoothedRTT <= 0 || st.LatestRTT <= 0 || st.MinRTT <= 0 {
		t.Fatalf("RTT %v %v %v", st.SmoothedRTT, st.MinRTT, st.LatestRTT)
	}
}

// TestStreamCreditWaits counts an open that had to wait for stream credit.
func TestStreamCreditWaits(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, &Config{NoIncomingStreams: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.OpenStream(); err == nil {
		t.Fatal("an open without credit succeeded")
	}
	if got := c.ConnectionStats().StreamCreditWaits; got != 0 {
		t.Fatalf("StreamCreditWaits = %d after an open that did not wait", got)
	}
	wctx, wcancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer wcancel()
	if _, err := c.OpenStreamSync(wctx); err == nil {
		t.Fatal("an open without credit succeeded")
	}
	if got := c.ConnectionStats().StreamCreditWaits; got != 1 {
		t.Fatalf("StreamCreditWaits = %d, want 1", got)
	}
}

// TestDirectionStateNames checks that the two direction state types name
// the states of STATE_MACHINE.md 3.1 and 3.2 and nothing else: a retired
// attachment has no snapshot, so neither type has a retired value.
func TestDirectionStateNames(t *testing.T) {
	out := []string{"none", "joining", "active", "suspect", "draining", "failed", "closed"}
	for i, want := range out {
		if got := OutboundState(i).String(); got != want {
			t.Errorf("OutboundState(%d) = %q, want %q", i, got, want)
		}
	}
	if got := OutboundState(len(out)).String(); got != "OutboundState(?)" {
		t.Errorf("value past OutboundClosed = %q", got)
	}
	in := []string{"none", "joining", "open", "ended"}
	for i, want := range in {
		if got := InboundState(i).String(); got != want {
			t.Errorf("InboundState(%d) = %q, want %q", i, got, want)
		}
	}
	if got := InboundState(len(in)).String(); got != "InboundState(?)" {
		t.Errorf("value past InboundEnded = %q", got)
	}
	if OutboundNone != 0 || InboundNone != 0 {
		t.Error("the None values are not the zero values")
	}
}

// unsentConn is a carrier whose sending half reports an unsent count.
type unsentConn struct {
	net.Conn
	n atomic.Uint64
}

func (u *unsentConn) Unsent() (uint64, bool) { return u.n.Load(), true }

// TestCarrierUnsentCapability checks the optional unsent count of a
// sending half: the driver queries it and PipeStats.QueuedBytes reports
// it, with QueueDelay following.
func TestCarrierUnsentCapability(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	ua := &unsentConn{Conn: a}
	ua.n.Store(777)
	c, _ := pair(t, ua, b, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("x")); err != nil { // a write: the driver queries the count
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the unsent count", func() bool {
		ps := c.PipeStats()
		return len(ps) == 1 && ps[0].QueuedBytes == 777
	})
	st.CancelWrite(0)
}
