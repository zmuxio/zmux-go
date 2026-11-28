package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/varint"
)

// packetRelayEnv is client -> relay -> server with datagrams on the first
// hop; the second hop has none ("off"), runs in cleartext ("tcp") or over TLS
// ("tls"), whose smaller records give it a smaller datagram bound.
func packetRelayEnv(t *testing.T, hop2 string) (client, relayIn, relayOut, server *Conn) {
	t.Helper()
	return packetRelayOn(t, hop2, func() (net.Conn, net.Conn) { return tcpPair(t) })
}

func packetRelayOn(t *testing.T, hop2 string, carrier func() (net.Conn, net.Conn)) (client, relayIn, relayOut, server *Conn) {
	t.Helper()
	cfg := &Config{EnableDatagrams: true, DatagramQueueDelay: -1}
	a1, b1 := carrier()
	client, relayIn = pair(t, a1, b1, cfg, cfg)
	a2, b2 := carrier()
	if hop2 == "tls" {
		srv, cli := selfSignedTLS(t)
		a2, b2 = tls.Client(a2, cli), tls.Server(b2, srv)
	}
	peer := *cfg
	peer.EnableDatagrams = hop2 != "off"
	relayOut, server = pair(t, a2, b2, cfg, &peer)
	return client, relayIn, relayOut, server
}

// relayStreams opens client -> relay -> server and runs fwd on the relay.
func relayStreams(t *testing.T, ctx context.Context, client, rin, rout, server *Conn,
	fwd func(ctx context.Context, a, b *Stream) (SpliceResult, error)) (cs, ss *Stream, res <-chan SpliceResult) {
	t.Helper()
	done := make(chan SpliceResult, 1)
	go func() {
		in, err := rin.AcceptStream(ctx)
		if err != nil {
			return
		}
		out, err := rout.OpenStreamSync(ctx)
		if err != nil {
			return
		}
		_ = out.Flush()
		r, _ := fwd(ctx, in, out)
		done <- r
	}()
	cs, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.Flush(); err != nil {
		t.Fatal(err)
	}
	ss, err = server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return cs, ss, done
}

// TestSplicePacketsConvertsDatagrams: datagrams that the second hop
// cannot carry arrive as capsules, those it can stay datagrams; capsules
// and an unknown capsule type pass through unchanged.
func TestSplicePacketsConvertsDatagrams(t *testing.T) {
	for _, hop2 := range []string{"off", "tcp", "tls"} {
		t.Run(hop2, func(t *testing.T) {
			leakcheck.Check(t)
			synctest.Test(t, func(t *testing.T) { testSplicePacketsConverts(t, hop2) })
		})
	}
}

func testSplicePacketsConverts(t *testing.T, hop2 string) {
	t.Helper()
	client, rin, rout, server := packetRelayOn(t, hop2, net.Pipe)
	ctx := t.Context()
	cs, ss, done := relayStreams(t, ctx, client, rin, rout, server,
		func(ctx context.Context, a, b *Stream) (SpliceResult, error) { return SplicePackets(ctx, a, b, nil) })
	// An unknown capsule (type 0x29), written as raw bytes first.
	unknown := varint.AppendVarint(varint.AppendVarint(nil, 0x29), 3)
	if _, err := cs.Write(append(unknown, "xyz"...)); err != nil {
		t.Fatal(err)
	}
	w := mustPackets(t, cs, &PacketOptions{Policy: PacketDatagramOnly})
	r := mustPackets(t, ss, nil)
	// The largest datagram of the first hop: above the second hop's
	// bound only over TLS.
	dg := bytes.Repeat([]byte{0x5a}, cs.MaxDatagramPayload(4))
	bigFits := hop2 == "tcp" && len(dg) <= ss.MaxDatagramPayload(4)
	// Complete each datagram probe before offering more input. A pending
	// fallback may be dropped when the source has reliable data ready.
	read := func(id uint64, datagram bool, data []byte) {
		t.Helper()
		pk, err := r.ReadPacket(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pk.ContextID != id || pk.Datagram != datagram || !bytes.Equal(pk.Data, data) {
			t.Fatalf("packet context=%d datagram=%v size=%d, want context=%d datagram=%v size=%d",
				pk.ContextID, pk.Datagram, len(pk.Data), id, datagram, len(data))
		}
	}
	synctest.Wait() // The unknown capsule has been forwarded.
	if err := w.WritePacket(ctx, 4, dg); err != nil {
		t.Fatal(err)
	}
	if hop2 == "tls" {
		synctest.Wait() // The relay drops this datagram at the TLS bound.
	} else {
		read(4, bigFits, dg)
	}
	if err := w.WritePacket(ctx, 5, []byte("small")); err != nil {
		t.Fatal(err)
	}
	read(5, hop2 != "off", []byte("small"))
	so := mustPackets(t, cs, &PacketOptions{Policy: PacketStreamOnly})
	if err := so.WritePacket(ctx, 6, []byte("capsule")); err != nil {
		t.Fatal(err)
	}
	read(6, false, []byte("capsule"))
	if st := r.Stats(); st.SkippedCapsules != 1 {
		t.Fatalf("receiver stats %+v", st)
	}
	_ = cs.Close()
	if _, err := r.ReadPacket(ctx, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("after FIN: %v", err)
	}
	_ = ss.Close()
	if _, err := io.ReadAll(cs); err != nil {
		t.Fatal(err)
	}
	res := <-done
	ab := res.AtoB
	enc, dropped, sent := uint64(0), uint64(0), uint64(len(dg)+5)
	switch hop2 {
	case "off":
		enc = 2
	case "tls":
		dropped, sent = 1, 5
	}
	if ab.End.Kind != SpliceFinished || res.BtoA.End.Kind != SpliceFinished || ab.DatagramsAsCapsules != enc ||
		ab.DatagramBytes != sent || spliceTestDropCount(ab.Drops) != dropped {
		t.Fatalf("result %+v", res)
	}
}

func TestSplicePacketFallbackYieldsToReliableSource(t *testing.T) {
	// A prior fallback completed and the relay observed reliable input
	// before attempting to admit the next datagram as a capsule.
	d := spliceRelay{preferSource: true}
	d.source.Ready = 1
	g := spliceGroup{meta: SpliceMeta{Kind: SpliceDatagram}}
	d.queueDatagramCapsules(t.Context(), &g)
	if d.result.Drops[SpliceDropBlocked] != 1 || len(d.pendingReliable) != 0 {
		t.Fatalf("fallback disposition: drops=%v pending=%d", d.result.Drops, len(d.pendingReliable))
	}
}

// The splice reports malformed capsules on the two affected halves.
func TestSplicePacketsMalformed(t *testing.T) {
	leakcheck.Check(t)
	client, rin, rout, server := packetRelayEnv(t, "tcp")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss, done := relayStreams(t, ctx, client, rin, rout, server,
		func(ctx context.Context, a, b *Stream) (SpliceResult, error) { return SplicePackets(ctx, a, b, nil) })
	if _, err := cs.Write([]byte{0x00, 0x06, 0x00, 'a'}); err != nil {
		t.Fatal(err)
	}
	_ = cs.Close()
	got, err := io.ReadAll(ss)
	var se *StreamError
	if !errors.As(err, &se) || se.ErrorCode != StreamPacketError {
		t.Fatalf("truncated: %x %v", got, err)
	}
	_ = ss.Close()
	if _, err := io.ReadAll(cs); err != nil {
		t.Fatal(err)
	}
	if res := <-done; res.AtoB.End != (SpliceEnd{Kind: SplicePacketError, Code: StreamPacketError}) {
		t.Fatalf("result %+v", res)
	}
}

// dropLimiter refuses datagrams of one size and allows everything else.
type dropLimiter struct{ size int }

func (dropLimiter) MaxBurstBytes() int64 { return -1 }

func (l dropLimiter) Acquire(_ context.Context, _, maxBytes int64) (int64, error) {
	return maxBytes, nil
}
func (l dropLimiter) TryAcquire(_, maxBytes int64) (int64, error) {
	if maxBytes == int64(l.size) {
		return 0, errors.New("refused")
	}
	return maxBytes, nil
}
func (dropLimiter) Refund(int64) {}

// TestSpliceDatagramDropCauses: Splice counts a datagram too large for the
// next hop and one its limiter refused apart, within DatagramsDropped.
func TestSpliceDatagramDropCauses(t *testing.T) {
	leakcheck.Check(t)
	client, rin, rout, server := packetRelayEnv(t, "tls")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := &SpliceOptions{AtoB: dropLimiter{size: 200}}
	cs, ss, done := relayStreams(t, ctx, client, rin, rout, server,
		func(ctx context.Context, a, b *Stream) (SpliceResult, error) { return Splice(ctx, a, b, opts) })
	// Fits the first hop, not the second (TLS records).
	big := make([]byte, ss.MaxDatagramPayload(0)+1)
	if len(big) > cs.MaxDatagramPayload(0) {
		t.Fatalf("bounds %d, %d", cs.MaxDatagramPayload(0), ss.MaxDatagramPayload(0))
	}
	for _, p := range [][]byte{big, make([]byte, 200), []byte("last")} {
		if err := cs.SendDatagram(0, p); err != nil {
			t.Fatal(err)
		}
	}
	if _, p, err := ss.ReceiveDatagram(ctx); err != nil || string(p) != "last" {
		t.Fatalf("forwarded datagram %q, %v", p, err)
	}
	_ = cs.Close()
	_ = ss.Close()
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(cs); err != nil {
		t.Fatal(err)
	}
	ab := (<-done).AtoB
	if ab.Drops[SpliceDropLimiter] != 1 || spliceTestDropCount(ab.Drops) != 2 || ab.Drops[SpliceDropTooLarge] != 1 || ab.DatagramBytes != 4 {
		t.Fatalf("AtoB %+v", ab)
	}
}

// failLimiter fails every Acquire, as an exhausted budget does.
type failLimiter struct{}

func (failLimiter) MaxBurstBytes() int64 { return -1 }

func (failLimiter) Acquire(context.Context, int64, int64) (int64, error) {
	return 0, errors.New("budget exhausted")
}
func (failLimiter) TryAcquire(int64, int64) (int64, error) { return 0, errors.New("budget exhausted") }
func (failLimiter) Refund(int64)                           {}

// TestSpliceLimiterFailed: a failed Acquire ends its direction
// SpliceLimiterFailed, resetting the downstream and stopping the upstream
// with ZMUX_CANCELLED, while the other direction goes on and the call
// returns its result.
func TestSpliceLimiterFailed(t *testing.T) {
	for _, packets := range []bool{false, true} {
		t.Run(fmt.Sprint("packets=", packets), func(t *testing.T) {
			leakcheck.Check(t)
			client, rin, rout, server := packetRelayEnv(t, "tcp")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			opts := &SpliceOptions{AtoB: failLimiter{}}
			cs, ss, done := relayStreams(t, ctx, client, rin, rout, server,
				func(ctx context.Context, a, b *Stream) (SpliceResult, error) {
					if packets {
						return SplicePackets(ctx, a, b, opts)
					}
					return Splice(ctx, a, b, opts)
				})
			if _, err := cs.Write([]byte{0x00, 0x02, 0x00, 'x'}); err != nil {
				t.Fatal(err)
			}
			var se *StreamError
			if _, err := io.ReadAll(ss); !errors.As(err, &se) || se.ErrorCode != StreamCancelled {
				t.Fatalf("downstream: %v", err)
			}
			if err := waitStreamStop(ctx, cs); !errors.As(err, &se) || se.ErrorCode != StreamCancelled {
				t.Fatalf("upstream: %v", err)
			}
			// The other direction goes on.
			if _, err := ss.Write([]byte{0x00, 0x05, 0x00, 'b', 'a', 'c', 'k'}); err != nil {
				t.Fatal(err)
			}
			_ = ss.Close()
			got, err := io.ReadAll(cs)
			if err != nil || string(got) != "\x00\x05\x00back" {
				t.Fatalf("other direction: %q, %v", got, err)
			}
			res := <-done
			if res.AtoB.End != (SpliceEnd{Kind: SpliceLimiterFailed}) || res.BtoA.End.Kind != SpliceFinished || res.AtoB.Bytes != 0 {
				t.Fatalf("result %+v", res)
			}
		})
	}
}

// A datagram whose next session does not use stream datagrams is dropped
// and counted in DatagramsDropped alone, not as too large.
func TestSpliceDatagramNotInUseCountsDropped(t *testing.T) {
	leakcheck.Check(t)
	client, rin, rout, server := packetRelayEnv(t, "off")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	calls := make(chan struct{}, 2)
	hook := func(context.Context, SpliceUnit) (SpliceDecision, error) {
		calls <- struct{}{}
		return SpliceDecision{}, nil
	}
	opts := &SpliceOptions{HooksAtoB: SpliceHooks{Datagram: []SpliceHook{hook}}}
	cs, ss, done := relayStreams(t, ctx, client, rin, rout, server,
		func(ctx context.Context, a, b *Stream) (SpliceResult, error) { return Splice(ctx, a, b, opts) })
	for _, p := range [][]byte{make([]byte, 10), make([]byte, 5000)} {
		if err := cs.SendDatagram(0, p); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-calls:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	_ = cs.Close()
	_ = ss.Close()
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(cs); err != nil {
		t.Fatal(err)
	}
	ab := (<-done).AtoB
	if spliceTestDropCount(ab.Drops) != 2 || ab.Drops[SpliceDropTooLarge] != 0 || ab.Drops[SpliceDropLimiter] != 0 || ab.DatagramBytes != 0 {
		t.Fatalf("AtoB %+v", ab)
	}
}

func spliceTestDropCount(counts map[SpliceDropReason]uint64) (n uint64) {
	for _, count := range counts {
		n += count
	}
	return n
}
