package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/varint"
)

// packetRelayEnv is client -> relay -> server with datagrams on the first
// hop; the second hop has none ("off"), runs over TCP ("tcp") or over TLS
// ("tls"), whose smaller records give it a smaller datagram bound.
func packetRelayEnv(t *testing.T, hop2 string) (client, relayIn, relayOut, server *Conn) {
	cfg := &Config{EnableDatagrams: true}
	a1, b1 := tcpPair(t)
	client, relayIn = pair(t, a1, b1, cfg, cfg)
	a2, b2 := tcpPair(t)
	if hop2 == "tls" {
		srv, cli := selfSignedTLS(t)
		a2, b2 = tls.Client(a2, cli), tls.Server(b2, srv)
	}
	relayOut, server = pair(t, a2, b2, cfg, &Config{EnableDatagrams: hop2 != "off"})
	return
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
		t.Run(hop2, func(t *testing.T) { testSplicePacketsConverts(t, hop2) })
	}
}

func testSplicePacketsConverts(t *testing.T, hop2 string) {
	leakcheck.Check(t)
	client, rin, rout, server := packetRelayEnv(t, hop2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
	if err := w.WritePacket(ctx, 4, dg); err != nil {
		t.Fatal(err)
	}
	if err := w.WritePacket(ctx, 5, []byte("small")); err != nil {
		t.Fatal(err)
	}
	so := mustPackets(t, cs, &PacketOptions{Policy: PacketStreamOnly})
	if err := so.WritePacket(ctx, 6, []byte("capsule")); err != nil {
		t.Fatal(err)
	}
	got := map[uint64]Packet{}
	for len(got) < 3 {
		pk, err := r.ReadPacket(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		got[pk.ContextID] = pk
	}
	if pk := got[4]; pk.Datagram != bigFits || !bytes.Equal(pk.Data, dg) {
		t.Fatalf("datagram: datagram %v, %d bytes", pk.Datagram, len(pk.Data))
	}
	if pk := got[5]; pk.Datagram != (hop2 != "off") || string(pk.Data) != "small" {
		t.Fatalf("small datagram: %+v", pk)
	}
	if pk := got[6]; pk.Datagram || string(pk.Data) != "capsule" {
		t.Fatalf("capsule: %+v", pk)
	}
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
	enc := uint64(0)
	switch {
	case hop2 == "off":
		enc = 2
	case !bigFits:
		enc = 1
	}
	if ab.End.Kind != SpliceFinished || res.BtoA.End.Kind != SpliceFinished || ab.DatagramsEncapsulated != enc ||
		ab.DatagramBytes != uint64(len(dg)+5) || ab.DatagramsDropped != 0 {
		t.Fatalf("result %+v", res)
	}
}

// TestSplicePacketsMalformedReachesEnd: the relay passes a truncated
// capsule on; the receiving end fails it with ZMUX_PACKET_ERROR, and the
// code travels back to the opener.
func TestSplicePacketsMalformedReachesEnd(t *testing.T) {
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
	r := mustPackets(t, ss, nil)
	if _, err := r.ReadPacket(ctx, nil); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("truncated: %v", err)
	}
	_, err := io.ReadAll(cs)
	var se *StreamError
	if !errors.As(err, &se) || se.ErrorCode != StreamPacketError {
		t.Fatalf("opener: %v", err)
	}
	if res := <-done; res.BtoA.End != (SpliceEnd{Kind: SpliceReset, Code: StreamPacketError}) {
		t.Fatalf("result %+v", res)
	}
}

// dropLimiter refuses datagrams of one size and allows everything else.
type dropLimiter struct{ size int }

func (l dropLimiter) Ready(_ context.Context, _, maxBytes int) (int, error) { return maxBytes, nil }
func (l dropLimiter) TryReady(maxBytes int) int {
	if maxBytes == l.size {
		return 0
	}
	return maxBytes
}
func (dropLimiter) Charge(int) {}

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
	if ab.DatagramsLimited != 1 || ab.DatagramsDropped != 2 || ab.DatagramsTooLarge != 1 || ab.DatagramBytes != 4 {
		t.Fatalf("AtoB %+v", ab)
	}
}
