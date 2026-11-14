package zmux

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

type capsuleCase struct {
	ID         string          `json:"id"`
	Hex        string          `json:"hex"`
	Error      *string         `json:"error"`
	Packets    []capsulePacket `json:"packets"`
	MaxPayload int             `json:"max_payload"`
	Skipped    int             `json:"skipped"`
	Encode     bool            `json:"encode"`
}

type capsulePacket struct {
	Payload string `json:"payload"`
	Context uint64 `json:"context"`
}

func loadCapsuleCases(t *testing.T) []capsuleCase {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "spec", "fixtures", "capsules.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []capsuleCase `json:"cases"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("no capsule cases")
	}
	return doc.Cases
}

func (c *capsuleCase) bytes(t *testing.T) []byte {
	b, err := hex.DecodeString(c.Hex)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (c *capsuleCase) wantErr() string {
	if c.Error == nil {
		return ""
	}
	return *c.Error
}

func capsuleErrName(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errCapsuleOversized):
		return "oversized"
	case errors.Is(err, errCapsuleTruncated):
		return "truncated"
	}
	return err.Error()
}

// TestCapsuleVectors decodes every shared capsule vector with the header
// parser and checks the encoder against the canonical ones.
func TestCapsuleVectors(t *testing.T) {
	for _, c := range loadCapsuleCases(t) {
		b := c.bytes(t)
		var got []capsulePacket
		skipped, errName := 0, ""
		for len(b) > 0 {
			h, need, err := parseCapsuleHeader(b, c.MaxPayload)
			if err == nil && need > 0 {
				err = errCapsuleTruncated // the stream ends here
			}
			if err == nil && uint64(len(b)-h.n) < h.payload {
				err = errCapsuleTruncated
			}
			if err != nil {
				errName = capsuleErrName(err)
				break
			}
			v := b[h.n : h.n+int(h.payload)]
			b = b[h.n+int(h.payload):]
			if h.typ != capsuleDatagram {
				skipped++
				continue
			}
			got = append(got, capsulePacket{Context: h.cx, Payload: hex.EncodeToString(v)})
		}
		if errName != c.wantErr() || skipped != c.Skipped || !samePackets(got, c.Packets) {
			t.Errorf("%s: packets %v skipped %d error %q", c.ID, got, skipped, errName)
		}
		if c.Encode {
			var enc []byte
			for _, p := range c.Packets {
				pl, _ := hex.DecodeString(p.Payload)
				enc = append(appendCapsuleHeader(enc, p.Context, len(pl)), pl...)
			}
			if !bytes.Equal(enc, c.bytes(t)) {
				t.Errorf("%s: encodes to %x", c.ID, enc)
			}
		}
	}
}

func samePackets(a, b []capsulePacket) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func packetEnv(t *testing.T, dgrams bool) (client, server *Conn) {
	a, b := tcpPair(t)
	cfg := &Config{EnableDatagrams: dgrams}
	return pair(t, a, b, cfg, cfg)
}

// openPackets opens a stream from client and accepts it on server, after
// a first byte so that the stream is announced.
func openPackets(t *testing.T, ctx context.Context, client, server *Conn) (*Stream, *Stream) {
	t.Helper()
	cs, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SetPreamble([]byte("packets")); err != nil {
		t.Fatal(err)
	}
	if err := cs.Flush(); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return cs, ss
}

func mustPackets(t *testing.T, st *Stream, o *PacketOptions) *PacketStream {
	t.Helper()
	p, err := NewPacketStream(st, o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestPacketStreamVectorsOnWire replays each vector as the bytes of a
// stream: the packets arrive, unknown capsules are skipped, and a malformed
// stream is reset and stopped with ZMUX_PACKET_ERROR.
func TestPacketStreamVectorsOnWire(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, c := range loadCapsuleCases(t) {
		cs, ss := openPackets(t, ctx, client, server)
		if _, err := cs.Write(c.bytes(t)); err != nil {
			t.Fatal(err)
		}
		_ = cs.Close()
		ps := mustPackets(t, ss, &PacketOptions{MaxPacket: c.MaxPayload})
		var got []capsulePacket
		var err error
		for {
			var pk Packet
			pk, err = ps.ReadPacket(ctx, make([]byte, 0, 8))
			if err != nil {
				break
			}
			got = append(got, capsulePacket{Context: pk.ContextID, Payload: hex.EncodeToString(pk.Data)})
		}
		errName := ""
		if !errors.Is(err, io.EOF) {
			errName = capsuleErrName(err)
		}
		st := ps.Stats()
		if errName != c.wantErr() || !samePackets(got, c.Packets) || st.SkippedCapsules != uint64(c.Skipped) ||
			st.ReceivedCapsules != uint64(len(c.Packets)) {
			t.Errorf("%s: packets %v stats %+v err %v", c.ID, got, st, err)
		}
		if errName == "" {
			_ = ss.Close()
			continue
		}
		if _, err := ps.ReadPacket(ctx, nil); !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("%s: read after the error: %v", c.ID, err)
		}
		// The opener sees both parts ended with ZMUX_PACKET_ERROR.
		_, rerr := io.ReadAll(cs)
		var se *StreamError
		if !errors.As(rerr, &se) || se.ErrorCode != StreamPacketError || !se.Remote {
			t.Errorf("%s: opener read: %v", c.ID, rerr)
		}
	}
}

// TestPacketStreamPolicies: datagrams when they fit, capsules otherwise,
// context IDs on both paths, DatagramOnly failures, MaxPacket.
func TestPacketStreamPolicies(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := openPackets(t, ctx, client, server)
	w := mustPackets(t, cs, nil)
	r := mustPackets(t, ss, nil)
	maxDg := cs.MaxDatagramPayload(7)
	if maxDg <= 0 || maxDg >= DefaultMaxPacket {
		t.Fatalf("MaxDatagramPayload %d", maxDg)
	}
	small, big := []byte("small"), bytes.Repeat([]byte{0xab}, maxDg+1)
	read := func(want []byte, cx uint64, dgram bool) {
		t.Helper()
		pk, err := r.ReadPacket(ctx, make([]byte, DefaultMaxPacket))
		if err != nil || !bytes.Equal(pk.Data, want) || pk.ContextID != cx || pk.Datagram != dgram {
			t.Fatalf("packet %d bytes cx %d datagram %v, %v; want %d bytes cx %d datagram %v",
				len(pk.Data), pk.ContextID, pk.Datagram, err, len(want), cx, dgram)
		}
	}
	if err := w.WritePacket(ctx, 7, small); err != nil {
		t.Fatal(err)
	}
	read(small, 7, true)
	if err := w.WritePacket(ctx, 0, big); err != nil {
		t.Fatal(err)
	}
	read(big, 0, false)
	if err := w.WritePacket(ctx, 7, big[:maxDg]); err != nil {
		t.Fatal(err)
	}
	read(big[:maxDg], 7, true)
	if err := w.WritePacket(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	read([]byte{}, 0, true)
	if err := w.WritePacket(ctx, 0, make([]byte, DefaultMaxPacket+1)); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("above MaxPacket: %v", err)
	}

	so := mustPackets(t, cs, &PacketOptions{Policy: PacketStreamOnly})
	if err := so.WritePacket(ctx, 1<<40, small); err != nil {
		t.Fatal(err)
	}
	read(small, 1<<40, false)

	do := mustPackets(t, cs, &PacketOptions{Policy: PacketDatagramOnly})
	var tl *DatagramTooLargeError
	if err := do.WritePacket(ctx, 0, big); !errors.As(err, &tl) {
		t.Fatalf("DatagramOnly too large: %v", err)
	}
	if err := do.WritePacket(ctx, 3, small); err != nil {
		t.Fatal(err)
	}
	read(small, 3, true)
	if st := do.Stats(); st.DroppedTooLarge != 1 || st.SentDatagrams != 1 {
		t.Fatalf("DatagramOnly stats %+v", st)
	}
	if st := w.Stats(); st.SentDatagrams != 3 || st.SentCapsules != 1 {
		t.Fatalf("writer stats %+v", st)
	}
	if st := r.Stats(); st.ReceivedDatagrams != 4 || st.ReceivedCapsules != 2 {
		t.Fatalf("reader stats %+v", st)
	}

	// The end of the stream at a boundary is io.EOF.
	_ = w.Close()
	if _, err := r.ReadPacket(ctx, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("after FIN: %v", err)
	}
	_ = r.Close()
}

// TestPacketStreamWithoutDatagrams: PreferDatagram falls back to capsules,
// DatagramOnly fails with ErrNotNegotiated.
func TestPacketStreamWithoutDatagrams(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := openPackets(t, ctx, client, server)
	w := mustPackets(t, cs, nil)
	r := mustPackets(t, ss, nil)
	for i := range 3 {
		if err := w.WritePacket(ctx, uint64(i), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		pk, err := r.ReadPacket(ctx, nil)
		if err != nil || pk.Datagram || pk.ContextID != uint64(i) || !bytes.Equal(pk.Data, []byte{byte(i)}) {
			t.Fatalf("packet %d: %+v %v", i, pk, err)
		}
	}
	do := mustPackets(t, cs, &PacketOptions{Policy: PacketDatagramOnly})
	if err := do.WritePacket(ctx, 0, []byte("x")); !errors.Is(err, ErrNotNegotiated) {
		t.Fatalf("DatagramOnly without datagrams: %v", err)
	}
	if _, err := NewPacketStream(cs, &PacketOptions{MaxPacket: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative MaxPacket: %v", err)
	}
	_ = cs.Close()
	_ = ss.Close()
}

// TestPacketStreamResumesInterruptedRead: a read interrupted inside a
// capsule keeps what it read; the next call completes the packet, even
// with another buffer.
func TestPacketStreamResumesInterruptedRead(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := openPackets(t, ctx, client, server)
	r := mustPackets(t, ss, nil)
	payload := []byte("0123456789")
	full := append(appendCapsuleHeader(nil, 9, len(payload)), payload...)
	if _, err := cs.Write(full[:6]); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	short, scancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err := r.ReadPacket(short, buf)
	scancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("interrupted read: %v", err)
	}
	clear(buf)
	if _, err := cs.Write(full[6:]); err != nil {
		t.Fatal(err)
	}
	pk, err := r.ReadPacket(ctx, nil)
	if err != nil || pk.ContextID != 9 || !bytes.Equal(pk.Data, payload) {
		t.Fatalf("resumed read: %+v %v", pk, err)
	}
	_ = cs.Close()
	_ = ss.Close()
}

// TestPacketStreamConcurrentReadWrite runs one reader and one writer on
// each end at once.
func TestPacketStreamConcurrentReadWrite(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := openPackets(t, ctx, client, server)
	a := mustPackets(t, cs, &PacketOptions{Policy: PacketStreamOnly})
	b := mustPackets(t, ss, &PacketOptions{Policy: PacketStreamOnly})
	const n = 200
	errc := make(chan error, 4)
	send := func(p *PacketStream) {
		for i := range n {
			if err := p.WritePacket(ctx, uint64(i), bytes.Repeat([]byte{byte(i)}, i*37)); err != nil {
				errc <- err
				return
			}
		}
		errc <- p.Close()
	}
	recv := func(p *PacketStream) {
		buf := make([]byte, DefaultMaxPacket)
		for i := 0; ; i++ {
			pk, err := p.ReadPacket(ctx, buf)
			if errors.Is(err, io.EOF) && i == n {
				errc <- nil
				return
			}
			if err != nil || pk.ContextID != uint64(i) || len(pk.Data) != i*37 {
				errc <- errors.Join(err, errors.New("wrong packet"))
				return
			}
		}
	}
	go send(a)
	go send(b)
	go recv(a)
	go recv(b)
	for range 4 {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

// A WritePacket whose ctx ended before the call sends nothing, on either
// path, and the packet does not go out later.
func TestPacketStreamCancelledWriteSendsNothing(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := openPackets(t, ctx, client, server)
	w := mustPackets(t, cs, nil)
	r := mustPackets(t, ss, nil)
	dead, stop := context.WithCancel(ctx)
	stop()
	for _, pol := range []PacketPolicy{PacketPreferDatagram, PacketStreamOnly, PacketDatagramOnly} {
		w.policy = pol
		if err := w.WritePacket(dead, 1, []byte("lost")); !errors.Is(err, context.Canceled) {
			t.Fatalf("policy %v: %v", pol, err)
		}
	}
	w.policy = PacketPreferDatagram
	if err := w.WritePacket(ctx, 2, []byte("kept")); err != nil {
		t.Fatal(err)
	}
	pk, err := r.ReadPacket(ctx, nil)
	if err != nil || pk.ContextID != 2 || string(pk.Data) != "kept" {
		t.Fatalf("first packet %+v %v", pk, err)
	}
	short, c2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer c2()
	if pk, err := r.ReadPacket(short, nil); err == nil {
		t.Fatalf("a cancelled write was sent: %+v", pk)
	}
}

// Datagrams that arrived before the end of the stream at a capsule
// boundary are all delivered before io.EOF.
func TestPacketStreamDatagramsBeforeEOF(t *testing.T) {
	leakcheck.Check(t)
	client, server := packetEnv(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := openPackets(t, ctx, client, server)
	w := mustPackets(t, cs, &PacketOptions{Policy: PacketDatagramOnly})
	r := mustPackets(t, ss, nil)
	const n = 20
	for i := range n {
		if err := w.WritePacket(ctx, uint64(i), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	got := 0
	for {
		pk, err := r.ReadPacket(ctx, nil)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !pk.Datagram {
			t.Fatalf("not a datagram: %+v", pk)
		}
		got++
	}
	// Datagrams are unreliable, but this transport loses none; a drop
	// would show as a count short of n only if the queue overflowed.
	if got != n {
		t.Fatalf("got %d of %d datagrams before EOF", got, n)
	}
	if _, err := r.ReadPacket(ctx, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("after EOF: %v", err)
	}
}
