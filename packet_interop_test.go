package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// Packet streams (EXTENSIONS.md Section 12) against the scripted peer of
// ZMUX_INTEROP_PEER, whose "packets[:stream]" command echoes every packet
// with its Context ID and "packets-bad" writes a truncated capsule.

func TestInteropPackets(t *testing.T) {
	for _, datagrams := range []bool{true, false} {
		t.Run(fmt.Sprintf("datagrams=%v", datagrams), func(t *testing.T) {
			p := startPeer(t, "server")
			addr := p.expect(t, "LISTEN ")
			nc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c, err := Client(ctx, nc, &Config{EnableDatagrams: datagrams})
			if err != nil {
				t.Fatal(err)
			}
			p.expect(t, "ESTABLISHED")
			interopPacketEcho(t, ctx, c, datagrams)
			interopPacketStreamOnly(t, ctx, c)
			interopPacketMalformed(t, ctx, c)
			if datagrams {
				interopPacketRelay(t, ctx, c)
			}
			st, err := c.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_ = st.SetPreamble([]byte("close:42:bye"))
			_ = st.Close()
			var ae *ApplicationError
			if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 42 {
				t.Fatalf("close: %v", err)
			}
			p.expect(t, "CLOSED")
		})
	}
}

// interopMaxPacket is the peer's packet bound: room for a packet above
// the largest datagram.
const interopMaxPacket = 1 << 20

type interopPacket struct {
	cx   uint64
	data string
}

func openPacketStream(t *testing.T, ctx context.Context, c *Conn, cmd string, policy PacketPolicy) (*Stream, *PacketStream) {
	t.Helper()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte(cmd)); err != nil {
		t.Fatal(err)
	}
	ps, err := NewPacketStream(st, &PacketOptions{Policy: policy, MaxPacket: interopMaxPacket})
	if err != nil {
		t.Fatal(err)
	}
	return st, ps
}

// readPackets reads until EOF.
func readPackets(t *testing.T, ctx context.Context, ps *PacketStream) []Packet {
	t.Helper()
	var got []Packet
	for {
		pk, err := ps.ReadPacket(ctx, nil)
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("read packet: %v", err)
		}
		got = append(got, pk)
	}
}

// interopPacketEcho sends small packets (datagrams when the session has
// them), and two above MaxDatagramPayload (capsules), with Context IDs 0
// and 5. Capsules must all come back; datagrams at least one of them.
func interopPacketEcho(t *testing.T, ctx context.Context, c *Conn, datagrams bool) {
	st, ps := openPacketStream(t, ctx, c, "packets", PacketPreferDatagram)
	maxDgram := st.MaxDatagramPayload(5)
	if datagrams && maxDgram <= 0 || !datagrams && maxDgram != 0 {
		t.Fatalf("MaxDatagramPayload %d with datagrams %v", maxDgram, datagrams)
	}
	large := maxDgram + 100
	if large+16 > interopMaxPacket {
		t.Fatalf("MaxDatagramPayload %d leaves no room for a capsule-only packet", maxDgram)
	}
	small, big := map[interopPacket]bool{}, map[interopPacket]bool{}
	for i := range 8 {
		k := interopPacket{uint64(5 * (i % 2)), fmt.Sprintf("small-%d", i)}
		small[k] = true
	}
	for i, cx := range []uint64{0, 5} {
		b := bytes.Repeat([]byte{byte('A' + i)}, large+i)
		big[interopPacket{cx, string(b)}] = true
	}
	for k := range small {
		if err := ps.WritePacket(ctx, k.cx, []byte(k.data)); err != nil {
			t.Fatal(err)
		}
	}
	for k := range big {
		if err := ps.WritePacket(ctx, k.cx, []byte(k.data)); err != nil {
			t.Fatal(err)
		}
	}
	if s := ps.Stats(); s.SentCapsules < 2 || datagrams && s.SentDatagrams != 8 || !datagrams && s.SentCapsules != 10 {
		t.Fatalf("sent: %+v", s)
	}
	_ = ps.Close()
	var smallGot, smallDgram int
	for _, pk := range readPackets(t, ctx, ps) {
		k := interopPacket{pk.ContextID, string(pk.Data)}
		switch {
		case big[k]:
			if pk.Datagram {
				t.Fatalf("a packet above MaxDatagramPayload came back as a datagram")
			}
			delete(big, k)
		case small[k]:
			if pk.Datagram {
				smallDgram++
			}
			if pk.Datagram && !datagrams {
				t.Fatalf("a datagram without datagrams")
			}
			delete(small, k)
			smallGot++
		default:
			t.Fatalf("unexpected packet: context %d, %d bytes", pk.ContextID, len(pk.Data))
		}
	}
	if len(big) != 0 {
		t.Fatalf("%d capsule packets lost", len(big))
	}
	if !datagrams && smallGot != 8 || datagrams && smallDgram < 1 {
		t.Fatalf("small packets: %d back, %d as datagrams", smallGot, smallDgram)
	}
}

// interopPacketStreamOnly: capsules only both ways, in order.
func interopPacketStreamOnly(t *testing.T, ctx context.Context, c *Conn) {
	_, ps := openPacketStream(t, ctx, c, "packets:stream", PacketStreamOnly)
	var want []interopPacket
	for i, n := range []int{0, 1, 100, 1500, 20000, 65535} {
		want = append(want, interopPacket{uint64(5 * (i % 2)), string(bytes.Repeat([]byte{byte(i)}, n))})
	}
	for _, k := range want {
		if err := ps.WritePacket(ctx, k.cx, []byte(k.data)); err != nil {
			t.Fatal(err)
		}
	}
	_ = ps.Close()
	got := readPackets(t, ctx, ps)
	if len(got) != len(want) {
		t.Fatalf("stream only: %d of %d packets", len(got), len(want))
	}
	for i, pk := range got {
		if pk.Datagram || pk.ContextID != want[i].cx || string(pk.Data) != want[i].data {
			t.Fatalf("stream only: packet %d: context %d, %d bytes, datagram %v", i, pk.ContextID, len(pk.Data), pk.Datagram)
		}
	}
}

// truncatedCapsule is a DATAGRAM capsule of Length 10 with one payload byte.
var truncatedCapsule = []byte{0x00, 0x0a, 0x00, 0xab}

// interopPacketMalformed: a truncated capsule then FIN, either way, ends the
// stream with ZMUX_PACKET_ERROR, seen by the writer.
func interopPacketMalformed(t *testing.T, ctx context.Context, c *Conn) {
	{
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = st.SetPreamble([]byte("packets"))
		_, _ = st.Write(truncatedCapsule)
		_ = st.Close()
		_, err = io.ReadAll(st)
		var se *StreamError
		if !errors.As(err, &se) || se.ErrorCode != StreamPacketError || !se.Remote {
			t.Fatalf("malformed to the peer: %v", err)
		}
	}
	{
		report := make(chan string, 1)
		go func() {
			rs, err := c.AcceptUniStream(ctx)
			if err != nil {
				report <- err.Error()
				return
			}
			b, _ := io.ReadAll(rs)
			report <- string(b)
		}()
		st, ps := openPacketStream(t, ctx, c, "packets-bad", PacketPreferDatagram)
		if err := st.Flush(); err != nil {
			t.Fatal(err)
		}
		if _, err := ps.ReadPacket(ctx, nil); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("malformed from the peer: %v", err)
		}
		if r := <-report; r != fmt.Sprintf("packet-error=%d", StreamPacketError) {
			t.Fatalf("peer saw: %q", r)
		}
	}
}

// interopPacketRelay: this side relays the peer's packet stream with
// SplicePackets onto a hop without datagrams; the peer's datagram echoes
// reach the far end as capsules.
func interopPacketRelay(t *testing.T, ctx context.Context, c *Conn) {
	a, b := net.Pipe()
	type res struct {
		c   *Conn
		err error
	}
	srvc := make(chan res, 1)
	go func() { s, err := Server(ctx, b, &Config{}); srvc <- res{s, err} }()
	cl, err := Client(ctx, a, &Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	r := <-srvc
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer func() { _ = r.c.Close() }()
	up, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = up.SetPreamble([]byte("packets"))
	far, err := cl.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fps, _ := NewPacketStream(far, &PacketOptions{MaxPacket: interopMaxPacket})
	const n = 4
	for i := range n {
		if err := fps.WritePacket(ctx, uint64(i%2*5), []byte(fmt.Sprintf("relayed-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	down, err := r.c.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spliced := make(chan SpliceResult, 1)
	go func() { res, _ := SplicePackets(ctx, up, down, nil); spliced <- res }()
	got := 0
	for got < n {
		rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
		pk, err := fps.ReadPacket(rctx, nil)
		rcancel()
		if err != nil {
			break
		}
		if pk.Datagram || pk.ContextID != 0 && pk.ContextID != 5 || !bytes.HasPrefix(pk.Data, []byte("relayed-")) {
			t.Fatalf("relayed packet: context %d, datagram %v", pk.ContextID, pk.Datagram)
		}
		got++
	}
	if got < 1 {
		t.Fatal("no relayed packet came back")
	}
	_ = fps.Close()
	if rest := readPackets(t, ctx, fps); len(rest)+got > n {
		t.Fatalf("relay: %d packets back of %d", len(rest)+got, n)
	}
	sr := <-spliced
	if sr.AtoB.DatagramsEncapsulated < 1 || sr.AtoB.End.Kind != SpliceFinished || sr.BtoA.End.Kind != SpliceFinished {
		t.Fatalf("relay: %+v", sr)
	}
}
