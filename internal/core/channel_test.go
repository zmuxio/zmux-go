package core

import (
	"bytes"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// testChannel is a carrier datagram channel of a fixed size.
type testChannel struct {
	id    uint64
	hasID bool
	max   int
}

// use is what the driver reads from the channel for PollChannel.
func (c *testChannel) use() ChannelUse {
	if c == nil {
		return ChannelUse{}
	}
	return ChannelUse{Limit: c.max, Room: c.max, ID: c.id, HasID: c.hasID}
}

// channelSession is an established session whose pipe 0 has ch and whose
// peer accepts datagrams; negotiated tells whether both endpoints sent
// zmux_carrier_datagrams.
func channelSession(t testing.TB, ch *testChannel, negotiated bool) (*Session, *Pipe) {
	t.Helper()
	return channelSessionWithConfig(t, DefaultConfig(), ch, negotiated)
}

func channelSessionWithConfig(t testing.TB, cfg Config, ch *testChannel, negotiated bool) (*Session, *Pipe) {
	t.Helper()
	cfg.Local.Has |= wire.HasDatagram | wire.HasDatagramFamily
	cfg.Local.MaxDatagramFrameSize = 65535
	s := New(cfg, 0)
	info := CarrierInfo{Channel: ch != nil}
	p := s.AttachBootstrap(info, info, nil)
	p.chUse = ch.use() // as the channel goroutine's first poll reads it
	if ch != nil && s.cfg.Local.Has&wire.HasCarrierDatagrams == 0 {
		t.Fatal("zmux_carrier_datagrams not sent with a channel")
	}
	p.out.lanes[0].tpSent = true
	s.peerSeen = true
	s.state = StateActive
	s.peerMaxDgram = 65535
	s.peerMaxRecord = 65535
	s.feat = Features{DatagramFamily: true, CarrierDatagrams: negotiated}
	s.gates.MaxDatagram = 65535
	return s, p
}

func TestChannelSend(t *testing.T) {
	ch := &testChannel{id: 4, hasID: true, max: 1200}
	s, p := channelSession(t, ch, true)
	s.SetMemberRotation(p.member.ID, 1, 0)
	p.out.sig = &WriteSignal{}
	sig := NewSignal()
	s.SetChannelSignal(p.id, sig)
	use := ch.use()
	s.TakeWakes(nil)
	if err := s.SendDatagram(nil, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if !sig.queued {
		t.Fatal("channel sender not woken")
	}
	if p.out.sig.Ready.Load() || len(s.TakePollRequests()) != 0 || s.WantsWrite(p.id, 0) {
		t.Fatal("a datagram for the channel registered a record writer")
	}
	out, live := s.PollChannel(p.id, use, nil, s.now)
	want := wire.AppendChannelDatagram(nil, true, 4, append([]byte{0, 0}, "hello"...))
	if !live || len(out) != 1 || !bytes.Equal(out[0].Frame, want) {
		t.Fatalf("PollChannel: %v %v, want %x", out, live, want)
	}
	if st := s.SessionStats(); st.DatagramsSent != 0 || st.DatagramsDroppedSend != 0 {
		t.Fatalf("stats before the carrier's result %+v", st)
	}
	out[0].Failed = true
	s.ChannelDone(out, s.now)
	if st := s.SessionStats(); st.DatagramsSent != 0 || st.DatagramsSentChannel != 0 || st.DatagramsDroppedSend != 1 ||
		st.DatagramBytesSent != 0 {
		t.Fatalf("stats after a refusal %+v", st)
	}
	if sent := p.member.rotation.sent; sent != 0 {
		t.Fatalf("a refused datagram counted %d bytes toward rotation", sent)
	}
	// A datagram the carrier takes counts as sent, its payload in
	// DatagramBytesSent.
	if err := s.SendDatagram(nil, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	out, _ = s.PollChannel(p.id, use, nil, s.now)
	s.ChannelDone(out, s.now)
	if got, want := p.member.rotation.sent, uint64(len(want)); got != want {
		t.Fatalf("datagram counted %d bytes toward rotation, want %d", got, want)
	}
	if st := s.SessionStats(); st.DatagramsSent != 1 || st.DatagramsSentChannel != 1 || st.BytesSent != 0 ||
		st.DatagramBytesSent != 5 {
		t.Fatalf("stats %+v", st)
	}

	// Above the channel's maximum, Pipe Stream ID included: in a record.
	big := make([]byte, ch.max-3) // Sequence, Flow, type and ID add four bytes.
	if err := s.SendDatagram(nil, 0, big); err != nil {
		t.Fatal(err)
	}
	if out, _ := s.PollChannel(p.id, use, nil, s.now); len(out) != 0 {
		t.Fatal("datagram above the channel maximum sent on it")
	}
	if !s.WantsWrite(p.id, 0) {
		t.Fatal("datagram above the channel maximum not pending for records")
	}
	// One byte less fits.
	s.removeDatagram(s.dg.flows[nil], false)
	if err := s.SendDatagram(nil, 0, big[1:]); err != nil {
		t.Fatal(err)
	}
	if out, _ := s.PollChannel(p.id, use, nil, s.now); len(out) != 1 || len(out[0].Frame) != ch.max {
		t.Fatalf("datagram of the channel maximum: %d", len(out))
	}
}

func TestChannelSendFallback(t *testing.T) {
	for name, tc := range map[string]struct {
		ch         *testChannel
		negotiated bool
	}{
		"not negotiated": {&testChannel{id: 0, hasID: true, max: 1200}, false},
		"no channel":     {nil, true},
		"channel closed": {&testChannel{max: 0}, true},
	} {
		t.Run(name, func(t *testing.T) {
			s, p := channelSession(t, tc.ch, tc.negotiated)
			if err := s.SendDatagram(nil, 0, []byte("x")); err != nil {
				t.Fatal(err)
			}
			if out, _ := s.PollChannel(p.id, p.chUse, nil, s.now); len(out) != 0 {
				t.Fatal("sent on the channel")
			}
			if !s.WantsWrite(p.id, 0) {
				t.Fatal("not pending for records")
			}
		})
	}
}

type nopShaper struct{}

func (nopShaper) Capabilities() shaping.Caps                    { return shaping.CapPlan }
func (nopShaper) Plan(*shaping.RecordView, *shaping.RecordPlan) {}
func (nopShaper) OnEvent(*shaping.Event, *shaping.Effects)      {}

func TestChannelReceive(t *testing.T) {
	frame := func(data string) []byte {
		return wire.AppendChannelDatagram(nil, false, 0, append([]byte{0, 0}, data...))
	}
	t.Run("delivered", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		before := s.timers.lastActivity
		s.ReceiveChannelDatagram(p.id, frame("hi"), Time(5*Second))
		d, ok := s.RecvDatagram()
		if !ok || string(d) != "hi" {
			t.Fatalf("got %q %v", d, ok)
		}
		st := s.SessionStats()
		if st.DatagramsReceived != 1 || st.DatagramsReceivedChannel != 1 || st.RecordsReceived != 0 || st.BytesReceived != 0 {
			t.Fatalf("stats %+v", st)
		}
		if s.timers.lastActivity != before {
			t.Fatal("a channel datagram restarted the idle timer")
		}
	})
	for name, tc := range map[string]struct {
		negotiated bool
		pipe       uint64
	}{
		"unknown pipe":   {true, 7},
		"not negotiated": {false, 0},
	} {
		t.Run(name, func(t *testing.T) {
			s, _ := channelSession(t, &testChannel{hasID: true, max: 1200}, tc.negotiated)
			s.ReceiveChannelDatagram(tc.pipe, frame("hi"), 0)
			if _, ok := s.RecvDatagram(); ok || s.Err() != nil {
				t.Fatal("datagram delivered, or the session failed")
			}
			if st := s.SessionStats(); st.DatagramsDroppedRecv != 1 || st.DatagramsReceivedChannel != 0 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
	t.Run("before the peer's parameters", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		s.peerSeen = false
		s.ReceiveChannelDatagram(p.id, frame("hi"), 0)
		if s.Err() != nil || s.SessionStats().DatagramsDroppedRecv != 1 {
			t.Fatal("not dropped and counted")
		}
	})
	for name, b := range map[string][]byte{
		"empty":          {},
		"with Length":    {0x31, 0x02, 0, 'x'},
		"two frames":     {0x00, 0x30, 0, 'x'},
		"other frame":    {0x01},
		"truncated flow": {0x30},
	} {
		t.Run(name, func(t *testing.T) {
			s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
			s.ReceiveChannelDatagram(p.id, b, 0)
			e := s.Err()
			if e == nil || e.Code != wire.ProtocolViolation {
				t.Fatalf("session error %v, want PROTOCOL_VIOLATION", e)
			}
		})
	}
}

// streamFrame is the channel frame of a datagram for stream id with
// context 0.
func streamFrame(seq, id uint64, payload string) []byte {
	data := varint.AppendVarint(varint.AppendVarint(varint.AppendVarint(nil, seq), id+1), 0)
	return wire.AppendChannelDatagram(nil, false, 0, append(data, payload...))
}

func recvStream(t *testing.T, s *Session, st *Stream) []string {
	t.Helper()
	var got []string
	for {
		_, p, ok := s.RecvStreamDatagram(st)
		if !ok {
			return got
		}
		got = append(got, string(p))
	}
}

// Datagrams of a peer stream that arrive before its opening frame are held
// for one smoothed RTT and delivered when it opens, in arrival order.
func TestHeldStreamDatagrams(t *testing.T) {
	const peerStream = 1 // the responder's first bidirectional stream
	t.Run("delivered on open", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		s.ReceiveChannelDatagram(p.id, streamFrame(0, peerStream, "a"), 0)
		s.ReceiveChannelDatagram(p.id, streamFrame(1, peerStream, "b"), 0)
		s.ReceiveChannelDatagram(p.id, streamFrame(2, peerStream+4, "other"), 0)
		if st := s.SessionStats(); s.Err() != nil || st.DatagramsDroppedRecv != 0 || st.DatagramsReceived != 0 {
			t.Fatalf("not held: %v %+v", s.Err(), st)
		}
		if s.NextTimeout() > Time(s.srtt()) {
			t.Fatal("no expiry deadline for the held datagrams")
		}
		st, err := s.lookupStream(peerStream, true)
		if err != nil || st == nil {
			t.Fatal(err)
		}
		if got := recvStream(t, s, st); len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("got %q", got)
		}
		if st := s.SessionStats(); st.DatagramsReceived != 2 || st.DatagramsReceivedChannel != 2 {
			t.Fatalf("delivered: %+v", st)
		}
		if len(s.dg.held) != 1 || s.dg.recvSize != uint64(len("other")+heldCost) {
			t.Fatalf("held %d, %d bytes", len(s.dg.held), s.dg.recvSize)
		}
	})
	t.Run("in a record too", func(t *testing.T) {
		s, _ := channelSession(t, nil, false)
		f := wire.Frame{Kind: wire.KindDatagram, Type: wire.TypeDatagram,
			Data: append(varint.AppendVarint(varint.AppendVarint([]byte{0}, peerStream+1), 0), 'r')}
		if err := s.onDatagram(nil, &f, nil, false); err != nil {
			t.Fatal(err)
		}
		st, _ := s.lookupStream(peerStream, true)
		if got := recvStream(t, s, st); len(got) != 1 || got[0] != "r" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("expiry drops", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		s.ReceiveChannelDatagram(p.id, streamFrame(0, peerStream, "late"), 0)
		at := s.heldDeadline()
		if at == Never || at > Time(s.srtt()) {
			t.Fatalf("deadline %v", at)
		}
		s.HandleTimeout(at)
		if st := s.SessionStats(); st.DatagramsDroppedRecv != 1 || s.dg.recvSize != 0 || len(s.dg.held) != 0 ||
			st.DatagramsReceived != 0 || st.DatagramsReceivedChannel != 0 {
			t.Fatalf("not dropped once: %+v", st)
		}
		st, _ := s.lookupStream(peerStream, true)
		if got := recvStream(t, s, st); len(got) != 0 {
			t.Fatalf("expired datagram delivered: %q", got)
		}
	})
	t.Run("byte limit drops the oldest", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		s.cfg.DgramRecvSize = 2 * (41 + heldCost) // two of 40 bytes and a context ID
		for seq, x := range []string{"1", "2", "3"} {
			s.ReceiveChannelDatagram(p.id, streamFrame(uint64(seq), peerStream, x+string(make([]byte, 39))), 0)
		}
		if st := s.SessionStats(); st.DatagramsDroppedRecv != 1 || s.dg.recvSize > s.cfg.DgramRecvSize {
			t.Fatalf("%+v, %d bytes", st, s.dg.recvSize)
		}
		st, _ := s.lookupStream(peerStream, true)
		got := recvStream(t, s, st)
		if len(got) != 2 || got[0][0] != '2' || got[1][0] != '3' {
			t.Fatalf("got %d datagrams", len(got))
		}
	})
	t.Run("byte limit bounds tiny datagrams", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		s.cfg.DgramRecvSize = 64 << 10
		const n = 10000
		for seq := range n {
			s.ReceiveChannelDatagram(p.id, streamFrame(uint64(seq), peerStream, "x"), 0)
		}
		if bound := int(s.cfg.DgramRecvSize / heldCost); len(s.dg.held) > bound {
			t.Fatalf("%d held, more than %d", len(s.dg.held), bound)
		}
		if st := s.SessionStats(); st.DatagramsDroppedRecv != uint64(n-len(s.dg.held)) {
			t.Fatalf("%+v with %d held", st, len(s.dg.held))
		}
	})
	t.Run("above the limit", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		id := s.rem[0].limit<<2 | peerStream
		s.ReceiveChannelDatagram(p.id, streamFrame(0, id, "x"), 0)
		if e := s.Err(); e == nil || e.Code != wire.ProtocolViolation {
			t.Fatalf("session error %v, want PROTOCOL_VIOLATION", e)
		}
	})
	t.Run("released stream drops", func(t *testing.T) {
		s, p := channelSession(t, &testChannel{hasID: true, max: 1200}, true)
		st, _ := s.lookupStream(peerStream, true)
		delete(s.streams, st.id) // its state discarded
		s.ReceiveChannelDatagram(p.id, streamFrame(0, peerStream, "x"), 0)
		if len(s.dg.held) != 0 || s.SessionStats().DatagramsDroppedRecv != 1 {
			t.Fatal("datagram of a discarded stream held")
		}
		if st := s.SessionStats(); st.DatagramsReceivedChannel > st.DatagramsReceived {
			t.Fatalf("channel count above the total: %+v", st)
		}
	})
}
