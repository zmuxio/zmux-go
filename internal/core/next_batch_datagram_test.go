package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

func TestNextBatchE1SequenceWidthsAndExhaustion(t *testing.T) {
	ch := &testChannel{max: 1200}
	s, p := channelSession(t, ch, true)
	for _, seq := range []uint64{0, 63, 64, 16383, 16384, 1<<30 - 1, 1 << 30, varint.MaxVarint} {
		s.dgState().nextSequence = seq
		if err := s.SendDatagram(nil, 0, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if s.dg.nextSequence != seq {
			t.Fatal("queueing consumed a sequence")
		}
		out, _ := s.PollChannel(p.id, p.chUse, nil, s.now)
		if len(out) != 1 {
			t.Fatalf("dispatch sequence %d: %d datagrams", seq, len(out))
		}
		var f wire.Frame
		if err := wire.DecodeChannelFrame(out[0].Frame, &f, false); err != nil {
			t.Fatal(err)
		}
		got, n := varint.ConsumeVarint(f.Data)
		if got != seq || n != varint.SizeVarint(seq) || string(f.Data[n:]) != "\x00x" {
			t.Fatalf("sequence %d: data %x", seq, f.Data)
		}
	}
	err := s.SendDatagram(nil, 0, []byte("exhausted"))
	if drop, ok := errors.AsType[DatagramDroppedError](err); !ok || drop.Reason != DatagramDropSequenceExhausted || s.MaxDatagramPayload(1) != 0 {
		t.Fatalf("exhausted sequence space: %v", err)
	}
}

func TestNextBatchE1CrossChannelCommitWindow(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.cfg.DgramWindow = 64
	s.cfg.DgramRecvSize = 1
	arrive := func(seq uint64, data string, channel bool) {
		t.Helper()
		f := wire.Frame{Kind: wire.KindDatagram, Type: wire.TypeDatagram,
			Data: append(varint.AppendVarint(nil, seq), append([]byte{0}, data...)...)}
		if err := s.onDatagram(p, &f, nil, channel); err != nil {
			t.Fatal(err)
		}
	}
	arrive(104, "too big", true)
	arrive(40, "s", false)
	arrive(104, "x", false)
	arrive(104, "x", true)
	if payload, ok := s.RecvDatagram(); !ok || string(payload) != "x" {
		t.Fatalf("received %q, %v", payload, ok)
	}
	if _, ok := s.RecvDatagram(); ok {
		t.Fatal("duplication reached the application")
	}
	stats := s.SessionStats()
	if stats.DatagramDuplicatesDropped != 1 || stats.DatagramStaleDropped != 1 {
		t.Fatalf("duplicate and stale counts: %d, %d", stats.DatagramDuplicatesDropped, stats.DatagramStaleDropped)
	}
}

func TestNextBatchE4QueuedDatagramExpiresWithoutWriteOpportunity(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.SetWritable(p.id, 0, false, 0)
	o := DatagramOptions{DeadlineMode: DatagramDeadlineAfter, TTL: 8 * Millisecond}
	if err := s.SendDatagramWith(nil, 0, []byte("expires"), o, 0); err != nil {
		t.Fatal(err)
	}
	if at := s.NextTimeout(); at != 8*Time(Millisecond) {
		t.Fatalf("next timeout = %v", at)
	}
	s.HandleTimeout(8 * Time(Millisecond))
	if s.dg.sendCount != 0 || s.dg.nextSequence != 0 || s.SessionStats().DatagramDeadlineDropped != 1 {
		t.Fatal("expired datagram stayed queued or consumed a sequence")
	}
}

func TestNextBatchE5ReportMatchesOnlyNamedChannel(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.gates.DatagramAck = true
	s.datagramDispatch(p, false, 4)
	s.datagramDispatch(p, true, 4)
	s.SetNow(Time(33 * Millisecond))
	a := wire.DatagramAck{PipeID: p.id, Channel: 1, Largest: 4, Delay: 3000}
	s.onDatagramAck(&a)
	s.onDatagramAck(&a)
	h := p.datagramFeedback
	if h.sent[0].acknowledged != 0 || h.sent[1].acknowledged != 1 || h.sent[1].rtt != 30*Millisecond {
		t.Fatalf("feedback = %+v", h.sent)
	}
	if p.out.lanes[0].acked != 0 {
		t.Fatal("DATAGRAM_ACK advanced the reliable byte position")
	}
}

func queueDatagramReplica(t *testing.T) (*Session, *Pipe, *Pipe) {
	t.Helper()
	s, first := channelSession(t, &testChannel{max: 1200}, true)
	s.mp = &multiPath{}
	first.mp = &attach{out: OutActive, in: InOpen, weight: 1}
	second := capacityExtra(s)
	second.out.lanes[0].info.Channel = true
	second.out.lanes[0].confirmed = true
	second.chUse = ChannelUse{Limit: 1000, Room: 1000}
	opts := DatagramOptions{Copy: DatagramDuplicate, Copies: 2}
	if err := s.SendDatagramWith(nil, 0, []byte("one"), opts, 0); err != nil {
		t.Fatal(err)
	}
	return s, first, second
}

func TestNextBatchE3CopiesShareSequenceAcrossPaths(t *testing.T) {
	s, first, second := queueDatagramReplica(t)
	out, _ := s.PollChannel(first.id, first.chUse, nil, s.now)
	copies, _ := s.PollChannel(second.id, second.chUse, nil, s.now)
	if len(out) != 1 || len(copies) != 1 || !bytes.Equal(out[0].Frame, copies[0].Frame) {
		t.Fatalf("original %v, copies %v", out, copies)
	}
	s.ChannelDone(out, s.now)
	s.ChannelDone(copies, s.now)
	receiver, p := channelSession(t, &testChannel{max: 1200}, true)
	for _, frame := range [][]byte{out[0].Frame, copies[0].Frame} {
		receiver.ReceiveChannelDatagram(p.id, frame, receiver.now)
	}
	if got, ok := receiver.RecvDatagram(); !ok || string(got) != "one" {
		t.Fatalf("received %q, %v", got, ok)
	}
	if _, ok := receiver.RecvDatagram(); ok {
		t.Fatal("replica reached delivery queue")
	}
	if st := s.SessionStats(); st.DatagramsSent != 1 || st.DatagramCopiesSent != 1 || st.DatagramsSentChannel != 2 {
		t.Fatalf("different logical and physical counters: %+v", st)
	}
}

func TestNextBatchE3FailedPathReleasesDatagramBudget(t *testing.T) {
	s, first, second := queueDatagramReplica(t)
	out, _ := s.PollChannel(first.id, first.chUse, nil, s.now)
	s.ChannelDone(out, s.now)
	if len(out) != 1 || len(second.datagramCopies) != 1 || s.dg.copyBytes == 0 {
		t.Fatal("the second path did not reserve a queued replica")
	}
	s.failPipe(second, wire.PipeFailed, errors.New("carrier closed"))
	if s.dg.copyCount != 0 || s.dg.copyBytes != 0 || s.SessionStats().DatagramCopyPathDropped != 1 {
		t.Fatal("failed path still holds replica budget")
	}
}

func TestDatagramAdmissionDistinguishesDeadlineAndRepresentation(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.roleKnown = true
	s.loc[0].limit = 4
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	req := DatagramRequest{Stream: st, Payload: []byte{1}, Representation: DatagramOrCapsule,
		Options: DatagramOptions{DeadlineMode: DatagramDeadlineAfter}}
	if got, err := s.AdmitDatagram(req); got != DatagramRefused || !errors.Is(err, DatagramDroppedError{Reason: DatagramDropDeadline}) || st.idSet {
		t.Fatalf("zero TTL: admission %v, error %v, pinned %v", got, err, st.idSet)
	}
	req.Options.TTL = Duration(Never)
	if got, err := s.AdmitDatagram(req); got != DatagramRefused || !errors.Is(err, ErrInvalid) || st.idSet {
		t.Fatalf("overflow TTL: admission %v, error %v, pinned %v", got, err, st.idSet)
	}
	req.Options = DatagramOptions{Carrier: DatagramRequireNative}
	req.Representation = CapsuleOnly
	if got, err := s.AdmitDatagram(req); got != DatagramRefused || !errors.Is(err, ErrInvalid) || st.idSet {
		t.Fatalf("capsule-only with native requirement: admission %v, error %v, pinned %v", got, err, st.idSet)
	}

	req.Representation = DatagramOrCapsule
	req.Options = DatagramOptions{}
	p.chUse.Room = 1 // a temporary MTU cannot trigger reliable fallback
	if got, err := s.AdmitDatagram(req); got != DatagramQueued || err != nil {
		t.Fatalf("temporarily blocked native path: admission %v, error %v", got, err)
	}
	req.Payload = make([]byte, s.peerMaxDgram)
	if got, err := s.AdmitDatagram(req); got != DatagramCapsule || err != nil {
		t.Fatalf("structural oversize: admission %v, error %v", got, err)
	}
}

func TestDatagramPathCanCarryOnlyAnEmptyNativePayload(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 3}, true)
	if _, _, size, available := s.DatagramPath(nil, 0); size != 0 || !available {
		t.Fatalf("native datagram path size %d, availability %v", size, available)
	}
	req := DatagramRequest{Options: DatagramOptions{Carrier: DatagramRequireNative}}
	if got, err := s.AdmitDatagram(req); got != DatagramQueued || err != nil {
		t.Fatalf("empty datagram admission %v, error %v", got, err)
	}
	out, _ := s.PollChannel(p.id, p.chUse, nil, s.now)
	if len(out) != 1 {
		t.Fatalf("empty native dispatches: %d", len(out))
	}
	s.ChannelDone(out, s.now)
}
