package core

import (
	"strings"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// batchFrameOrder renders the STREAM and DATAGRAM frames of a byte-stream
// batch in order: S for an opening STREAM frame without data, D for a
// STREAM frame with data, F for one with the FIN, G for a DATAGRAM.
func batchFrameOrder(t *testing.T, b *Batch) string {
	t.Helper()
	var all []byte
	for _, x := range b.Bufs() {
		all = append(all, x...)
	}
	var o strings.Builder
	for len(all) > 0 {
		size, n := varint.ConsumeVarint(all)
		rec := all[n : n+int(size)]
		all = all[n+int(size):]
		for len(rec) > 0 {
			var f wire.Frame
			l, err := wire.DecodeFrame(rec, &f)
			if err != nil {
				t.Fatal(err)
			}
			rec = rec[l:]
			switch f.Kind {
			case wire.KindStream:
				switch {
				case f.Fin:
					o.WriteByte('F')
				case len(f.Data) > 0:
					o.WriteByte('D')
				default:
					o.WriteByte('S')
				}
			case wire.KindDatagram:
				o.WriteByte('G')
			default:
			}
		}
	}
	return o.String()
}

// Datagrams sent before their stream's first frame wait for the opening;
// the opening frame then goes alone, the datagrams right behind it, and
// the stream's data and FIN after them, so that a receiver that ends the
// stream's datagram flow at the FIN sees them all.
func TestDatagramsQueuedBeforeOpeningPrecedeStreamData(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.SendDatagram(st, 0, []byte("dg")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Write(st, []byte("data"), true); err != nil {
		t.Fatal(err)
	}
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second)) {
		t.Fatal("nothing sent")
	}
	if got := batchFrameOrder(t, b); got != "SGGF" {
		t.Fatalf("frame order %q, want SGGF", got)
	}
	s.WriteDone(b, nil, Time(Second))
}

// While a stream's opening frame is unacknowledged, its datagrams follow
// it on the pipe that carried it, even when that is not the pipe of the
// control frames, and stay queued while that pipe may carry no DATAGRAM
// frames; once the opening is acknowledged they go by the usual rule
// (MULTIPATH.md S7).
func TestStreamDatagramsFollowUnackedOpening(t *testing.T) {
	s, _ := statsMP()
	s.peerSeen, s.state = true, StateActive
	s.peerMaxDgram, s.peerMaxRecord = 65535, 65535
	s.feat = Features{Flows: true, Multipath: true}
	prim, _ := s.primary()
	var open *Pipe
	for _, p := range s.pipes {
		if p != prim {
			open = p
		}
	}
	if open == nil {
		t.Fatal("statsMP has no pipe besides the primary")
	}
	st := &Stream{id: 4, idSet: true, bidi: true, local: true, opened: true, hasS: true}
	s.streams[st.id] = st
	m := st.snd.mpState()
	m.openPipe, m.openSet = open.id, true
	s.dg = &dgramState{}
	s.dg.send = []outDgram{{st: st, data: []byte{5, 0, 'a'}, n: 1}, {data: []byte{0, 'b'}, n: 1}}
	write := func(p *Pipe) string {
		t.Helper()
		b := GetBatch()
		defer PutBatch(b)
		var w recWriter
		w.init(b, FramingBytes, 65535, DefaultBatch)
		s.writeDatagrams(&w, p)
		w.end()
		var got strings.Builder
		var all []byte
		for _, x := range b.Bufs() {
			all = append(all, x...)
		}
		for len(all) > 0 {
			size, n := varint.ConsumeVarint(all)
			rec := all[n : n+int(size)]
			all = all[n+int(size):]
			for len(rec) > 0 {
				var f wire.Frame
				l, err := wire.DecodeFrame(rec, &f)
				if err != nil {
					t.Fatal(err)
				}
				rec = rec[l:]
				if f.Kind == wire.KindDatagram {
					got.WriteByte(f.Data[len(f.Data)-1])
				}
			}
		}
		return got.String()
	}
	open.mp.out = OutSuspect
	if got := write(open) + write(prim); got != "b" {
		t.Fatalf("with the opening's pipe SUSPECT: %q, want only the session datagram", got)
	}
	open.mp.out = OutActive
	if got := write(prim); got != "" {
		t.Fatalf("the control pipe sent %q", got)
	}
	s.dg.send = append(s.dg.send, outDgram{st: st, data: []byte{5, 0, 'c'}, n: 1})
	if got := write(open); got != "ac" {
		t.Fatalf("the opening's pipe sent %q, want ac", got)
	}
	s.dg.send = append(s.dg.send, outDgram{st: st, data: []byte{5, 0, 'd'}, n: 1})
	s.acked(&frameDesc{kind: dStream, id: st.id})
	if got := write(open); got != "" {
		t.Fatalf("after the opening's acknowledgement the opening's pipe sent %q", got)
	}
	if got := write(prim); got != "d" {
		t.Fatalf("after the opening's acknowledgement the control pipe sent %q, want d", got)
	}
}
