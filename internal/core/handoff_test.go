package core

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// A carrier may accept an entire earlier record and only part of the
// next record. Keep the second reservation alive across loss and retries.
func TestHandoffCompleteRecordPrefixAndRetry(t *testing.T) {
	s := &Session{}
	var sent, unsent int
	report := func(a, b int) { sent += a; unsent += b }
	q := sendQ{}
	first, second := s.newHandoff(3, report), s.newHandoff(4, report)
	q.writeAccounted([]byte("one"), nil, first)
	q.writeAccounted([]byte("four"), nil, second)
	b := &Batch{owner: &outLane{info: CarrierInfo{Framing: FramingRecords}}}
	w := &recWriter{}
	w.init(b, FramingRecords, 1024, 4096)
	if !w.stream(0, 0, []byte("one"), pin{}, false, 3) {
		t.Fatal("first frame refused")
	}
	q.markHandoffs(b, 0, 3)
	w.end()
	if !w.stream(0, 3, []byte("four"), pin{}, false, 4) {
		t.Fatal("second frame refused")
	}
	q.markHandoffs(b, 3, 7)
	w.end()
	s.WriteDone(b, b.recs[0].size+b.recs[1].size-1, errors.New("partial write"), s.now)
	deliverHandoffTest(s)
	if earned := s.ShapeMeters().EarnedBytes; sent != 3 || unsent != 0 || earned != 3 {
		t.Fatalf("partial carrier completion settled %d/%d and earned %d", sent, unsent, earned)
	}
	retry, replica := &Batch{}, &Batch{}
	q.markHandoffs(retry, 3, 7)
	q.markHandoffs(replica, 3, 7)
	q.drop() // the queue no longer owns the data
	retry.settleHandoffs(0)
	deliverHandoffTest(s)
	if sent != 3 || unsent != 0 {
		t.Fatalf("failed primary refunded while a copy was in-flight: %d/%d", sent, unsent)
	}
	replica.settleHandoffs(1)
	deliverHandoffTest(s)
	if sent != 7 || unsent != 0 {
		t.Fatalf("copy settled %d/%d", sent, unsent)
	}
}

func TestHandoffPartialRangesOverlapAndRefund(t *testing.T) {
	s := &Session{}
	var sent, unsent int
	r := s.newHandoff(100, func(a, b int) { sent += a; unsent += b })
	r.admit(20, 60)
	r.admit(50, 70)
	r.admit(5, 20)
	r.admit(5, 20)
	r.unref()
	deliverHandoffTest(s)
	if sent != 65 || unsent != 35 {
		t.Fatalf("handoff ranges %d sent %d unsent", sent, unsent)
	}
}

func TestHandoffDatagramSameBackingAndReplica(t *testing.T) {
	s, _ := channelSession(t, nil, false)
	s.loc[0].limit = 10
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	origin, _ := channelSession(t, nil, false)
	in, lookupErr := origin.lookupStream(1, true)
	if lookupErr != nil {
		t.Fatal(lookupErr)
	}
	original := make([]byte, 1024)
	copy(original[47:], []byte{0, 2, 9}) // Sequence, Flow, Context
	buf := &Buf{B: original}
	buf.Ref()
	if err := origin.onDatagram(nil, &wire.Frame{Data: original[47:75]}, buf, false); err != nil {
		t.Fatal(err)
	}
	item, ok := origin.TakeSpliceDatagram(in, len(original), SpliceReadBudget{Remaining: cap(original)})
	cx, payload, owner := item.Context, item.Data, item.Backing
	if !ok || cx != 9 || owner != buf || &payload[0] != &original[50] {
		t.Fatal("receive queue changed the backing")
	}
	var sent, unsent int
	admission, err := s.AdmitDatagram(DatagramRequest{Stream: st, Context: cx, Payload: payload, Backing: owner, At: s.now,
		Owned: true, Report: func(a, b int) { sent += a; unsent += b }})
	item.Lease.Release()
	owner.Unref()
	if admission != DatagramQueued || err != nil {
		t.Fatalf("admission %v: %v", admission, err)
	}
	f := s.dg.flows[st]
	queued := &f.queue[0]
	if queued.data.buf != buf || &queued.data.payload[0] != &original[50] || buf.Refs() != 2 {
		t.Fatal("datagram admission replaced the source backing")
	}
	primary, replica := queued.handoff.hold(), queued.handoff.hold()
	s.removeDatagram(f, false)
	primary.datagramDone(false)
	deliverHandoffTest(s)
	if sent != 0 || unsent != 0 {
		t.Fatalf("primary failure settled %d/%d", sent, unsent)
	}
	replica.datagramDone(true)
	deliverHandoffTest(s)
	if sent != 25 || unsent != 0 || buf.Refs() != 1 {
		t.Fatalf("replica settled %d/%d, backing refs %d", sent, unsent, buf.Refs())
	}
	buf.Unref()
}

func deliverHandoffTest(s *Session) {
	for _, event := range s.TakeHandoffEvents() {
		event.Deliver()
	}
}
