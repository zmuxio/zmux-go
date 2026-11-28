package core

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// A normal receive view and multiple splice groups can share a root. The
// external pin ledger counts the allocation once through the last release.
func TestSpliceLeaseSharesReceivePin(t *testing.T) {
	a := &recvAccount{loanCap: 256}
	root := NewBuf(make([]byte, 2, 256))
	root.account.Store(a)
	a.bytes.Store(256)
	if !root.borrow() {
		t.Fatal("ordinary view refused")
	}
	first, second := a.spliceLease(root), a.spliceLease(root)
	if first == nil || second == nil ||
		a.loaned.Load() != 256 || a.leases.Load() != 3 {
		t.Fatalf("charges: bytes=%d views=%d", a.loaned.Load(), a.leases.Load())
	}
	root.ReturnView()
	first.Release()
	root.Unref()
	if root.Refs() != 1 || a.loaned.Load() != 256 || a.leases.Load() != 1 {
		t.Fatal("remaining group lost its pin")
	}
	second.Release()
	if root.Refs() != 0 || a.loaned.Load() != 0 || a.leases.Load() != 0 || a.bytes.Load() != 0 {
		t.Fatal("last release left a charge")
	}
	root.Reuse()
	root.account.Store(a)
	a.bytes.Store(256)
	next := a.spliceLease(root)
	if next == nil || a.loaned.Load() != 256 {
		t.Fatal("reused allocation inherited old pins")
	}
	next.Release()
	root.Unref()
}

func TestSpliceDatagramCopyReservesQueueAndReceiveMemory(t *testing.T) {
	const rootBytes = 64
	payload := []byte("data")
	memory := NewBudget(rootBytes + uint64(len(payload)))
	s := New(DefaultConfig(), 0)
	s.state = StateActive
	s.datagramMemory = newBudgetAllowance(memory, 0, 0)
	st := s.newStream(true, false)
	b := NewBuf(make([]byte, rootBytes))
	defer b.Unref()
	copy(b.B, payload)
	e, ok := s.retainReceivedDatagram(7, b.B[:len(payload)], b)
	if !ok || !s.queueDatagram(st, e) {
		t.Fatal("failed to queue source datagram")
	}
	budget := SpliceReadBudget{Remaining: len(payload), ForQueue: true, QueueRemaining: len(payload) - 1}
	if _, ok := s.TakeSpliceDatagram(st, len(payload), budget); ok || len(st.dg.q) != 1 || memory.Used() != rootBytes {
		t.Fatal("copy consumed a datagram without queue room")
	}
	s.conn.rx.account.loanCap = 1
	budget.QueueRemaining++
	if !s.datagramMemory.adjust(1) {
		t.Fatal("failed to reserve the last byte needed for a copy")
	}
	if _, ok := s.TakeSpliceDatagram(st, len(payload), budget); ok || len(st.dg.q) != 1 || memory.Used() != rootBytes+1 {
		t.Fatal("copy consumed a datagram without receive memory")
	}
	s.datagramMemory.adjust(-1)
	in, ok := s.TakeSpliceDatagram(st, len(payload), budget)
	if !ok || in.Backing == nil || in.Backing == b || in.Lease != nil || in.Copied != len(payload) ||
		!bytes.Equal(in.Data, payload) || memory.Used() != rootBytes+uint64(len(payload)) || len(st.dg.q) != 0 {
		t.Fatalf("copy result %+v, received=%t used=%d", in, ok, memory.Used())
	}
	in.Backing.Unref()
	if memory.Used() != rootBytes {
		t.Fatalf("copy retained %d bytes", memory.Used()-rootBytes)
	}
}

func TestSpliceDatagramCountsTransferOncePerReceiveLifetime(t *testing.T) {
	s := New(DefaultConfig(), 0)
	s.state = StateActive
	st := s.newStream(true, false)
	b := NewBuf(make([]byte, 2, 64))
	defer b.Unref()
	capacity := uint64(cap(b.B))
	queue := func(n int) {
		t.Helper()
		for range n {
			e, ok := s.retainReceivedDatagram(0, b.B, b)
			if !ok || !s.queueDatagram(st, e) {
				t.Fatal("failed to queue source datagram")
			}
		}
	}
	take := func(total uint64, remaining int) {
		t.Helper()
		in, ok := s.TakeSpliceDatagram(st, len(b.B), SpliceReadBudget{Remaining: cap(b.B)})
		if !ok || in.Lease == nil || in.Backing != b {
			t.Fatal("failed to borrow queued root")
		}
		state := s.SpliceReceiveState(st, b)
		if state.Transferred != total || state.Queued != remaining || state.Held != 0 || state.OwnsRoot || !state.SpliceOwnsRoot {
			t.Fatalf("state while borrowed %+v", state)
		}
		in.Lease.Release()
		in.Backing.Unref()
	}
	queue(2)
	take(capacity, 1)
	if state := s.SpliceReceiveState(st, b); state.Held != capacity || !state.OwnsRoot || state.SpliceOwnsRoot {
		t.Fatalf("state after release %+v", state)
	}
	take(capacity, 0)
	queue(1)
	take(2*capacity, 0)
}

func TestSpliceReceiveStateWithOnlyDatagramSendState(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st := s.newStream(true, true)
	s.dgState()
	if got := s.SpliceReceiveState(st, nil); got != (SpliceReceiveState{}) {
		t.Fatalf("send-only state %+v", got)
	}
}

func TestOrdinaryDatagramReadSkipsDisarmedExpiry(t *testing.T) {
	s := New(DefaultConfig(), 0)
	s.state = StateActive
	st := s.newStream(true, false)
	for _, e := range []receivedDgram{{context: 1, expires: 100}, {context: 2}, {context: 3}} {
		if !s.queueDatagram(st, e) {
			t.Fatal("failed to queue source datagram")
		}
	}
	if cx, _, ok := s.RecvStreamDatagram(st); !ok || cx != 1 || st.dg.expiry.index != 0 {
		t.Fatal("reading the only expiring datagram did not disarm the deadline")
	}
	// An unregistered sentinel makes an otherwise redundant queue scan visible.
	st.dg.q[1].expires = 100
	s.SetNow(100)
	if cx, _, ok := s.RecvStreamDatagram(st); !ok || cx != 2 || len(st.dg.q) != 1 || st.dg.expired != 0 || st.dg.expiry.index != 0 {
		t.Fatal("ordinary read scanned a disarmed queue")
	}
	st.dg.q[0].expires = 0
	if cx, _, ok := s.RecvStreamDatagram(st); !ok || cx != 3 || st.dg.expiry.index != 0 {
		t.Fatal("reading a non-expiring item rearmed its deadline")
	}
}

func TestSpliceGroupCopiesCountOnlyAtCommit(t *testing.T) {
	s, _, _ := quietWriterPipes()
	s.feat.DatagramFamily = true
	s.peerMaxDgram, s.gates.MaxDatagram = 100, 100
	st := s.newStream(true, true)
	const initial = 5
	copied := uint64(initial)
	req := DatagramRequest{Stream: st, At: s.now, PayloadCopied: &copied}
	items := []DatagramGroupMember{{Payload: []byte("abc"), Queued: true}, {Payload: []byte("defg"), Queued: true}}
	s.cfg.DatagramSendBytes = 20
	class, err := s.AdmitDatagramGroup(req, items)
	if !errors.Is(err, DatagramDroppedError{Reason: DatagramDropQueueOverflow}) || class != DatagramRefused ||
		copied != initial || s.dg.sendCount != 0 || items[0].Queued || items[1].Queued {
		t.Fatalf("refusal class=%v err=%v copied=%d queue=%d items=%+v", class, err, copied, s.dg.sendCount, items)
	}
	s.cfg.DatagramSendBytes = 100
	class, err = s.AdmitDatagramGroup(req, items)
	if err != nil || class != DatagramQueued || copied != initial+7 || !items[0].Queued || !items[1].Queued {
		t.Fatalf("commit class=%v err=%v copied=%d items=%+v", class, err, copied, items)
	}
	f := s.dg.flows[st]
	if s.dg.sendCount != 2 || f == nil || len(f.queue) != 2 || string(f.queue[0].data.payload) != "abc" || string(f.queue[1].data.payload) != "defg" {
		t.Fatal("committed group missing or out of order")
	}
	s.closeDatagramSends()
}

func TestSpliceGroupLateRoutePublishesNothing(t *testing.T) {
	s, p := channelSession(t, &testChannel{max: 1200}, true)
	s.SetNow(1)
	p.sh = &pipeShape{s: s, p: p}
	p.sh.native.ready = 100
	copied := uint64(0)
	req := DatagramRequest{At: s.now, PayloadCopied: &copied,
		Options: DatagramOptions{Carrier: DatagramRequireNative, DeadlineMode: DatagramDeadlineAt, Deadline: 50}}
	items := []DatagramGroupMember{{Payload: []byte("a")}, {Payload: []byte("bc")}}
	class, err := s.AdmitDatagramGroup(req, items)
	if class != DatagramRefused || !errors.Is(err, DatagramDroppedError{Reason: DatagramDropDeadline}) ||
		copied != 0 || s.dg.sendCount != 0 || len(s.dg.flows) != 0 || items[0].Queued || items[1].Queued {
		t.Fatalf("late route class=%v err=%v copied=%d queue=%d items=%+v", class, err, copied, s.dg.sendCount, items)
	}
}

func TestSpliceGroupUsesSharedReplicaBudgetAtDispatch(t *testing.T) {
	s, primary, replica := quietWriterPipes()
	s.feat.DatagramFamily = true
	s.peerMaxDgram, s.gates.MaxDatagram = 65535, 65535
	s.cfg.ReplicaPercent = 100
	s.cfg.DatagramCopyLimit = 2
	lane := replica.out.lanes[0]
	lane.writing = true
	req := DatagramRequest{At: s.now, Options: DatagramOptions{Copy: DatagramDuplicate, Copies: 2}}
	items := []DatagramGroupMember{{Payload: []byte("a")}, {Payload: []byte("bc")}}
	if class, err := s.AdmitDatagramGroup(req, items); class != DatagramQueued || err != nil {
		t.Fatalf("admission class=%v err=%v", class, err)
	}
	if frames := drainRequested(t, s, primary); !slices.Contains(frames, wire.KindDatagram) || len(replica.datagramCopies) != len(items) {
		t.Fatalf("origins %v and copies %d", frames, len(replica.datagramCopies))
	}
	// Revoke the shared permission after originals commit, before the copy writer runs.
	s.cfg.ReplicaPercent = 0
	lane.writing = false
	s.wakeDatagramCopy(replica)
	if frames := drainRequested(t, s, replica); slices.Contains(frames, wire.KindDatagram) ||
		s.dg.copyCount != 0 || s.stats.DatagramCopyBudgetDropped != uint64(len(items)) {
		t.Fatalf("replica frames %v, queued %d, budget drops %d", frames, s.dg.copyCount, s.stats.DatagramCopyBudgetDropped)
	}
}

// Options can change while a host limiter is preparing a group. Revalidate
// the class, including the whole remaining sequence space, before publication.
func TestSpliceGroupRechecksClassAndSequence(t *testing.T) {
	s, _, _ := quietWriterPipes()
	s.feat.DatagramFamily = true
	s.peerMaxDgram, s.gates.MaxDatagram = 7, 7
	st := s.newStream(true, true)
	req := DatagramRequest{Stream: st, At: s.now, Representation: DatagramOrCapsule}
	items := []DatagramGroupMember{{Payload: []byte("a")}, {Payload: []byte("0123456789")}}
	class, err := s.SpliceDatagramClass(req, items)
	if err != nil || class != DatagramCapsule {
		t.Fatalf("initial class %v %v", class, err)
	}
	if err := s.SetDatagramOptions(st, DatagramOptions{Carrier: DatagramRequireNative}); err != nil {
		t.Fatal(err)
	}
	class, err = s.AdmitDatagramGroup(req, items)
	if err == nil || class != DatagramRefused || st.idSet {
		t.Fatalf("strict-native revalidation %v %v", class, err)
	}
	if err := s.SetDatagramOptions(st, DatagramOptions{}); err != nil {
		t.Fatal(err)
	}
	d := s.dgState()
	d.nextSequence = datagramSequenceSpace - 1
	class, err = s.AdmitDatagramGroup(req, items)
	if !errors.Is(err, DatagramDroppedError{Reason: DatagramDropSequenceExhausted}) || class != DatagramRefused || st.idSet || d.sendCount != 0 {
		t.Fatalf("partial sequence space: %v %v", class, err)
	}
}
