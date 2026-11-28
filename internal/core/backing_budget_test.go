package core

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestBackingBudgetKeepsJoinAndWriteSpace(t *testing.T) {
	const window = MinConnWindow + 64
	const limit = receiveWorkingSpace + receiveLoanFloor + joinFlightSpace + sendWorkingSpace + receiveCreditCost*window
	budget := NewBudget(limit)
	cfg := DefaultConfig()
	cfg.Budget = budget
	cfg.Local.Has |= wire.HasMultipath
	if err := cfg.ReserveReceive(); err != nil || cfg.Local.InitialMaxData != window {
		t.Fatalf("advertised window %d, error %v", cfg.Local.InitialMaxData, err)
	}
	s := New(cfg, 0)
	st := s.newStream(true, true)
	s.conn.rx.live = 32
	s.conn.recv = s.conn.adv
	s.conn.rel = 32
	if credit := s.nextReceiveCredit(); credit != window {
		t.Fatalf("advanced MAX_DATA across fillable holes to %d", credit)
	}
	s.conn.settled = 16
	if credit := s.nextReceiveCredit(); credit != window+16 {
		t.Fatalf("MAX_DATA after consumption = %d", credit)
	}
	s.growConnWindow(window * 2)
	if s.conn.win != window || budget.SetLimit(limit-1) {
		t.Fatal("receive growth consumed a competing reservation")
	}
	if !s.ReserveFlight(joinFlightSpace) || s.ReserveFlight(1) {
		t.Fatal("Join reservation unavailable or overcommitted")
	}
	s.ReleaseFlight(joinFlightSpace)
	if n, err := s.Write(st, make([]byte, smallHeld+1), false); n != smallHeld || !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("write took %d bytes, error %v", n, err)
	}
	st.snd.q.drop()
	space, err := s.WriteSpace(st, blockSize)
	if err != nil || len(space) != blockSize {
		t.Fatalf("direct write space %d, error %v", len(space), err)
	}
	if err := s.CommitWrite(st, 0); err != nil {
		t.Fatal(err)
	}
	if used := budget.Used(); used != limit {
		t.Fatalf("used %d, want %d", used, limit)
	}
	s.finishClose()
	if used := budget.Used(); used != 0 {
		t.Fatalf("close retained %d bytes", used)
	}
}

func TestBackingBudgetSendLimitRoundsDownToPhysicalBlocks(t *testing.T) {
	const limit = 128 << 20
	const held = 80 << 20 // above the auto send-buffer floor
	cfg := DefaultConfig()
	cfg.Budget = NewBudget(limit)
	s := New(cfg, 0)
	s.conn.sendHeld = held
	if !cfg.Budget.TryCharge(limit - (sendBlockCharge - 1)) {
		t.Fatal("could not set up a sub-block budget remainder")
	}
	if got := s.sendBufferLimit(); got != held {
		t.Fatalf("send-buffer limit %d with no room for a backing, want %d", got, held)
	}
	if !cfg.Budget.Release(1) {
		t.Fatal("could not return the last byte of a block")
	}
	if got := s.sendBufferLimit(); got != held+blockSize {
		t.Fatalf("send-buffer limit %d with room for one backing, want %d", got, held+blockSize)
	}
}

func TestBackingChargeFollowsOutstandingLoan(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Budget = NewBudget(1 << 24)
	if err := cfg.ReserveReceive(); err != nil {
		t.Fatal(err)
	}
	s := New(cfg, 0)
	st := s.newStream(true, false)
	st.id, st.idSet = 1, true
	s.streams[1] = st
	b := NewBuf(make([]byte, 4096))
	if n := st.rcv.buf.insert(0, b, 0, len(b.B)); n != 4096 {
		t.Fatalf("inserted %d", n)
	}
	b.Unref()
	s.bufAdd(4096)
	v, _, err := s.ReadChunk(st, 4096)
	if err != nil || v.Buf != b {
		t.Fatalf("loan %+v, error %v", v, err)
	}
	s.finishClose()
	if used := cfg.Budget.Used(); used != 4096+pieceCost {
		t.Fatalf("loan holds %d bytes after close", used)
	}
	v.Buf.ReturnView()
	if used := cfg.Budget.Used(); used != 0 {
		t.Fatalf("release retained %d bytes", used)
	}
}

func TestBackingBudgetRetainsDatagramsOwnedByClosedStream(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Budget = NewBudget(1 << 24)
	if err := cfg.ReserveReceive(); err != nil {
		t.Fatal(err)
	}
	s := New(cfg, 0)
	s.state = StateActive
	st := s.newStream(true, false)
	st.id, st.idSet = 1, true
	s.streams[1] = st
	b := NewBuf(make([]byte, 4096))
	b.B[0] = 42
	e, ok := s.retainReceivedDatagram(1, b.B[:1], b)
	b.Unref()
	if !ok || !s.queueDatagram(st, e) {
		t.Fatal("datagram was refused")
	}
	s.finishClose()
	if used := cfg.Budget.Used(); used != 4096 {
		t.Fatalf("queued datagram holds %d bytes after close", used)
	}
	item, ok := s.TakeSpliceDatagram(st, len(b.B), SpliceReadBudget{Remaining: cap(b.B)})
	cx, data, owner := item.Context, item.Data, item.Backing
	if !ok || cx != 1 || len(data) != 1 || data[0] != 42 || owner != b {
		t.Fatalf("received context %d, data %v, owner %p, ok %t", cx, data, owner, ok)
	}
	item.Lease.Release()
	owner.Unref()
	if used := cfg.Budget.Used(); used != 0 {
		t.Fatalf("release retained %d bytes", used)
	}
}

func TestReceiveBackingCompactsAtTwoToOne(t *testing.T) {
	m := &recvMemory{}
	q := &recvBuf{memory: m}
	for i := range 8 {
		b := GetRecvBuf(1024)
		for j := range 512 {
			b.B[j] = byte(i)
		}
		q.insert(uint64(i*512), b, 0, 512)
		b.Unref()
	}
	m.compact(nil)
	if m.compacted != 0 || m.queued != 2*m.live {
		t.Fatalf("at two-to-one: queued %d, logical %d, copied %d", m.queued, m.live, m.compacted)
	}
	q.discard(2 * 512 / 3)
	m.compact(nil)
	if m.compacted == 0 || m.queued > 2*m.live {
		t.Fatalf("after compaction: queued %d, logical %d, copied %d", m.queued, m.live, m.compacted)
	}
	data := make([]byte, 4096-2*512/3)
	if got, _, err := q.consume(2*512/3, data, len(data), false); got != len(data) || err != nil {
		t.Fatalf("consume took %d, error %v", got, err)
	}
	for i, got := range data {
		if want := byte((i + 2*512/3) / 512); got != want {
			t.Fatalf("byte %d = %d, want %d", i, got, want)
		}
	}
}

func TestBudgetLimitAndChargeAreOneLedgerCommit(t *testing.T) {
	budget := NewBudget(100)
	if !budget.TryCharge(20) {
		t.Fatal("initial charge")
	}
	start := make(chan struct{})
	charged, lowered := make(chan bool, 1), make(chan bool, 1)
	go func() {
		<-start
		charged <- budget.TryCharge(40)
	}()
	go func() {
		<-start
		lowered <- budget.SetLimit(50)
	}()
	close(start)
	added, resized := <-charged, <-lowered
	if added == resized || budget.Used() > budget.Limit() {
		t.Fatalf("charge %t, limit update %t, used %d, limit %d", added, resized, budget.Used(), budget.Limit())
	}
}
