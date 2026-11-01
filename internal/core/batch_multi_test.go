package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// lenShaper plans every record with MaxLen; from record delayAt on (when
// set) the plan carries a delay.
type lenShaper struct {
	maxLen  int
	delayAt uint64
	delay   time.Duration
	plans   int
}

func (l *lenShaper) Capabilities() shaping.Caps { return shaping.CapPlan }
func (l *lenShaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	l.plans++
	p.MaxLen = l.maxLen
	if l.delay > 0 && v.Seq >= l.delayAt {
		p.Delay = l.delay
	}
}
func (l *lenShaper) OnEvent(*shaping.Event, *shaping.Reply) {}

func multiSession(t *testing.T, recMax uint64, data int) (*Session, *Pipe) {
	t.Helper()
	s, p := channelSession(t, nil, false)
	s.peerMaxRecord = recMax
	s.peer.MaxStreamDataBidiRem = 1 << 30
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 30
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(st, make([]byte, data), false); err != nil {
		t.Fatal(err)
	}
	return s, p
}

func sum(xs []int) int {
	n := 0
	for _, x := range xs {
		n += x
	}
	return n
}

// Unshaped small records: one batch carries records up to the batch's
// record bound, each full but the last; the stream's bytes all follow in
// order over later batches.
func TestBatchManyRecordsUnshaped(t *testing.T) {
	const recMax, total = 1000, 200 << 10
	s, p := multiSession(t, recMax, total)
	now := Time(Second)
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
		t.Fatal("nothing sent")
	}
	recs := recordData(t, b)
	if len(recs) < 2 {
		t.Fatalf("%d records in the batch, want many", len(recs))
	}
	if b.Len() < batchRecBound-recMax-8 || b.Len() > batchRecBound {
		t.Fatalf("batch of %d bytes, want about %d", b.Len(), batchRecBound)
	}
	for i, r := range b.recs {
		if i < len(b.recs)-1 && r.size < recMax-8 {
			t.Fatalf("record %d of %d bytes, want full %d", i, r.size, recMax)
		}
	}
	got := sum(recs)
	s.WriteDone(b, nil, now)
	for got < total {
		if !s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
			t.Fatalf("stalled at %d of %d", got, total)
		}
		got += sum(recordData(t, b))
		s.WriteDone(b, nil, now)
	}
	if got != total {
		t.Fatalf("%d bytes sent, want %d", got, total)
	}
}

// Full-size records keep one record per batch: the bound starts no
// second 64 KiB record.
func TestBatchFullRecordAlone(t *testing.T) {
	s, p := multiSession(t, 65535, 1<<20)
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second)) {
		t.Fatal("nothing sent")
	}
	if n := len(b.recs); n != 1 {
		t.Fatalf("%d records, want 1", n)
	}
}

// Shaped records: each record is planned on its own and honours its
// MaxLen; one batch carries many of them.
func TestBatchManyRecordsShaped(t *testing.T) {
	s, p := multiSession(t, 65535, 64<<10)
	sh := &lenShaper{maxLen: 300}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return sh })
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second)) {
		t.Fatal("nothing sent")
	}
	if len(b.recs) < 100 {
		t.Fatalf("%d records in the batch, want >= 100", len(b.recs))
	}
	for i, r := range b.recs {
		if r.size > 300 {
			t.Fatalf("record %d of %d bytes beyond MaxLen 300", i, r.size)
		}
	}
	if sh.plans < len(b.recs) || sh.plans > len(b.recs)+1 {
		t.Fatalf("%d plans for %d records", sh.plans, len(b.recs))
	}
}

// A delayed plan ends the batch: the records before it go, the delayed
// one waits for its delay and then goes in the next batch.
func TestBatchDelayedRecordEndsBatch(t *testing.T) {
	s, p := multiSession(t, 65535, 16<<10)
	const at = 5
	sh := &lenShaper{maxLen: 400, delayAt: at, delay: 10 * time.Millisecond}
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return sh })
	now := Time(Second)
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
		t.Fatal("nothing sent")
	}
	if n := len(b.recs); n != at {
		t.Fatalf("%d records before the delayed one, want %d", n, at)
	}
	s.WriteDone(b, nil, now)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, now) {
		t.Fatal("the delayed record went early")
	}
	due := now.Add(10 * Millisecond)
	s.HandleTimeout(due)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, due) {
		t.Fatal("nothing sent after the delay")
	}
	// Every later record carries a delay too: one record per batch.
	if n := len(b.recs); n != 1 {
		t.Fatalf("%d records after the delay, want 1", n)
	}
}

// Control frames go ahead of the data records, in the first record.
func TestBatchControlFirst(t *testing.T) {
	s, p := multiSession(t, 1000, 20<<10)
	s.pend.maxData, s.pend.forceMaxData = true, true
	b := GetBatch()
	defer PutBatch(b)
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second)) {
		t.Fatal("nothing sent")
	}
	recs := batchFrames(t, b)
	if len(recs) < 2 || recs[0][0] != wire.KindMaxData {
		t.Fatalf("records %v: MAX_DATA not first", recs[:min(len(recs), 2)])
	}
	for _, r := range recs[1:] {
		for _, k := range r {
			if k != wire.KindStream {
				t.Fatalf("records %v: control after data", recs)
			}
		}
	}
}

// The limiter's room, not one record, bounds the data across all the records of a batch.
func TestBatchDataRoomAcrossRecords(t *testing.T) {
	s, p := multiSession(t, 1000, 20<<10)
	var w recWriter
	b := GetBatch()
	defer PutBatch(b)
	w.init(b, FramingBytes, 1000, DefaultBatch)
	w.dataRoom = 2500
	s.writeData(&w, p, p.out.lane(0))
	w.end()
	// The limiter lets the chunk that reaches its room through whole.
	if got := sum(recordData(t, b)); got < 2500 || got >= 2500+1000 {
		t.Fatalf("%d data bytes in %d records, want 2500 plus at most one record", got, len(b.recs))
	}
	b.release()
}
