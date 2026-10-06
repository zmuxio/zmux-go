package zmux

import "testing"

// At the tombstone limit every stream close reaps the oldest tombstone. The
// order queue must not be rewritten on every reap (O(limit) per close under
// c.mu); compaction runs only once dead slots reach the live count, and FIFO
// reap order and order indices stay consistent.
func TestTombstoneReapAtLimitCompactsAmortized(t *testing.T) {
	t.Parallel()

	const limit = 512
	c := newSessionMemoryTestConn()
	c.registry.tombstoneLimit = limit
	closeStream := func(id uint64) {
		stream := testOpenedBidiStream(c, id, testWithApplicationVisible())
		stream.setSendFin()
		stream.setRecvFin()
		c.maybeCompactTerminalLocked(stream)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	next := uint64(4)
	for i := 0; i <= limit; i++ {
		closeStream(next)
		next += 4
	}
	if got := c.tombstoneCountLocked(); got != limit {
		t.Fatalf("tombstones = %d, want %d", got, limit)
	}
	if c.registry.tombstoneHead != 1 || len(c.registry.tombstoneOrder) != limit+1 {
		t.Fatalf("after one reap: head=%d len(order)=%d, want 1 and %d (no compaction)", c.registry.tombstoneHead, len(c.registry.tombstoneOrder), limit+1)
	}

	compactions := 0
	for i := 0; i < 4*limit; i++ {
		closeStream(next)
		next += 4
		if c.registry.tombstoneHead == 0 {
			compactions++
		}
		if count := c.tombstoneCountLocked(); len(c.registry.tombstoneOrder) > 2*count+1 {
			t.Fatalf("order queue length %d for %d tombstones, want <= %d", len(c.registry.tombstoneOrder), count, 2*count+1)
		}
		// Interleave a removal from the middle of the queue.
		if i%7 == 0 {
			if !c.removeTombstoneLocked(next - 4*limit/2) {
				t.Fatalf("remove middle tombstone %d failed", next-4*limit/2)
			}
		}
	}
	if compactions > 8 {
		t.Fatalf("queue compacted %d times in %d reaps, want amortized (a few)", compactions, 4*limit)
	}

	// FIFO order and order indices survive the delayed compaction.
	head := c.tombstoneHeadLocked()
	if !head.found() {
		t.Fatal("no tombstone head")
	}
	prev := uint64(0)
	retained := 0
	for i := c.registry.tombstoneHead; i < len(c.registry.tombstoneOrder); i++ {
		entry := c.tombstoneOrderEntryLocked(i)
		if !entry.found() {
			continue
		}
		if retained == 0 && entry.streamID != head.streamID {
			t.Fatalf("first live entry %d, want head %d", entry.streamID, head.streamID)
		}
		if entry.streamID <= prev {
			t.Fatalf("tombstone order not FIFO: %d after %d", entry.streamID, prev)
		}
		if idx := c.tombstoneIndexLocked(entry.streamID, entry.tombstone.OrderIndex); !idx.found() || idx.index != i {
			t.Fatalf("tombstone %d index = (%d,%v), want (%d,true)", entry.streamID, idx.index, idx.found(), i)
		}
		prev = entry.streamID
		retained++
	}
	if retained != c.tombstoneCountLocked() || retained != len(c.registry.tombstones) {
		t.Fatalf("live order entries = %d, count = %d, map = %d; want equal", retained, c.tombstoneCountLocked(), len(c.registry.tombstones))
	}
}
