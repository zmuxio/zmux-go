package zmux

import "sync"

// replayKey identifies a request or token in a replayCache.
type replayKey [24]byte

// replayCache records keys for as long as the time they were sent at stays
// acceptable. Times are integers (seconds, slots); a key sent at t is kept
// in the bucket of t/width, and a bucket is dropped as a whole once all of
// its times are below the acceptable range, so expiry costs O(1) per add
// and nothing is ever scanned key by key. The number of keys is bounded:
// when full, add fails.
type replayCache struct {
	mu    sync.Mutex
	width int64
	max   int
	n     int
	b     []replayBucket
}

type replayBucket struct {
	idx int64
	m   map[replayKey]struct{}
}

// newReplayCache returns a cache for acceptable ranges of at most span
// consecutive times, with buckets of width times, holding at most max keys.
func newReplayCache(span, width int64, max int) *replayCache {
	n := (span+width-1)/width + 2 // buckets a range can touch, plus one
	return &replayCache{width: width, max: max, b: make([]replayBucket, n)}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// add records k, sent at t, while times below low are no longer
// acceptable. It returns false when k is already recorded for that bucket
// or the cache is full.
func (c *replayCache) add(k replayKey, t, low int64) bool {
	idx := floorDiv(t, c.width)
	lowIdx := floorDiv(low, c.width)
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.b {
		if bk := &c.b[i]; bk.m != nil && bk.idx < lowIdx {
			c.n -= len(bk.m)
			bk.m = nil
		}
	}
	nb := int64(len(c.b))
	bk := &c.b[((idx%nb)+nb)%nb]
	if bk.m != nil && bk.idx != idx {
		// Older than every acceptable time: stale.
		c.n -= len(bk.m)
		bk.m = nil
	}
	if _, dup := bk.m[k]; dup {
		return false
	}
	if c.n >= c.max {
		return false
	}
	if bk.m == nil {
		bk.idx, bk.m = idx, map[replayKey]struct{}{}
	}
	bk.m[k] = struct{}{}
	c.n++
	return true
}
