package core

import "math"

// codelInterval is the CoDel interval (RFC 8289): a queue whose sojourn
// stays above the target this long starts dropping.
const codelInterval = 100 * Millisecond

// DefaultDgramTarget is the default CoDel target of the datagram queues.
const DefaultDgramTarget = 20 * Millisecond

// codel is the CoDel state of one datagram queue (RFC 8289, section 5):
// the sojourn time of a datagram leaving the queue decides, a datagram
// condemned is dropped instead of delivered, and the drop rate of a
// dropping state grows with the square root of its drops.
type codel struct {
	firstAbove Time // when a sojourn above target first counts; 0: below it
	dropNext   Time
	count      uint32
	lastCount  uint32
	dropping   bool
}

// okToDrop reports whether a datagram queued at time at may be dropped at
// now: sojourns have been above target for an interval. A datagram alone
// in its queue is never condemned.
func (c *codel) okToDrop(at, now Time, target Duration, alone bool) bool {
	if alone || now.Sub(at) < target {
		c.firstAbove = 0
		return false
	}
	if c.firstAbove == 0 {
		c.firstAbove = now.Add(codelInterval)
		return false
	}
	return now >= c.firstAbove
}

// controlLaw is the time of the next drop of a dropping state.
func controlLaw(t Time, count uint32) Time {
	return t.Add(Duration(float64(codelInterval) / math.Sqrt(float64(max(count, 1)))))
}

// drop decides on a datagram, queued at at, that leaves its queue at now:
// true drops it. target <= 0 turns CoDel off. A queue that empties calls
// idle.
func (c *codel) drop(at, now Time, target Duration, alone bool) bool {
	if target <= 0 {
		return false
	}
	ok := c.okToDrop(at, now, target, alone)
	if c.dropping {
		if !ok {
			c.dropping = false
			return false
		}
		if now < c.dropNext {
			return false
		}
		c.count++
		c.dropNext = controlLaw(c.dropNext, c.count)
		return true
	}
	if !ok {
		return false
	}
	c.dropping = true
	// Back to dropping soon after leaving it: resume near the last rate.
	if delta := c.count - c.lastCount; delta > 1 && now.Sub(c.dropNext) < 16*codelInterval {
		c.count = delta
	} else {
		c.count = 1
	}
	c.dropNext = controlLaw(now, c.count)
	c.lastCount = c.count
	return true
}

// idle ends the standing-queue state of a queue that emptied.
func (c *codel) idle() {
	c.firstAbove = 0
	c.dropping = false
}
