package zmux

import (
	"fmt"
	"hash/maphash"
	"sync"
	"time"

	"github.com/djylb/timeshim"
)

// ReplayVerdict is the outcome of ReplayFilter.Check.
type ReplayVerdict uint8

const (
	// ReplayFresh: the key is new and now recorded.
	ReplayFresh ReplayVerdict = iota
	// ReplaySeen: the key was seen within its window.
	ReplaySeen
	// ReplayOutside: the stamp is outside the window of the filter's
	// clock, or before its NotBefore.
	ReplayOutside
	// ReplayFull: the filter holds Max keys and records nothing new.
	ReplayFull
)

func (v ReplayVerdict) String() string {
	switch v {
	case ReplayFresh:
		return "fresh"
	case ReplaySeen:
		return "seen"
	case ReplayOutside:
		return "outside"
	case ReplayFull:
		return "full"
	}
	return "invalid"
}

// ReplayConfig configures a ReplayFilter.
type ReplayConfig struct {
	// Window is how far a stamp may be from the clock (> 0).
	Window time.Duration
	// Max bounds the keys held (> 0).
	Max int
	// NotBefore rejects stamps before it (ReplayOutside), such as the
	// process start, so that what was sent to an earlier process with an
	// empty filter is not accepted again; zero: none.
	NotBefore time.Time
	// Clock is the filter's clock; nil: the host clock.
	Clock timeshim.Clock
}

// ReplayFilter remembers keys for a window of time so that each is
// accepted once (single-use nonces, tokens, request identifiers): Check
// accepts a key k stamped t when t is within Window of the clock and k was
// not seen, and records k until t leaves the window, at most 2·Window. At
// most Max keys are held, and a full filter accepts nothing new. Keys are
// hashed with a per-filter random seed; expiry costs O(1) per Check and no
// key is ever scanned. It is safe for concurrent use.
type ReplayFilter struct {
	window    int64 // ms
	width     int64 // ms per bucket
	max       int
	notBefore int64 // Unix ms; 0: none
	clk       timeshim.Clock
	seeds     [2]maphash.Seed

	mu sync.Mutex
	n  int
	b  []replayBucket
}

type replayKey [2]uint64

type replayBucket struct {
	idx int64
	m   map[replayKey]struct{}
}

// NewReplayFilter returns a filter for c; an invalid c is an
// ErrInvalidConfig error.
func NewReplayFilter(c ReplayConfig) (*ReplayFilter, error) {
	w := c.Window.Milliseconds()
	switch {
	case w <= 0:
		return nil, fmt.Errorf("%w: ReplayConfig.Window %v below 1ms", ErrInvalidConfig, c.Window)
	case c.Max <= 0:
		return nil, fmt.Errorf("%w: ReplayConfig.Max %d", ErrInvalidConfig, c.Max)
	}
	width := max(w/4, 1)
	// A key lives in the bucket of t+window: buckets from now-window+window
	// to now+window+window, plus one.
	n := (2*w+width-1)/width + 2
	f := &ReplayFilter{window: w, width: width, max: c.Max, clk: timeshim.Or(c.Clock),
		seeds: [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()},
		b:     make([]replayBucket, n)}
	if !c.NotBefore.IsZero() {
		f.notBefore = c.NotBefore.UnixMilli()
	}
	return f, nil
}

// Check checks k stamped t (Unix milliseconds) and records a fresh k. k
// may be of any length; the filter retains neither k nor its contents.
func (f *ReplayFilter) Check(k []byte, t int64) ReplayVerdict {
	return f.checkAt(k, t, f.clk.Now().UnixMilli())
}

// checkAt is Check at now (Unix ms).
func (f *ReplayFilter) checkAt(k []byte, t, now int64) ReplayVerdict {
	if t < now-f.window || t > now+f.window || f.notBefore != 0 && t < f.notBefore {
		return ReplayOutside
	}
	key := replayKey{maphash.Bytes(f.seeds[0], k), maphash.Bytes(f.seeds[1], k)}
	// Expiry: k matters until t+window, when its stamp leaves the window.
	idx := floorDiv(t+f.window, f.width)
	lowIdx := floorDiv(now, f.width)
	f.mu.Lock()
	defer f.mu.Unlock()
	nb := int64(len(f.b))
	for i := range f.b {
		if bk := &f.b[i]; bk.m != nil && bk.idx < lowIdx {
			f.n -= len(bk.m)
			bk.m = nil
		}
	}
	// A key with the same stamp sits in one bucket, but a replay may carry
	// another stamp: look in every live bucket.
	for i := range f.b {
		if _, dup := f.b[i].m[key]; dup {
			return ReplaySeen
		}
	}
	if f.n >= f.max {
		return ReplayFull
	}
	bk := &f.b[((idx%nb)+nb)%nb]
	if bk.m != nil && bk.idx != idx { // unreachable with n buckets; kept safe
		f.n -= len(bk.m)
		bk.m = nil
	}
	if bk.m == nil {
		bk.idx, bk.m = idx, map[replayKey]struct{}{}
	}
	bk.m[key] = struct{}{}
	f.n++
	return ReplayFresh
}

// Len is the number of keys held.
func (f *ReplayFilter) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
