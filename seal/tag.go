package seal

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/djylb/timeshim"
)

// Bootstrap tags. A bootstrap msg1 names its client by
// Trunc16(HMAC-SHA256(K_tag, BE64(bucket))), with K_tag =
// HKDF-SHA256(IKM = the Noise psk, salt empty, info "zmux sc tag", 32) and
// bucket = floor(Unix ms / 600000) of the client's estimate of the server
// clock. The server indexes the tags of every client for the buckets
// within ±2 of its own.
const (
	bucketMs  = 10 * 60 * 1000
	tagBucket = 2 // buckets on either side
	tagSlots  = 2*tagBucket + 1
)

func bucketOf(ms int64) int64 { return floorDiv(ms, bucketMs) }

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func tagKey(psk *[keyLen]byte) (k [32]byte) {
	b, err := hkdf.Key(sha256.New, psk[:], nil, "zmux sc tag", 32)
	if err != nil {
		panic(err)
	}
	copy(k[:], b)
	return k
}

func tagAt(key *[32]byte, bucket int64) (t [nxTagLen]byte) {
	m := newHMAC(key[:])
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(bucket))
	m.Write(b[:])
	var sum [hashLen]byte
	m.Sum(sum[:0])
	copy(t[:], sum[:nxTagLen])
	return t
}

// TagTable is the server's table of bootstrap clients: each has an
// identifier, a PSK and the value its carriers are admitted with
// (Peer.Value). Lookups cost one map access; when the server's bucket
// moves on (every 10 minutes), the next lookup computes one tag per
// client. It is safe for concurrent use; its size is the host's to bound.
type TagTable struct {
	clk timeshim.Clock

	mu      sync.RWMutex
	entries map[string]*tagEntry
	index   map[[nxTagLen]byte]*tagEntry
	center  int64 // the bucket the index is built around
	built   bool
}

type tagEntry struct {
	id    string
	psk   [keyLen]byte
	key   [32]byte
	value any
	tags  [tagSlots][nxTagLen]byte // by bucket mod tagSlots
}

// NewTagTable returns an empty table on clock (nil: the host clock), the
// server's: give the Server the same clock.
func NewTagTable(clock timeshim.Clock) *TagTable {
	return &TagTable{clk: timeshim.Or(clock), entries: map[string]*tagEntry{}, index: map[[nxTagLen]byte]*tagEntry{}}
}

// ErrShortPSK is returned for a PSK shorter than 16 bytes.
var ErrShortPSK = errors.New("seal: PSK shorter than 16 bytes")

// Set adds the client id, or replaces it, with psk (at least 16 bytes, of
// at least 128 bits of uniform randomness) and value.
func (t *TagTable) Set(id string, psk []byte, value any) error {
	if len(psk) < 16 {
		return ErrShortPSK
	}
	e := &tagEntry{id: id, psk: compressPSK(psk), value: value}
	e.key = tagKey(&e.psk)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removeLocked(id)
	t.entries[id] = e
	if t.built {
		for b := t.center - tagBucket; b <= t.center+tagBucket; b++ {
			t.placeLocked(e, b)
		}
	}
	return nil
}

// Delete removes the client id.
func (t *TagTable) Delete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removeLocked(id)
}

// Len is the number of clients.
func (t *TagTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.entries)
}

func slot(b int64) int { return int(((b % tagSlots) + tagSlots) % tagSlots) }

func (t *TagTable) removeLocked(id string) {
	e := t.entries[id]
	if e == nil {
		return
	}
	delete(t.entries, id)
	if t.built {
		for b := t.center - tagBucket; b <= t.center+tagBucket; b++ {
			if t.index[e.tags[slot(b)]] == e {
				delete(t.index, e.tags[slot(b)])
			}
		}
	}
}

// placeLocked indexes e's tag of bucket b.
func (t *TagTable) placeLocked(e *tagEntry, b int64) {
	tag := tagAt(&e.key, b)
	e.tags[slot(b)] = tag
	t.index[tag] = e
}

// lookup finds the client of tag at the clock's now.
func (t *TagTable) lookup(tag [nxTagLen]byte) (*tagEntry, bool) {
	c := bucketOf(t.clk.Now().UnixMilli())
	t.mu.RLock()
	if t.built && t.center == c {
		e, ok := t.index[tag]
		t.mu.RUnlock()
		return e, ok
	}
	t.mu.RUnlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rollLocked(c)
	e, ok := t.index[tag]
	return e, ok
}

// rollLocked moves the index to center c: buckets that left the range
// are dropped, buckets that entered it are computed.
func (t *TagTable) rollLocked(c int64) {
	if t.built && t.center == c {
		return
	}
	lo, hi := c-tagBucket, c+tagBucket
	if !t.built || c-t.center >= tagSlots || t.center-c >= tagSlots {
		clear(t.index)
		for _, e := range t.entries {
			for b := lo; b <= hi; b++ {
				t.placeLocked(e, b)
			}
		}
	} else {
		for b := lo; b <= hi; b++ {
			if b >= t.center-tagBucket && b <= t.center+tagBucket {
				continue // still in range
			}
			for _, e := range t.entries {
				if old := e.tags[slot(b)]; t.index[old] == e {
					delete(t.index, old)
				}
				t.placeLocked(e, b)
			}
		}
	}
	t.center, t.built = c, true
}

// tagFor is the client's tag for psk at ms (Unix, server clock estimate).
func tagFor(psk *[keyLen]byte, ms int64) [nxTagLen]byte {
	k := tagKey(psk)
	return tagAt(&k, bucketOf(ms))
}
