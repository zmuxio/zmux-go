package admit

import (
	"container/heap"
	"context"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// PrefixWindow is immutable for a domain. An increase in Skew requires proof
// that the widened window still has complete replay history, or retirement of
// the affected tickets before switching windows.
type PrefixWindow struct {
	Slot time.Duration
	Skew int
}

func (w PrefixWindow) valid() bool {
	return validPrefixSlot(w.Slot) && w.Skew >= 0 && w.Skew <= (math.MaxInt-1)/2
}

// PrefixGeneration is one installed outer key and its inclusive issue-slot
// interval. At cutover U, set the outgoing LastSlot to U-1 and the incoming
// FirstSlot to U. Use math.MaxUint64 for an open-ended interval.
type PrefixGeneration struct {
	Key                 [32]byte
	FirstSlot, LastSlot uint64
}

func (g PrefixGeneration) contains(slot uint64) bool {
	return g.FirstSlot <= slot && slot <= g.LastSlot
}

func (g PrefixGeneration) intersects(current, skew uint64) bool {
	return g.FirstSlot <= prefixLastSlot(current, skew) && g.LastSlot >= current-min(current, skew)
}

// PrefixTicket is a ticket authority's record. FirstSlot and LastSlot are the
// inclusive UTC slots in which it is live. Version identifies an immutable
// record of this ID. Issue new credentials with distinct IDs.
type PrefixTicket struct {
	PrefixCredential
	Version             uint64
	FirstSlot, LastSlot uint64
	SingleUse           bool
}

func (t PrefixTicket) liveAt(current uint64) bool {
	return t.FirstSlot <= current && current <= t.LastSlot
}

// PrefixSchedule is an immutable local copy of the installed outer keys.
type PrefixSchedule struct {
	generations []PrefixGeneration
	version     uint64
}

// NewPrefixSchedule copies the outer keys for use by verifiers. Version must
// match the authority's revision and change on every schedule change.
func NewPrefixSchedule(version uint64, generations []PrefixGeneration) (*PrefixSchedule, error) {
	generations, err := prefixSchedule(generations)
	if err != nil {
		return nil, err
	}
	return &PrefixSchedule{generations: generations, version: version}, nil
}

// PrefixClaim is a conditional redemption from a verifier. CommitID uniquely
// identifies this operation, including when its reply is lost. The context
// carries the attempt's cancellation and deadline.
type PrefixClaim struct {
	ID, Nonce, CommitID [16]byte
	Version             uint64
	Schedule            uint64
	Generation          uint64 // first issue slot of the matched generation
	IssueSlot           uint64
	CheckedSlot         uint64 // C at which the verifier checked generation eligibility
	LocalSlot           uint64 // present UTC slot sampled immediately before Claim
}

// PrefixStore is the linearizable authority for one admission domain. All
// entrances share it, including across outer-key rotations. A durable store
// restores the schedule, tickets, floor and all live claims together. If it
// cannot prove their freshness it must fail closed.
type PrefixStore interface {
	Window() PrefixWindow
	// LocalSchedule returns the locally cached schedule and an optional
	// cached floor lower bound. It must not lock, allocate, or use remote I/O.
	// Publish rotations and revocations to the cache independently of requests.
	// A stale cache can refuse a presentation; Claim still checks the authority.
	LocalSchedule() (schedule *PrefixSchedule, floorHint uint64)
	// Floor reads the authoritative rollback floor after outer authentication.
	Floor(context.Context) (uint64, error)
	// Lookup returns an immutable ticket version and whether it exists and has
	// not been revoked. It runs once, after unique outer authentication.
	Lookup(context.Context, [16]byte) (PrefixTicket, bool, error)
	// Claim atomically checks the current floor and UTC slot, schedule,
	// generation interval, ticket version and validity, and both claims. A
	// different C than CheckedSlot requires rechecking outer eligibility;
	// implementations may refuse that attempt. On acceptance record (ID,Nonce),
	// consume a single-use ID, and advance the floor to C in one operation.
	// Expiry must also advance the floor atomically with removing the claims.
	// Keep nonce claims through IssueSlot+Skew and consumption through ticket
	// expiry. Honor cancellation and the attempt deadline at the commit point.
	Claim(context.Context, PrefixClaim) (bool, error)
	// ResolveClaim checks the outcome of the original CommitID after a failed
	// Claim call. It reports true only if that exact operation committed. It
	// neither claims a ticket nor reverses a committed claim. The verifier uses
	// a separate, short deadline even if admission was cancelled. If a
	// single-use claim commits and the request ends before admission or the
	// receipt cannot be confirmed in time, the ticket is lost. Issue a new ID.
	ResolveClaim(context.Context, PrefixClaim) (bool, error)
}

type prefixNonce struct{ id, nonce [16]byte }

type memoryPrefixTicket struct {
	ticket   PrefixTicket
	revoked  bool
	consumed bool
}

type prefixExpiry struct {
	key      prefixNonce
	lastSlot uint64
}

type prefixExpiries []prefixExpiry

func (p *prefixExpiries) Len() int           { return len(*p) }
func (p *prefixExpiries) Less(i, j int) bool { return (*p)[i].lastSlot < (*p)[j].lastSlot }
func (p *prefixExpiries) Swap(i, j int)      { (*p)[i], (*p)[j] = (*p)[j], (*p)[i] }
func (p *prefixExpiries) Push(x any)         { *p = append(*p, x.(prefixExpiry)) }
func (p *prefixExpiries) Pop() any {
	last := len(*p) - 1
	result := (*p)[last]
	*p = (*p)[:last]
	return result
}

// MemoryPrefixStore holds one domain's authority in process memory. Share the
// same instance across listeners and adapters. Use new ticket IDs and keys if
// restarting without restored replay state, or use a durable PrefixStore.
type MemoryPrefixStore struct {
	window   PrefixWindow
	capacity int

	schedule atomic.Pointer[PrefixSchedule]
	floor    atomic.Uint64

	mu       sync.Mutex
	tickets  map[[16]byte]memoryPrefixTicket
	claims   map[prefixNonce][16]byte
	expiries prefixExpiries
}

// NewMemoryPrefixStore constructs an authority. Capacity separately bounds
// retained nonce claims and the total number of IDs issued or revoked over
// the process lifetime for this authority.
// Zero selects 65536 for each bound. IDs leave permanent tombstones after
// expiry or revocation. At ID capacity all previously unseen IDs are denied.
// Use a durable PrefixStore for ticket issuers that must retain more IDs.
func NewMemoryPrefixStore(w PrefixWindow, generations []PrefixGeneration, capacity int) (*MemoryPrefixStore, error) {
	if !w.valid() || capacity < 0 {
		return nil, prefixConfigError("invalid prefix window or claim capacity")
	}
	if capacity == 0 {
		capacity = 1 << 16
	}
	s := &MemoryPrefixStore{window: w, capacity: capacity, tickets: make(map[[16]byte]memoryPrefixTicket),
		claims: make(map[prefixNonce][16]byte)}
	if err := s.SetGenerations(generations); err != nil {
		return nil, err
	}
	return s, nil
}

// Window returns the immutable slot length and skew.
func (s *MemoryPrefixStore) Window() PrefixWindow { return s.window }

// LocalSchedule returns the published schedule and a cached floor.
func (s *MemoryPrefixStore) LocalSchedule() (*PrefixSchedule, uint64) {
	return s.schedule.Load(), s.floor.Load()
}

// Floor returns the authoritative rollback floor.
func (s *MemoryPrefixStore) Floor(ctx context.Context) (uint64, error) {
	return s.floor.Load(), ctx.Err()
}

// SetGenerations replaces the installed schedule at once. To revoke a
// generation, remove it. To plan a rotation, install both intervals before
// issuing presentations with the incoming key.
func (s *MemoryPrefixStore) SetGenerations(generations []PrefixGeneration) error {
	generations, err := prefixSchedule(generations)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var version uint64
	if previous := s.schedule.Load(); previous != nil {
		version = previous.version
	}
	if version == math.MaxUint64 {
		return prefixConfigError("prefix generation schedule versions exhausted")
	}
	s.schedule.Store(&PrefixSchedule{generations: generations, version: version + 1})
	return nil
}

func prefixSchedule(generations []PrefixGeneration) ([]PrefixGeneration, error) {
	generations = slices.Clone(generations)
	slices.SortFunc(generations, func(a, b PrefixGeneration) int {
		switch {
		case a.FirstSlot < b.FirstSlot:
			return -1
		case a.FirstSlot > b.FirstSlot:
			return 1
		default:
			return 0
		}
	})
	for i, g := range generations {
		if g.FirstSlot > g.LastSlot || i > 0 && generations[i-1].LastSlot >= g.FirstSlot {
			return nil, prefixConfigError("invalid or overlapping prefix generation intervals")
		}
	}
	return generations, nil
}

// AddTicket installs a previously unused ticket ID. The authority's issuer
// must generate a fresh ID for each new ticket, including after revocation.
func (s *MemoryPrefixStore) AddTicket(t PrefixTicket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.tickets[t.ID]
	if exists || len(s.tickets) >= s.capacity || t.FirstSlot > t.LastSlot || t.LastSlot < s.floor.Load() {
		return prefixConfigError("duplicate or invalid prefix ticket")
	}
	s.tickets[t.ID] = memoryPrefixTicket{ticket: t}
	return nil
}

// RevokeTicket withdraws the ticket at once, including from in-flight
// claims. Revoking an unknown ID permanently reserves it. At ID capacity,
// every unknown ID is already barred from installation.
func (s *MemoryPrefixStore) RevokeTicket(id [16]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, exists := s.tickets[id]
	if exists || len(s.tickets) < s.capacity {
		t.revoked = true
		clear(t.ticket.Key[:])
		s.tickets[id] = t
	}
}

// Lookup retrieves the immutable version of a ticket.
func (s *MemoryPrefixStore) Lookup(ctx context.Context, id [16]byte) (PrefixTicket, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return PrefixTicket{}, false, err
	}
	t, ok := s.tickets[id]
	return t.ticket, ok && !t.revoked, nil
}

// Claim conditionally commits the nonce and single-use redemption.
func (s *MemoryPrefixStore) Claim(ctx context.Context, c PrefixClaim) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	current := max(c.LocalSlot, s.floor.Load())
	t, live := s.tickets[c.ID]
	key := prefixNonce{id: c.ID, nonce: c.Nonce}
	skew := uint64(s.window.Skew)
	schedule := s.schedule.Load()
	if !live || t.revoked || t.consumed || t.ticket.Version != c.Version || !t.ticket.liveAt(current) ||
		schedule.version != c.Schedule || !schedule.sameEligibleGenerations(c.CheckedSlot, current, skew) ||
		c.IssueSlot < current-min(current, skew) || c.IssueSlot > prefixLastSlot(current, skew) ||
		!schedule.issuedBy(c.Generation, c.IssueSlot) {
		return false, nil
	}
	s.expireClaims(current)
	if _, replay := s.claims[key]; replay || len(s.claims) >= s.capacity {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.claims[key] = c.CommitID
	heap.Push(&s.expiries, prefixExpiry{key: key, lastSlot: prefixLastSlot(c.IssueSlot, skew)})
	if t.ticket.SingleUse {
		t.consumed = true
		s.tickets[c.ID] = t
	}
	s.floor.Store(current)
	return true, nil
}

func (s *PrefixSchedule) sameEligibleGenerations(checked, current, skew uint64) bool {
	if checked == current {
		return true
	}
	for _, g := range s.generations {
		if g.intersects(checked, skew) != g.intersects(current, skew) {
			return false
		}
	}
	return true
}

func (s *PrefixSchedule) issuedBy(generation, slot uint64) bool {
	for _, g := range s.generations {
		if g.FirstSlot == generation {
			return g.contains(slot)
		}
	}
	return false
}

// ResolveClaim checks whether the same operation committed.
func (s *MemoryPrefixStore) ResolveClaim(ctx context.Context, c PrefixClaim) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	commit, ok := s.claims[prefixNonce{id: c.ID, nonce: c.Nonce}]
	return ok && commit == c.CommitID, nil
}

// Expire retires replay state outside the current window and drops expired
// ticket keys. It keeps the IDs as tombstones. The floor advances atomically
// with the retirement of replay state.
func (s *MemoryPrefixStore) Expire(now time.Time) {
	local, ok := prefixSlot(now, s.window.Slot)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := max(local, s.floor.Load())
	s.expireClaims(current)
	for id, t := range s.tickets {
		if t.ticket.LastSlot < current {
			s.tickets[id] = memoryPrefixTicket{revoked: true}
			if t.consumed {
				s.floor.Store(current)
			}
		}
	}
}

func (s *MemoryPrefixStore) expireClaims(current uint64) {
	for len(s.expiries) != 0 && s.expiries[0].lastSlot < current {
		old := heap.Pop(&s.expiries).(prefixExpiry)
		delete(s.claims, old.key)
		s.floor.Store(current)
	}
}
