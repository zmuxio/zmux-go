package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// SpliceUnitKind identifies the boundary of an input or injected unit.
type SpliceUnitKind uint8

// Unit kinds distinguish reliable byte chunks, reliable capsules and datagrams.
const (
	SpliceStream SpliceUnitKind = iota
	SpliceCapsule
	SpliceDatagram
)

// SpliceOrigin identifies how a unit entered the splice.
type SpliceOrigin uint8

// Unit origins distinguish received and injected data.
const (
	SpliceReceived SpliceOrigin = iota
	SpliceInjected
)

// SpliceMeta stays the same throughout a unit's chain. Offset is the
// source application's reliable byte offset, including any capsule header.
type SpliceMeta struct {
	SourceTag, TargetTag         string
	OpenInfo                     []byte
	TLVs                         []TLV
	Sequence, Offset, Generation uint64
	Type, ContextID              uint64
	SourceID, TargetID           StreamID
	Kind                         SpliceUnitKind
	Origin                       SpliceOrigin
	Reverse                      bool // true for B to A
	HasSourceID, HasTargetID     bool
	Replayable                   bool
}

// SpliceHooks are immutable after admission. Each shape has its own chain.
type SpliceHooks struct {
	Stream, Capsule, Datagram []SpliceHook
}

// SpliceHook runs synchronously outside session locks. It may inspect all
// members and decide once for the group. It must return promptly and must
// avoid I/O and waiting for commands on this splice. Different directions
// may call the same hook concurrently.
type SpliceHook func(context.Context, SpliceUnit) (SpliceDecision, error)

// SpliceAction selects the disposition of a unit.
type SpliceAction uint8

// Actions pass a group, drop it, or reset the direction.
const (
	SpliceForward SpliceAction = iota
	SpliceDrop
	SpliceResetByHook
)

// SpliceDecision forwards unchanged when zero. A non-nil Replacement is
// an ordered list of output payloads created or borrowed in this invocation.
// An empty list drops the unit. Code is the application code for Reset.
type SpliceDecision struct {
	Wait        SpliceWait
	Replacement []SplicePayload
	Code        StreamErrorCode
	Action      SpliceAction
}

// SpliceWait sets the earliest commit, optional closed-channel resume
// condition and expiry. Durations are relative to this unit's admission.
type SpliceWait struct {
	Resume <-chan struct{}
	After  time.Duration
	Expiry time.Duration
}

// SpliceLimits bounds the units and queues of each direction. Zero fields
// select defaults. Capsule and datagram unit sizes obey protocol limits.
type SpliceLimits struct {
	HookSteps            int           // maximum hooks per shape (default 16)
	OutputItems          int           // number of outputs of one input (default 16)
	OutputBytes          int           // logical bytes of one output group (default 256 KiB)
	BackingBytes         int           // input and replacement backing per group (default 1 MiB)
	Segments             int           // payload descriptors per group (default 256)
	QueueItems           int           // admitted injected and delayed units (default 64)
	QueueBytes           int           // backing bytes of queued units (default 4 MiB)
	ReliableCleanupSlots int           // outstanding reliable limiter callbacks (default 1)
	DatagramCleanupSlots int           // outstanding datagram group callbacks (default 4)
	Residence            time.Duration // total assembly or wait cap (default 30 s, negative disables)
}

func (l SpliceLimits) defaults(unit int) (SpliceLimits, error) {
	if unit <= 0 || unit > math.MaxInt/2 || l.OutputBytes > math.MaxInt-unit {
		return l, ErrInvalidConfig
	}
	if l.OutputBytes == 0 {
		l.OutputBytes = max(256<<10, unit)
	}
	for _, p := range []struct {
		field *int
		def   int
	}{
		{&l.HookSteps, 16}, {&l.OutputItems, 16}, {&l.OutputBytes, l.OutputBytes},
		{&l.BackingBytes, max(1<<20, unit+l.OutputBytes)}, {&l.Segments, 256},
		{&l.ReliableCleanupSlots, 1}, {&l.DatagramCleanupSlots, 4}, {&l.QueueItems, 64}, {&l.QueueBytes, max(4<<20, unit)},
	} {
		if *p.field < 0 {
			return l, ErrInvalidConfig
		}
		if *p.field == 0 {
			*p.field = p.def
		}
	}
	if l.Residence == 0 {
		l.Residence = 30 * time.Second
	}
	// A single select covers control, session events, the latest
	// timer and every pending one-shot resume. Reflect limits it to 65536 channels.
	const maxResumes = 65536 - 16
	if l.Segments < 2 || l.QueueItems >= maxResumes ||
		l.HookSteps > (maxResumes-l.QueueItems)/(l.QueueItems+1) || l.OutputBytes < unit || l.QueueBytes < unit ||
		l.BackingBytes < unit || l.BackingBytes-unit < l.OutputBytes {
		return l, fmt.Errorf("%w: splice budgets cannot hold the largest unit and its outputs", ErrInvalidConfig)
	}
	return l, nil
}

func (l SpliceLimits) deadline(admitted core.Time) core.Time {
	if l.Residence < 0 {
		return core.Never
	}
	return admitted.Add(core.Duration(l.Residence))
}

func (h SpliceHooks) freeze(packet bool, maxSteps int) (SpliceHooks, error) {
	if packet && len(h.Stream) != 0 || !packet && len(h.Capsule) != 0 {
		return SpliceHooks{}, fmt.Errorf("%w: hook chain does not match splice mode", ErrInvalidConfig)
	}
	for _, chain := range [...][]SpliceHook{h.Stream, h.Capsule, h.Datagram} {
		if len(chain) > maxSteps {
			return SpliceHooks{}, ErrInvalidConfig
		}
		if slices.ContainsFunc(chain, func(h SpliceHook) bool { return h == nil }) {
			return SpliceHooks{}, fmt.Errorf("%w: nil splice hook", ErrInvalidConfig)
		}
	}
	return SpliceHooks{Stream: slices.Clone(h.Stream), Capsule: slices.Clone(h.Capsule), Datagram: slices.Clone(h.Datagram)}, nil
}

func (h SpliceHooks) chain(k SpliceUnitKind) []SpliceHook {
	switch k {
	case SpliceStream:
		return h.Stream
	case SpliceCapsule:
		return h.Capsule
	default:
		return h.Datagram
	}
}

func (h SpliceHooks) empty() bool {
	return len(h.Stream)+len(h.Capsule)+len(h.Datagram) == 0
}

// SpliceUnit contains a callback-scoped, read-only view of a group.
type SpliceUnit struct {
	scope *spliceScope
	meta  SpliceMeta
	count int
}

// Meta returns the incoming unit's metadata. The open data belongs to this
// invocation and can be retained or changed without affecting later hooks.
func (u SpliceUnit) Meta() SpliceMeta { return cloneSpliceMeta(u.meta) }

func cloneSpliceMeta(m SpliceMeta) SpliceMeta {
	m.OpenInfo = slices.Clone(m.OpenInfo)
	m.TLVs = slices.Clone(m.TLVs)
	for i := range m.TLVs {
		m.TLVs[i].Value = slices.Clone(m.TLVs[i].Value)
	}
	return m
}

// Len returns the number of members in the group.
func (u SpliceUnit) Len() int { return u.count }

// Payload returns a member. An invalid index gives a zero handle.
func (u SpliceUnit) Payload(index int) SplicePayload {
	if index < 0 || index >= u.count {
		return SplicePayload{}
	}
	return SplicePayload{u.scope, index}
}

// Alloc creates a writable output of n bytes, initially zero. Fill it using
// WriteAt and return it in Replacement. Writes become invalid when the
// callback ends.
func (u SpliceUnit) Alloc(n int) (SplicePayload, error) {
	return u.AllocBacking(n, n)
}

// AllocBacking reserves the entire backing capacity and makes its first n
// bytes writable. Only the n-byte output is exposed when it is frozen.
func (u SpliceUnit) AllocBacking(n, capacity int) (SplicePayload, error) {
	s := u.scope
	if s == nil {
		return SplicePayload{}, ErrSplicePayload
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return SplicePayload{}, ErrSplicePayload
	}
	if n < 0 || n > s.maxItem || capacity < n || capacity > s.left || s.handles == 0 || s.segments == 0 {
		s.failed = true
		return SplicePayload{}, ErrSpliceBudget
	}
	s.left -= capacity
	s.segments--
	ch := ownedSpliceChunk(make([]byte, n, capacity))
	return s.add(splicePayloadView{parts: []spliceChunk{ch}, n: n, writable: true}), nil
}

// SplicePayload is an opaque handle valid only in its own callback.
type SplicePayload struct {
	scope *spliceScope
	index int
}

// ErrSplicePayload identifies an invalid or expired payload handle.
var ErrSplicePayload = errors.New("zmux: invalid splice payload")

// ErrSpliceBudget identifies a splice unit or queue exceeding its budget.
var ErrSpliceBudget = errors.New("zmux: splice budget exceeded")

type splicePayloadView struct {
	parts    []spliceChunk
	n        int
	writable bool
}

type spliceScope struct {
	copied                           uint64
	views                            []splicePayloadView
	maxItem, left, handles, segments int
	mu                               sync.Mutex
	failed                           bool
	open                             bool
}

func (s *spliceScope) add(v splicePayloadView) SplicePayload {
	p := SplicePayload{s, len(s.views)}
	s.views = append(s.views, v)
	s.handles--
	return p
}

func (s *spliceScope) view(p SplicePayload) *splicePayloadView {
	if s.open && p.scope == s && p.index >= 0 && p.index < len(s.views) {
		return &s.views[p.index]
	}
	return nil
}

func (s *spliceScope) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
}

func (s *spliceScope) closeLocked() {
	s.open = false
	for i := range s.views {
		if s.views[i].writable {
			releaseSpliceChunks(s.views[i].parts)
		}
	}
	s.views = nil
}

// Len returns the size of this payload, or zero once the callback ends.
func (p SplicePayload) Len() int {
	s := p.scope
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v := s.view(p); v != nil {
		return v.n
	}
	return 0
}

// CopyTo copies payload data into dst starting at offset off. It follows
// io.ReaderAt's short-read convention.
func (p SplicePayload) CopyTo(dst []byte, off int) (int, error) {
	s := p.scope
	if s == nil {
		return 0, ErrSplicePayload
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.view(p)
	if v == nil || off < 0 || off > v.n {
		return 0, ErrSplicePayload
	}
	n := 0
	for _, part := range v.parts {
		if off >= len(part.Data) {
			off -= len(part.Data)
			continue
		}
		n += copy(dst[n:], part.Data[off:])
		off = 0
		if n == len(dst) {
			break
		}
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt fills an allocated payload without exporting its writable backing.
func (p SplicePayload) WriteAt(src []byte, off int) (int, error) {
	s := p.scope
	if s == nil {
		return 0, ErrSplicePayload
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.view(p)
	if v == nil || !v.writable || off < 0 || off > v.n {
		return 0, ErrSplicePayload
	}
	n := copy(v.parts[0].Data[off:], src)
	s.copied += uint64(n)
	if n != len(src) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// Slice selects an immutable subrange with no payload copy.
func (p SplicePayload) Slice(lo, hi int) (SplicePayload, error) {
	s := p.scope
	if s == nil {
		return SplicePayload{}, ErrSplicePayload
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.view(p)
	if v == nil || v.writable || lo < 0 || hi < lo || hi > v.n {
		return SplicePayload{}, ErrSplicePayload
	}
	if s.handles == 0 {
		s.failed = true
		return SplicePayload{}, ErrSpliceBudget
	}
	parts := make([]spliceChunk, 0, min(len(v.parts), s.segments))
	n := hi - lo
	for _, part := range v.parts {
		if lo < len(part.Data) && hi > lo {
			if len(parts) == s.segments {
				s.failed = true
				return SplicePayload{}, ErrSpliceBudget
			}
			data := part.Data[lo:min(hi, len(part.Data)):min(hi, len(part.Data))]
			parts = append(parts, spliceChunk{Data: data, buf: part.buf, lease: part.lease})
		}
		lo = max(0, lo-len(part.Data))
		hi -= len(part.Data)
	}
	s.segments -= len(parts)
	return s.add(splicePayloadView{parts: parts, n: n}), nil
}

// spliceChunk has a single owner inside the relay. Hook slices borrow its
// bytes until capture takes a separate reference before closing the scope.
type spliceChunk struct {
	buf   *core.Buf
	Data  []byte
	lease *core.SpliceLease
}

func (ch *spliceChunk) Release() {
	ch.lease.Release()
	if ch.buf != nil {
		ch.buf.Unref()
	}
	*ch = spliceChunk{}
}

type spliceMember struct {
	parts   []spliceChunk
	n, wire int
}

func ownedSpliceChunk(p []byte) spliceChunk {
	if cap(p) == 0 {
		return spliceChunk{}
	}
	b := &core.Buf{B: p}
	b.Ref()
	return spliceChunk{Data: p, buf: b}
}

func releaseSpliceChunks(parts []spliceChunk) {
	for i := range parts {
		parts[i].Release()
	}
}

func releaseSpliceMembers(group []spliceMember) {
	for _, member := range group {
		releaseSpliceChunks(member.parts)
	}
}

func spliceGroupCost(group []spliceMember) (bytes, segments int) {
	seen := make(map[*core.Buf]struct{})
	for _, member := range group {
		segments += len(member.parts)
		for _, ch := range member.parts {
			if ch.buf == nil {
				bytes += cap(ch.Data)
				continue
			}
			if _, ok := seen[ch.buf]; !ok {
				seen[ch.buf] = struct{}{}
				bytes += cap(ch.buf.B)
			}
		}
	}
	return bytes, segments
}

func (s *spliceScope) capture(outputs []SplicePayload, l SpliceLimits) ([]spliceMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.closeLocked()
	if len(outputs) > l.OutputItems {
		return nil, ErrSpliceBudget
	}
	group := make([]spliceMember, 0, len(outputs))

	bytes, segments := l.OutputBytes, l.Segments
	for _, p := range outputs {
		v := s.view(p)
		if v == nil {
			return nil, ErrSplicePayload
		}
		if v.n > s.maxItem || v.n > bytes || len(v.parts) > segments {
			return nil, ErrSpliceBudget
		}
		bytes -= v.n
		segments -= len(v.parts)

		group = append(group, spliceMember{parts: v.parts, n: v.n})
	}
	for i := range group {
		group[i].parts = slices.Clone(group[i].parts)
		for j := range group[i].parts {
			ch := &group[i].parts[j]
			if ch.buf != nil {
				ch.buf.Ref()
				ch.lease.Retain()
			}
		}
	}
	return group, nil
}

func callSpliceHook(ctx context.Context, h SpliceHook, u SpliceUnit) (decision SpliceDecision, err error) {
	defer func() {
		if value := recover(); value != nil {
			if cause, ok := value.(error); ok {
				err = fmt.Errorf("zmux: splice hook panicked: %w", cause)
			} else {
				err = fmt.Errorf("zmux: splice hook panicked: %v", value)
			}
		}
	}()
	return h(ctx, u)
}
