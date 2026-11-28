package zmux

import (
	"cmp"
	"context"
	"errors"
	"math/bits"
	"net"
	"slices"
	"sync"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// ErrAccountingSources reports exhaustion of a strict source budget.
var ErrAccountingSources = errors.New("zmux: accounting source limit")

// ErrAccountingSourceMissing reports an unattributed half under strict accounting.
var ErrAccountingSourceMissing = errors.New("zmux: accounting source missing")

// AccountingPolicy enables completed-record accounting on every attachment.
// SourceLimit bounds distinct sources per attachment; zero uses 16.
type AccountingPolicy struct {
	SourceLimit int
	Enabled     bool
	Strict      bool
}

func (p AccountingPolicy) sourceLimit() int {
	if p.SourceLimit == 0 {
		return 16
	}
	return p.SourceLimit
}

// RecordTotals gives protocol bytes, including record and channel framing.
// PartialBytes counts known bytes of incomplete records and does not enter
// Bytes. Imprecise marks an I/O failure whose byte count was unavailable.
type RecordTotals struct {
	Bytes, Records, PartialBytes                 uint64
	ChannelBytes, ChannelDatagrams               uint64
	Valid, AuthenticationFailed, ProtocolInvalid uint64
	Imprecise                                    bool
}

func (t *RecordTotals) merge(other RecordTotals) {
	t.Bytes = addAccounting(t.Bytes, other.Bytes)
	t.Records = addAccounting(t.Records, other.Records)
	t.PartialBytes = addAccounting(t.PartialBytes, other.PartialBytes)
	t.ChannelBytes = addAccounting(t.ChannelBytes, other.ChannelBytes)
	t.ChannelDatagrams = addAccounting(t.ChannelDatagrams, other.ChannelDatagrams)
	t.Valid = addAccounting(t.Valid, other.Valid)
	t.AuthenticationFailed = addAccounting(t.AuthenticationFailed, other.AuthenticationFailed)
	t.ProtocolInvalid = addAccounting(t.ProtocolInvalid, other.ProtocolInvalid)
	t.Imprecise = t.Imprecise || other.Imprecise
}

// recordReadError preserves framed I/O progress at a transform's failed read.
type recordReadError struct {
	cause  error
	bytes  uint64
	badTag bool
}

func (e *recordReadError) Error() string { return e.cause.Error() }
func (e *recordReadError) Unwrap() error { return e.cause }

type recordWriteError struct {
	cause    error
	bytes    uint64
	complete bool
}

func (e *recordWriteError) Error() string { return e.cause.Error() }
func (e *recordWriteError) Unwrap() error { return e.cause }

func accountMessageRead(record *ReceivedMessage, err error, graceful bool) RecordTotals {
	if record.Present() {
		if record.framedBytes != 0 {
			return RecordTotals{Bytes: uint64(record.framedBytes), Records: 1}
		}
		return RecordTotals{Bytes: uint64(record.Len() + varint.SizeVarint(uint64(record.Len()))), Records: 1}
	}
	if progress, ok := errors.AsType[*recordReadError](err); ok {
		if progress.badTag {
			return RecordTotals{Bytes: progress.bytes, Records: 1, AuthenticationFailed: 1}
		}
		return RecordTotals{PartialBytes: progress.bytes}
	}
	return RecordTotals{Imprecise: err != nil && !graceful}
}

func addAccounting(a, b uint64) uint64 {
	sum, carry := bits.Add64(a, b, 0)
	return sum | -carry
}

// HalfTotals splits one source's totals between active and retired lanes.
type HalfTotals struct {
	Source          CarrierHalfSource
	Active, Retired RecordTotals
	SourceOverflow  bool
}

// PipeAccounting is the accounting of an attachment, including retired lanes.
type PipeAccounting struct {
	Send, Receive []HalfTotals
	ActiveLanes   []LaneAccounting
	PipeID        PipeID
}

// LaneAccounting identifies a live half and its totals. Retired lanes are
// folded into the ticket's bounded source buckets.
type LaneAccounting struct {
	Source CarrierHalfSource
	Totals RecordTotals
	Lane   uint64
	Send   bool
}

// AccountingTicket completes after the final physical segment and all its
// workers stop. It may be waited on more than once.
type AccountingTicket interface {
	Snapshot() PipeAccounting
	Wait(context.Context) (PipeAccounting, error)
}

type pipeAccounting struct {
	done     chan struct{}
	sources  map[CarrierHalfSource]struct{}
	active   map[*accountHalf]struct{}
	totals   PipeAccounting
	segments int
	limit    int
	mu       sync.Mutex
	strict   bool
}

func newPipeAccounting(id uint64, policy AccountingPolicy) *pipeAccounting {
	return &pipeAccounting{done: make(chan struct{}), totals: PipeAccounting{PipeID: PipeID(id)},
		sources: make(map[CarrierHalfSource]struct{}), limit: policy.sourceLimit(), strict: policy.Strict}
}

func (a *pipeAccounting) startSegment() *accountSegment {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.segments++
	return &accountSegment{a: a}
}

func (a *pipeAccounting) checkSources(receive, send CarrierHalfSource, read, write bool) error {
	if !a.strict {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return checkAccountingSources(a.sources, a.limit, receive, send, read, write)
}

func checkAccountingSources(sources map[CarrierHalfSource]struct{}, limit int, receive, send CarrierHalfSource, read, write bool) error {
	if read && receive == (CarrierHalfSource{}) || write && send == (CarrierHalfSource{}) {
		return ErrAccountingSourceMissing
	}
	if !read {
		receive = CarrierHalfSource{}
	}
	if !write {
		send = CarrierHalfSource{}
	}
	extra := 0
	for i, src := range [2]CarrierHalfSource{receive, send} {
		if src == (CarrierHalfSource{}) || i == 1 && src == receive {
			continue
		}
		if _, ok := sources[src]; !ok {
			extra++
		}
	}
	if len(sources)+extra > limit {
		return ErrAccountingSources
	}
	return nil
}

// accountSegment is one physical attachment, including its lanes. Early
// bootstrap restart can place more than one segment on the same ticket.
type accountSegment struct {
	a *pipeAccounting
}

func (s *accountSegment) retire() {
	if s == nil {
		return
	}
	s.a.mu.Lock()
	defer s.a.mu.Unlock()
	s.retireLocked()
}

func (s *accountSegment) retireLocked() {
	for h := range s.a.active {
		if h.segment == s {
			h.retireLocked()
		}
	}
}

func (s *accountSegment) finish() {
	a := s.a
	a.mu.Lock()
	defer a.mu.Unlock()
	s.retireLocked()
	a.segments--
	if a.segments == 0 {
		close(a.done)
	}
}

type accountHalf struct {
	segment *accountSegment
	active  RecordTotals
	index   int
	lane    uint64
	send    bool
	retired bool
}

// startHalf is called after source admission under the session lock.
func (s *accountSegment) startHalf(source CarrierHalfSource, send bool, lane uint64) *accountHalf {
	a := s.a
	a.mu.Lock()
	defer a.mu.Unlock()
	overflow := false
	if source != (CarrierHalfSource{}) {
		if _, ok := a.sources[source]; !ok {
			switch {
			case len(a.sources) < a.limit:
				a.sources[source] = struct{}{}
			case a.strict:
				panic(ErrAccountingSources)
			default:
				source, overflow = CarrierHalfSource{}, true
			}
		}
	}
	totals := a.totals.half(send)
	i := slices.IndexFunc(*totals, func(e HalfTotals) bool {
		return e.Source == source && e.SourceOverflow == overflow
	})
	if i < 0 {
		i = len(*totals)
		*totals = append(*totals, HalfTotals{Source: source, SourceOverflow: overflow})
	}
	h := &accountHalf{segment: s, send: send, index: i, lane: lane}
	if a.active == nil {
		a.active = make(map[*accountHalf]struct{})
	}
	a.active[h] = struct{}{}
	return h
}

func (a *PipeAccounting) half(send bool) *[]HalfTotals {
	if send {
		return &a.Send
	}
	return &a.Receive
}

func (h *accountHalf) retire() {
	if h == nil {
		return
	}
	a := h.segment.a
	a.mu.Lock()
	defer a.mu.Unlock()
	h.retireLocked()
}

func (h *accountHalf) retireLocked() {
	if h.retired {
		return
	}
	a := h.segment.a
	(*a.totals.half(h.send))[h.index].Retired.merge(h.active)
	h.active = RecordTotals{}
	h.retired = true
	delete(a.active, h)
}

// A batch already in flight can finish after logical retirement. Its final
// I/O observation goes directly to the retired bucket before the ticket ends.
func (h *accountHalf) add(totals RecordTotals, retire bool) {
	a := h.segment.a
	a.mu.Lock()
	defer a.mu.Unlock()
	t := &h.active
	if h.retired {
		t = &(*a.totals.half(h.send))[h.index].Retired
	}
	t.merge(totals)
	if retire {
		h.retireLocked()
	}
}

func tallyStreamRead(data wire.Parts) (totals RecordTotals) {
	for data.Len() != 0 {
		size, n := data.Varint()
		if n == 0 || size == 0 || size > uint64(data.Len()-n) || size > MaxRecordSize {
			totals.PartialBytes = uint64(data.Len())
			break
		}
		length := n + int(size)
		totals.Bytes += uint64(length)
		totals.Records++
		data = data.Slice(length, data.Len())
	}
	return totals
}

func (a *pipeAccounting) Snapshot() PipeAccounting {
	a.mu.Lock()
	defer a.mu.Unlock()
	view := PipeAccounting{PipeID: a.totals.PipeID, Send: slices.Clone(a.totals.Send), Receive: slices.Clone(a.totals.Receive)}
	if len(a.active) != 0 {
		view.ActiveLanes = make([]LaneAccounting, 0, len(a.active))
	}
	for h := range a.active {
		source := &(*view.half(h.send))[h.index]
		source.Active.merge(h.active)
		view.ActiveLanes = append(view.ActiveLanes, LaneAccounting{Source: source.Source, Totals: h.active, Lane: h.lane, Send: h.send})
	}
	slices.SortFunc(view.ActiveLanes, func(a, b LaneAccounting) int {
		if a.Send != b.Send {
			if a.Send {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.Lane, b.Lane)
	})
	return view
}

func (a *pipeAccounting) Wait(ctx context.Context) (PipeAccounting, error) {
	select {
	case <-a.done:
		return a.Snapshot(), nil
	case <-ctx.Done():
		select {
		case <-a.done:
			return a.Snapshot(), nil
		default:
		}
		return a.Snapshot(), ctx.Err()
	}
}

// Accounting returns the final accounting ticket of an observed attachment,
// including bootstrap. A ticket remains valid after the attachment retires.
func (p *Pipe) Accounting() (AccountingTicket, error) {
	if p == nil || p.c == nil || p.end == nil {
		return nil, net.ErrClosed
	}
	var a *pipeAccounting
	p.c.peek(func() { a = p.end.accounting })
	if a == nil {
		return nil, ErrNotNegotiated
	}
	return a, nil
}
