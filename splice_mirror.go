package zmux

import (
	"context"
	"io"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// SpliceMirrorPhase selects the point sampled by a mirror.
type SpliceMirrorPhase uint8

// Mirrors can sample at input, output and drop boundaries.
const (
	SpliceMirrorInput SpliceMirrorPhase = 1 << iota
	SpliceMirrorOutput
	SpliceMirrorDrop
)

// SpliceMirrorOptions bounds queued samples and unfinished output samples.
type SpliceMirrorOptions struct {
	Items, BackingBytes, MaxPayload int
	Every                           uint64
	Phases                          SpliceMirrorPhase
	// Lease shares immutable roots. Samples that would extend a receive
	// borrow are omitted. The default copies into an independent buffer.
	Lease bool
}

// SpliceMirrorEvent describes a sample captured at an observation boundary.
type SpliceMirrorEvent struct {
	Data         []byte
	Meta         SpliceMeta
	Member, Size int
	Phase        SpliceMirrorPhase
}

type spliceSample struct {
	event SpliceMirrorEvent
	parts []spliceChunk
}

// SpliceMirror holds independently bounded samples from splice directions.
type SpliceMirror struct {
	mu          sync.Mutex
	opts        SpliceMirrorOptions
	events      chan *spliceSample
	roots       map[*core.Buf]int
	held, items int
	closed      bool
}

// NewSpliceMirror creates an independently bounded asynchronous sample queue.
func NewSpliceMirror(opts SpliceMirrorOptions) (*SpliceMirror, error) {
	if opts.Items <= 0 || opts.MaxPayload < 0 || opts.MaxPayload > MaxRecordSize || opts.BackingBytes <= 0 || opts.Every == 0 || opts.Phases & ^(SpliceMirrorInput|SpliceMirrorOutput|SpliceMirrorDrop) != 0 {
		return nil, ErrInvalidConfig
	}
	if opts.Phases == 0 {
		opts.Phases = SpliceMirrorInput | SpliceMirrorOutput | SpliceMirrorDrop
	}
	return &SpliceMirror{opts: opts, events: make(chan *spliceSample, opts.Items), roots: make(map[*core.Buf]int)}, nil
}

func (m *SpliceMirror) sampled(meta SpliceMeta, phase SpliceMirrorPhase) bool {
	return m != nil && m.opts.Phases&phase != 0 && (meta.Sequence-1)%m.opts.Every == 0
}

// Next transfers a sample to the consumer. Data is private to the caller.
func (m *SpliceMirror) Next(ctx context.Context) (SpliceMirrorEvent, error) {
	select {
	case <-ctx.Done():
		return SpliceMirrorEvent{}, ctx.Err()
	case s, ok := <-m.events:
		if !ok {
			return SpliceMirrorEvent{}, io.EOF
		}
		event := s.event
		if len(s.parts) != 0 {
			event.Data = copySpliceSample(s.parts, min(event.Size, m.opts.MaxPayload))
		}
		m.mu.Lock()
		m.release(s)
		m.mu.Unlock()
		return event, nil
	}
}

func (m *SpliceMirror) release(s *spliceSample) {
	m.items--
	m.held -= cap(s.event.Data)
	for _, part := range s.parts {
		b := part.buf
		m.roots[b]--
		if m.roots[b] == 0 {
			delete(m.roots, b)
			m.held -= cap(b.B)
		}
		b.Unref()
	}
}

// Close releases queued samples and wakes consumers.
func (m *SpliceMirror) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.events)
	for s := range m.events {
		m.release(s)
	}
}

func copySpliceSample(parts []spliceChunk, n int) []byte {
	data := make([]byte, n)
	pos := 0
	for _, part := range parts {
		pos += copy(data[pos:], part.Data)
		if pos == n {
			break
		}
	}
	return data
}

func (m *SpliceMirror) sample(meta SpliceMeta, phase SpliceMirrorPhase, i int, member spliceMember) *spliceSample {
	if !m.mu.TryLock() {
		return nil
	}
	defer m.mu.Unlock()
	if m.closed || m.items >= m.opts.Items {
		return nil
	}
	n := min(member.n, m.opts.MaxPayload)
	s := &spliceSample{event: SpliceMirrorEvent{Meta: cloneSpliceMeta(meta), Phase: phase, Member: i, Size: member.n}}
	if !m.opts.Lease {
		if n > m.opts.BackingBytes-m.held {
			return nil
		}
		s.event.Data = copySpliceSample(member.parts, n)
		m.held += n
		m.items++
		return s
	}
	left := n
	m.items++
	for _, p := range member.parts {
		if left == 0 {
			break
		}
		if len(p.Data) == 0 {
			continue
		}
		if p.lease != nil || p.buf == nil || m.roots[p.buf] == 0 && cap(p.buf.B) > m.opts.BackingBytes-m.held {
			m.release(s)
			return nil
		}
		if m.roots[p.buf] == 0 {
			m.held += cap(p.buf.B)
		}
		m.roots[p.buf]++
		p.buf.Ref()
		p.Data = p.Data[:min(left, len(p.Data))]
		left -= len(p.Data)
		s.parts = append(s.parts, p)
	}
	return s
}

func (m *SpliceMirror) finish(s *spliceSample, publish bool) {
	if s == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if publish && !m.closed {
		m.events <- s
	} else {
		m.release(s)
	}
}

func (d *spliceRelay) sample(meta SpliceMeta, phase SpliceMirrorPhase, i int, member spliceMember) *spliceSample {
	if !d.mirror.sampled(meta, phase) {
		return nil
	}
	s := d.mirror.sample(meta, phase, i, member)
	if s == nil {
		d.result.MirrorDropped++
	} else {
		d.result.MirrorCopyBytes += uint64(len(s.event.Data))
	}
	return s
}

func (d *spliceRelay) publish(meta SpliceMeta, phase SpliceMirrorPhase, i int, member spliceMember) {
	d.mirror.finish(d.sample(meta, phase, i, member), true)
}
