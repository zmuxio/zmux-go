package core

import (
	"math"

	"github.com/zmuxio/zmux-go/varint"
)

const (
	DatagramSingle uint8 = iota + 1
	DatagramDuplicate
	DatagramAdaptive
)

const DatagramRequireNative = 2

type DatagramDeadlineMode uint8

const (
	DatagramDeadlineInherit DatagramDeadlineMode = iota
	DatagramDeadlineNever
	DatagramDeadlineAfter
	DatagramDeadlineAt
)

// DatagramOptions holds overrides of a sender's datagram defaults.
type DatagramOptions struct {
	Copies                      int
	Deadline                    Time
	TTL                         Duration
	Copy, Carrier               uint8
	Urgent, HasUrgent           bool
	Independent, HasIndependent bool
	DeadlineMode                DatagramDeadlineMode
}

func (o DatagramOptions) with(override DatagramOptions) DatagramOptions {
	if override.Copy != 0 {
		o.Copy, o.Copies = override.Copy, override.Copies
	}
	if override.Carrier != 0 {
		o.Carrier = override.Carrier
	}
	if override.HasUrgent {
		o.Urgent, o.HasUrgent = override.Urgent, true
	}
	if override.HasIndependent {
		o.Independent, o.HasIndependent = override.Independent, true
	}
	if override.DeadlineMode != DatagramDeadlineInherit {
		o.DeadlineMode, o.TTL, o.Deadline = override.DeadlineMode, override.TTL, override.Deadline
	}
	return o
}

// ResolveDeadline checks and converts the deadline to the owner clock.
func (o DatagramOptions) ResolveDeadline(at Time) (Time, error) {
	switch o.DeadlineMode {
	case DatagramDeadlineInherit, DatagramDeadlineNever:
		return Never, nil
	case DatagramDeadlineAfter:
		if o.TTL < 0 || at.Add(o.TTL) == Never {
			return 0, ErrInvalid
		}
		return at.Add(o.TTL), nil
	case DatagramDeadlineAt:
		if o.Deadline == Never || o.Deadline == math.MinInt64 {
			return 0, ErrInvalid
		}
		return o.Deadline, nil
	default:
		return 0, ErrInvalid
	}
}

func (s *Session) SetDatagramOptions(st *Stream, o DatagramOptions) error {
	if _, err := o.ResolveDeadline(s.now); err != nil {
		return err
	}
	if st == nil {
		s.cfg.DatagramOptions = o
		return nil
	}
	if st.appReleased() {
		return &StreamError{Kind: StreamClosedLocally, StreamID: st.id}
	}
	if o == (DatagramOptions{}) {
		st.dgOptions = nil
	} else {
		st.dgOptions = &o
	}
	return nil
}

func (s *Session) DatagramOptions(st *Stream, o DatagramOptions) DatagramOptions {
	d := s.cfg.DatagramOptions
	if st != nil && st.dgOptions != nil {
		d = d.with(*st.dgOptions)
	}
	return d.with(o)
}

func (s *Session) DatagramPath(st *Stream, context uint64) (admission, path, native int, nativeAvailable bool) {
	h := 1
	if st != nil {
		admission = s.StreamMaxDatagramPayload(st, context)
		if !s.peerSeen || !s.feat.DatagramFamily || !st.bidi || context > varint.MaxVarint {
			return 0, 0, 0, false
		}
		h = streamDgramPrefixLen(st, context)
	} else {
		admission = s.MaxDatagramPayload(h)
	}
	record, _, channel := s.datagramCapacity(st)
	nativeAvailable = s.peerSeen && s.peerMaxDgram > 0 && !s.datagramExhausted() && channel >= 0
	if nativeAvailable && s.feat.DatagramFamily {
		nativeAvailable = min(channel, int(s.peerMaxDgram-1)) >= h+s.datagramSequenceWidth()
	}
	return admission, s.datagramPayload(h, record, channel), s.datagramPayload(h, 0, channel), nativeAvailable
}
