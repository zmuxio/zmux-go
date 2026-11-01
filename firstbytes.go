package zmux

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// FirstBytes is what the first bytes of a carrier tell of its content
// (ClassifyFirstBytes).
type FirstBytes uint8

const (
	// FirstBytesNeedMore means that more bytes are needed to tell.
	FirstBytesNeedMore FirstBytes = iota
	// FirstBytesRecord means that the bytes can begin a first record of
	// this specification.
	FirstBytesRecord
	// FirstBytesOther means that the bytes cannot begin a first record.
	FirstBytesOther
)

func (c FirstBytes) String() string {
	switch c {
	case FirstBytesNeedMore:
		return "need more"
	case FirstBytesRecord:
		return "record"
	case FirstBytesOther:
		return "other"
	}
	return "invalid"
}

// MaxFirstBytes is the most bytes ClassifyFirstBytes needs.
const MaxFirstBytes = 5

// firstLeads are the first bytes of the encodings of the frame types that
// may begin a first record.
var firstLeads = func() (l [len(wire.FirstFrameTypes)]byte) {
	for i, t := range wire.FirstFrameTypes {
		l[i] = varint.AppendVarint(nil, t)[0]
	}
	return l
}()

func firstLead(b byte) bool {
	for _, l := range firstLeads {
		if b == l {
			return true
		}
	}
	return false
}

// ClassifyFirstBytes tells whether b, the first bytes a carrier delivered
// with framing f (for FramingRecords, the start of the first message), can
// begin a first record of this specification: on a byte stream a Size of
// at least 9 and at most MaxRecordSize, as a variable-length integer of 1,
// 2 or 4 bytes, then a first frame type (SPEC.md 6.2; its encoding begins
// with ff, 9a or ba, REGISTRY.md 2.4); in a message, the first frame type
// at once. need is the total number of bytes needed (at most
// MaxFirstBytes) when the result is FirstBytesNeedMore, else 0.
//
// It judges the syntax of a prefix only, not the limits of a receiver (the
// full check of SPEC.md 6.2 is the session's). b beginning 16 03, as TLS
// does, is always FirstBytesOther. It is the one authority for hosts and
// transforms that tell zmux records from other content, at admission
// (Admission.Decide) or when drawing a prefix that must not look like one
// (redrawn until both framings give FirstBytesOther).
func ClassifyFirstBytes(b []byte, f Framing) (c FirstBytes, need int) {
	if len(b) == 0 {
		return FirstBytesNeedMore, 1
	}
	if f == FramingRecords {
		if firstLead(b[0]) {
			return FirstBytesRecord, 0
		}
		return FirstBytesOther, 0
	}
	l := 1 << (b[0] >> 6)
	switch {
	case l == 8: // a Size of at most MaxRecordSize in 8 bytes: not one
		return FirstBytesOther, 0
	case l == 1 && b[0] < 9:
		return FirstBytesOther, 0
	case l == 4 && len(b) >= 2 && b[1] != 0: // above MaxRecordSize
		return FirstBytesOther, 0
	}
	if len(b) < l {
		return FirstBytesNeedMore, l + 1
	}
	size, _ := varint.ConsumeVarint(b)
	if size < 9 || size > MaxRecordSize {
		return FirstBytesOther, 0
	}
	if len(b) == l {
		return FirstBytesNeedMore, l + 1
	}
	if firstLead(b[l]) {
		return FirstBytesRecord, 0
	}
	return FirstBytesOther, 0
}
