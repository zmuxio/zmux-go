package admit

import (
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// FirstBytes is what the first bytes of a carrier tell of its content
// (ClassifyFirstBytes).
type FirstBytes uint8

const (
	// FirstBytesNeedMore means that more bytes are needed to tell.
	FirstBytesNeedMore FirstBytes = wire.FirstNeedMore
	// FirstBytesRecord means that the bytes can begin a first record of
	// this specification.
	FirstBytesRecord FirstBytes = wire.FirstRecord
	// FirstBytesOther means that the bytes cannot begin a first record.
	FirstBytesOther FirstBytes = wire.FirstOther
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
const MaxFirstBytes = wire.MaxFirstBytes

// ClassifyFirstBytes tells whether b, the first bytes a carrier delivered
// with framing f (for zmux.FramingRecords, the start of the first message), can
// begin a first record of this specification: on a byte stream a Size of
// at least 9 and at most zmux.MaxRecordSize, as a variable-length integer of 1,
// 2 or 4 bytes, then a first frame type (its encoding begins with ff or
// ba); in a message, the first frame type
// at once. need is the total number of bytes needed (at most
// MaxFirstBytes) when the result is FirstBytesNeedMore, else 0.
//
// It judges the syntax of a prefix only, not the limits of a receiver (the
// full check of a first record is the session's). b beginning 16 03, as TLS
// does, is always FirstBytesOther. It is the one authority for hosts and
// transforms that tell zmux records from other content, at admission
// (zmux.Admission.Decide) or when choosing a prefix (redrawn until both framings
// give FirstBytesOther).
func ClassifyFirstBytes(b []byte, f zmux.Framing) (c FirstBytes, need int) {
	class, need := wire.ClassifyFirstBytes(b, f == zmux.FramingRecords)
	return FirstBytes(class), need
}

// FirstRecordIsLane tells whether b, the first bytes of a carrier with framing f, begin
// a lane's first record. A transform that handles only its own
// handshakes passes such a carrier on: the session then checks its Lane Key, connection
// and record mode. need is the total number of bytes needed when more are wanted, else 0.
func FirstRecordIsLane(b []byte, f zmux.Framing) (lane bool, need int) {
	return wire.FirstRecordIsLane(b, f == zmux.FramingRecords)
}
