package wire

import "github.com/zmuxio/zmux-go/varint"

// The classes of ClassifyFirstBytes.
const (
	FirstNeedMore = iota // more bytes are needed to tell
	FirstRecord          // the bytes can begin a first record
	FirstOther           // the bytes cannot begin a first record
)

// MaxFirstBytes is the most bytes ClassifyFirstBytes needs.
const MaxFirstBytes = 5

// firstLeads are the first bytes of the encodings of the frame types that
// may begin a first record.
var firstLeads = func() (l [len(FirstFrameTypes)]byte) {
	for i, t := range FirstFrameTypes {
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

// ClassifyFirstBytes tells whether b, the first bytes of a carrier (with
// messages set, the start of its first message), can begin a first record:
// on a byte stream a Size of at least 9 and at most MaxRecordSize, as a
// variable-length integer of 1, 2 or 4 bytes, then a first frame type; in
// a message, the first frame type at once. need is the total number of
// bytes needed (at most MaxFirstBytes) for FirstNeedMore, else 0.
func ClassifyFirstBytes(b []byte, messages bool) (class, need int) {
	if len(b) == 0 {
		return FirstNeedMore, 1
	}
	if messages {
		if firstLead(b[0]) {
			return FirstRecord, 0
		}
		return FirstOther, 0
	}
	l := 1 << (b[0] >> 6)
	switch {
	case l == 8: // a Size of at most MaxRecordSize in 8 bytes: not one
		return FirstOther, 0
	case l == 1 && b[0] < 9:
		return FirstOther, 0
	case l == 4 && len(b) >= 2 && b[1] != 0: // above MaxRecordSize
		return FirstOther, 0
	}
	if len(b) < l {
		return FirstNeedMore, l + 1
	}
	size, _ := varint.ConsumeVarint(b)
	if size < 9 || size > MaxRecordSize {
		return FirstOther, 0
	}
	if len(b) == l {
		return FirstNeedMore, l + 1
	}
	if firstLead(b[l]) {
		return FirstRecord, 0
	}
	return FirstOther, 0
}

// FirstRecordIsLane tells whether b, the first bytes of a carrier (with
// messages set, of its first message), begin a lane's first record. need
// is the total number of bytes needed when more are wanted, else 0.
func FirstRecordIsLane(b []byte, messages bool) (lane bool, need int) {
	lead := byte(0xba)
	if messages {
		return len(b) > 0 && b[0] == lead, 0
	}
	if len(b) == 0 {
		return false, 1
	}
	size, n := varint.ConsumeVarint(b)
	if n == 0 {
		return false, 1 << (b[0] >> 6)
	}
	if len(b) <= n {
		return false, n + 1
	}
	return size >= 9 && size <= MaxRecordSize && b[n] == lead, 0
}
