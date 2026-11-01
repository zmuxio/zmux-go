package seal

import (
	"encoding/binary"
	"errors"
)

// Transport frames. Each zmux record (its Frames, without Size) is one
// frame: Len(2, big-endian, the record's length) ‖ AEAD(record) on a byte
// stream, AEAD(record) alone as one message on a message carrier; the
// associated data is Len in both forms. Nonces are the cipher state's
// implicit counter; each direction rekeys before frame 2^20, 2·2^20, ….
const (
	rekeyInterval = 1 << 20
	frameOverhead = lenLen + tagLen // byte stream
	msgOverhead   = tagLen          // message carrier
	maxRecord     = 65535
)

var errRecordLen = errors.New("seal: record length out of range")

// frameState is one direction's cipher state with its rekey schedule.
type frameState struct {
	cs   cipherState
	next uint64 // the nonce before which the next rekey runs
	ad   [lenLen]byte
}

func newFrameState(cs cipherState) frameState { return frameState{cs: cs, next: rekeyInterval} }

func (f *frameState) schedule() {
	if f.cs.n == f.next {
		f.cs.rekey()
		f.next += rekeyInterval
	}
}

// seal appends the frame of record to dst, with Len when withLen.
func (f *frameState) seal(dst, record []byte, withLen bool) ([]byte, error) {
	if len(record) == 0 || len(record) > maxRecord {
		return nil, errRecordLen
	}
	f.schedule()
	binary.BigEndian.PutUint16(f.ad[:], uint16(len(record)))
	if withLen {
		dst = append(dst, f.ad[:]...)
	}
	return f.cs.seal(dst, f.ad[:], record)
}

// open decrypts ct, the AEAD part of a frame whose record has length n,
// in place, and returns the record.
func (f *frameState) open(ct []byte, n int) ([]byte, error) {
	if n == 0 || n > maxRecord || len(ct) != n+tagLen {
		return nil, errRecordLen
	}
	f.schedule()
	binary.BigEndian.PutUint16(f.ad[:], uint16(n))
	return f.cs.open(ct[:0], f.ad[:], ct)
}
