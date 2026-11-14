package proto

import (
	"encoding/binary"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/varint"
)

// Framing is how handshake messages and sealed records sit on a carrier.
type Framing = join.Framing

const (
	// Stream is a byte stream: units carry a two-byte big-endian length.
	Stream = join.Stream
	// Message is a message carrier: one unit per message, no length.
	Message = join.Message
)

// Dispatch classifies the first byte of a carrier; see join.Class.
var Dispatch = join.Dispatch

// HandleLen is the length of every Handle.
const HandleLen = 16

const (
	lenPrefix = 2
	// maxMessage is what the length prefix and the Noise message limit allow.
	maxMessage = join.MaxMessage
)

// Hello is the first unit of a handshake. Handle and Msg1 alias the parsed
// bytes.
type Hello struct {
	Handle  []byte
	Msg1    []byte
	Profile Profile
	Mode    Mode
}

// AppendHello appends the encoding of h. The profile must be known and the
// handle must suit it.
func AppendHello(dst []byte, f Framing, h Hello) ([]byte, error) {
	pat, ok := profiles[h.Profile]
	if !ok {
		return nil, ErrUnknownProfile
	}
	if h.Mode >= modeReserved {
		return nil, ErrBadFlags
	}
	if err := checkHandle(pat, h.Handle); err != nil {
		return nil, err
	}
	if len(h.Msg1) > maxMessage {
		return nil, ErrTooLarge
	}
	dst = appendWireProfile(dst, h.Profile)
	dst = append(dst, byte(h.Mode))
	dst = append(dst, h.Handle...)
	return AppendFrame(dst, f, h.Msg1)
}

// HelloHeaderLen is the length of a Hello before its Noise message, length prefix
// included.
func HelloHeaderLen(f Framing, p Profile) int {
	n := len(appendWireProfile(nil, p)) + 1
	if pat, ok := profiles[p]; ok && pat.hasHandle {
		n += HandleLen
	}
	return n + FrameHeaderLen(f)
}

// FrameHeaderLen is the length prefix of a unit.
func FrameHeaderLen(f Framing) int {
	if f == Stream {
		return lenPrefix
	}
	return 0
}

func appendWireProfile(dst []byte, p Profile) []byte {
	if p >= profileEscape {
		dst = append(dst, profileEscape)
		return varint.AppendVarint(dst, uint64(p))
	}
	return append(dst, byte(p))
}

func checkHandle(pat *pattern, handle []byte) error {
	if pat.hasHandle && len(handle) != HandleLen || !pat.hasHandle && len(handle) != 0 {
		return ErrBadHandle
	}
	return nil
}

// ParseHello parses the Hello at the start of b and returns the bytes it
// took. With ErrIncomplete, n is the smallest total length that can make
// progress, so a host can refuse an announced length before buffering it.
// On a Message carrier b is the whole unit.
func ParseHello(b []byte, f Framing) (h Hello, n int, err error) {
	short := func(need int) (Hello, int, error) {
		if f == Message {
			return Hello{}, 0, ErrMalformed
		}
		return Hello{}, need, ErrIncomplete
	}
	if len(b) == 0 {
		return short(1)
	}
	first := b[0]
	if first == 0 || first > profileEscape {
		return Hello{}, 0, ErrUnknownProfile
	}
	h.Profile, n = Profile(first), 1
	if first == profileEscape {
		if len(b) < 2 {
			return short(2)
		}
		v, m := varint.ConsumeVarint(b[1:])
		if m == 0 {
			return short(1 + 1<<(b[1]>>6))
		}
		if m != varint.SizeVarint(v) {
			return Hello{}, 0, ErrMalformed
		}
		if v <= profileEscape {
			return Hello{}, 0, ErrUnknownProfile
		}
		h.Profile, n = Profile(v), 1+m
	}
	pat, ok := profiles[h.Profile]
	if !ok {
		return Hello{}, 0, ErrUnknownProfile
	}
	if len(b) < n+1 {
		return short(n + 1)
	}
	flags := b[n]
	n++
	if flags&^flagsMode != 0 || flags&flagsMode == byte(modeReserved) {
		return Hello{}, 0, ErrBadFlags
	}
	h.Mode = Mode(flags)
	if pat.hasHandle {
		if len(b) < n+HandleLen {
			return short(n + HandleLen)
		}
		h.Handle = b[n : n+HandleLen]
		n += HandleLen
	}
	if f == Message {
		if len(b)-n > maxMessage {
			return Hello{}, 0, ErrTooLarge
		}
		h.Msg1 = b[n:]
		return h, len(b), nil
	}
	if len(b) < n+lenPrefix {
		return short(n + lenPrefix)
	}
	l := int(binary.BigEndian.Uint16(b[n:]))
	n += lenPrefix
	if len(b) < n+l {
		return short(n + l)
	}
	h.Msg1 = b[n : n+l]
	return h, n + l, nil
}

// ParseHelloExact is ParseHello for a responder's buffer: an initiator sends
// nothing after a Hello until it has the Reply.
func ParseHelloExact(b []byte, f Framing) (Hello, int, error) {
	h, n, err := ParseHello(b, f)
	if err == nil && n < len(b) {
		return Hello{}, n, ErrTrailing
	}
	return h, n, err
}

// IsHelloFirstByte reports whether b can begin a Hello. No such byte can begin
// a zmux first record, on either framing, so a host can dispatch on it.
func IsHelloFirstByte(b byte) bool { return b >= 1 && b <= profileEscape }

// AppendFrame appends a Reply, a Finish or a sealed record.
func AppendFrame(dst []byte, f Framing, unit []byte) ([]byte, error) {
	if len(unit) > maxMessage {
		return nil, ErrTooLarge
	}
	if f == Stream {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(unit)))
	}
	return append(dst, unit...), nil
}

// CutFrame returns the first unit of b and the bytes it took. With
// ErrIncomplete, n is the smallest total length that can make progress. On a
// Message carrier b is the whole unit.
func CutFrame(b []byte, f Framing) (unit []byte, n int, err error) {
	if f == Message {
		if len(b) > maxMessage {
			return nil, 0, ErrTooLarge
		}
		return b, len(b), nil
	}
	if len(b) < lenPrefix {
		return nil, lenPrefix, ErrIncomplete
	}
	n = lenPrefix + int(binary.BigEndian.Uint16(b))
	if len(b) < n {
		return nil, n, ErrIncomplete
	}
	return b[lenPrefix:n], n, nil
}

// prologue is what both ends must agree on; it is never sent as such.
func prologue(p Profile, m Mode, handle, binding []byte) []byte {
	b := make([]byte, 0, len(prologueLabel)+8+1+len(handle)+len(binding))
	b = append(b, prologueLabel...)
	b = varint.AppendVarint(b, uint64(p))
	b = append(b, byte(m))
	b = append(b, handle...)
	return append(b, binding...)
}

const prologueLabel = "zmux-seal"

func (p Profile) String() string { return fmt.Sprintf("profile %d", uint64(p)) }
