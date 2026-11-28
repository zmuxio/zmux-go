package join

import "github.com/zmuxio/zmux-go/varint"

// ItemSize is the size of a padding item with a value of v bytes.
func ItemSize(v int) int { return 1 + varint.SizeVarint(uint64(v)) + v }

// FitValue is the largest value length whose padding item fits in room; false
// when none does.
func FitValue(room int) (int, bool) {
	if room < 2 {
		return 0, false
	}
	v := room - 2
	for v > 0 && ItemSize(v) > room {
		v--
	}
	return v, true
}

// ItemFor is the value length of the padding item that takes a message gap
// bytes further, within room bytes: the smallest that reaches gap, or the
// largest that fits; false when the message gets no item.
func ItemFor(gap, room int) (int, bool) {
	if gap <= 0 || room < 2 {
		return 0, false
	}
	gap = min(gap, room)
	v := max(gap-2, 0)
	for v > 0 && ItemSize(v-1) >= gap {
		v--
	}
	for ItemSize(v) < gap {
		v++
	}
	if ItemSize(v) > room {
		return FitValue(room)
	}
	return v, true
}

// RawFill is the number of zero bytes that take a Confirm gap bytes further,
// within room bytes.
func RawFill(gap, room int) int { return max(min(gap, room), 0) }

func (p HelloPayload) padItem(b []byte) []byte {
	if p.Padding > 0 || p.PadEmpty {
		b = appendItem(b, tlvPadding, make([]byte, p.Padding))
	}
	return b
}

// HelloPlainLen is the plaintext length of p without padding.
func HelloPlainLen(p HelloPayload) (int, error) {
	p.Padding, p.PadEmpty = 0, false
	b, err := AppendHelloPayload(nil, p)
	return len(b), err
}

// ReplyPlainLen is the plaintext length of p without padding.
func ReplyPlainLen(p ReplyPayload) (int, error) {
	p.Padding, p.PadEmpty = 0, false
	b, err := AppendReplyPayload(nil, p)
	return len(b), err
}

// TagLen is the AEAD tag length of the Suite.
func (s *Suite) TagLen() int { return s.AEAD.TagLen() }

// FramingLen is the length prefix a unit has under f.
func FramingLen(f Framing) int {
	if f == Stream {
		return lenPrefix
	}
	return 0
}

// HelloWire is the length on the wire of a Hello with plain bytes of plaintext.
func (j *Joiner) HelloWire(plain int) int {
	s := j.cfg.Suite
	return 1 + varint.SizeVarint(s.ID) + 1 + 1 + len(j.cfg.Locator) + s.NonceLen + FramingLen(j.cfg.Framing) + plain + s.TagLen()
}

// ReplyWire is the length on the wire of a Reply with plain bytes of plaintext.
func (a *Acceptor) ReplyWire(plain int) int {
	s := a.c.suite
	return 1 + s.NonceLen + FramingLen(a.c.f) + plain + s.TagLen()
}

// ReplyPayloadOffset returns the start of the encrypted payload in a Reply.
func (a *Acceptor) ReplyPayloadOffset() int { return a.ReplyWire(0) - a.c.suite.TagLen() }

// HelloRoom is the bytes of items a Hello with plain bytes of plaintext can
// still take below the handshake message floor (negative: it is over).
func (j *Joiner) HelloRoom(plain int) int { return HandshakeFloor - j.HelloWire(plain) }

// ReplyRoom is the bytes of items a Reply with plain bytes of plaintext can
// still take below the handshake message floor (negative: it is over).
func (a *Acceptor) ReplyRoom(plain int) int { return HandshakeFloor - a.ReplyWire(plain) }

// WireLen is the length on the wire of a parsed Hello.
func (h *Hello) WireLen(f Framing) int { return len(h.clear) + FramingLen(f) + len(h.Ciphertext) }

// ConfirmWire is the length on the wire of the Confirm without zero bytes, and
// the zero bytes it can hold; 0, 0 in Bound.
func (j *Joiner) ConfirmWire() (w, room int) {
	switch j.cfg.Mode {
	case Sealed:
		t := j.cfg.Suite.TagLen()
		return FramingLen(j.cfg.Framing) + t, HandshakeFloor - FramingLen(j.cfg.Framing) - t
	case Trusted:
		h := j.cfg.Suite.Hash.Size()
		return FramingLen(j.cfg.Framing) + h, HandshakeFloor - FramingLen(j.cfg.Framing) - h
	default:
		return 0, 0
	}
}
