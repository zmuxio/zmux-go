package join

// Framing is how handshake messages and sealed records sit on a carrier.
type Framing uint8

const (
	// Stream is a byte stream: units carry a two-byte big-endian length.
	Stream Framing = iota
	// Message is a message carrier: one unit per message, no length.
	Message
)

// Class is what the first byte of a carrier tells.
type Class uint8

const (
	// ClassIncomplete means no byte has arrived on a byte stream.
	ClassIncomplete Class = iota
	// ClassEstablish is a Hello of an Establish profile, or the escape.
	ClassEstablish
	// ClassJoin is a Join Hello.
	ClassJoin
	// ClassReserved is a Profile value without a definition here.
	ClassReserved
	// ClassNotHello is no Hello: the zmux first-record test decides the rest.
	ClassNotHello
)

// ProfileJoin is the first byte of a Join Hello.
const ProfileJoin = 2

// Dispatch classifies the first byte of a carrier.
func Dispatch(b []byte, f Framing) Class {
	if len(b) == 0 {
		if f == Stream {
			return ClassIncomplete
		}
		return ClassNotHello
	}
	switch b[0] {
	case 1, 3, 4, 5, 8:
		return ClassEstablish
	case ProfileJoin:
		return ClassJoin
	case 6, 7:
		return ClassReserved
	}
	return ClassNotHello
}

// ReplyType is the first byte of a Join Reply.
const ReplyType = 0x03
