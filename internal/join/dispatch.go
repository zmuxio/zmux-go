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

// IsHello reports whether a carrier's first bytes begin a Join Hello: the
// one test of it, for every classifier of a first record.
func IsHello(b []byte) bool { return len(b) > 0 && b[0] == ProfileJoin }

// ReplyType is the first byte of a Join Reply.
const ReplyType = 0x03
