package wire

import "github.com/zmuxio/zmux-go/varint"

// Channel datagrams (EXTENSIONS.md 6.8): one DATAGRAM frame sent on a
// carrier's own datagram channel instead of in a record, after the Pipe
// Stream ID on channels shared by the pipes of a connection (QUIC,
// WebTransport) and alone where the channel belongs to the pipe (HTTP/3).

// ChannelDatagramLen is the length of a channel datagram carrying n bytes
// of DATAGRAM data, with the Pipe Stream ID id when withID.
func ChannelDatagramLen(withID bool, id uint64, n int) int {
	l := 1 + n
	if withID {
		l += varint.SizeVarint(id)
	}
	return l
}

// AppendChannelDatagram appends a channel datagram: the Pipe Stream ID id
// when withID, then a DATAGRAM frame of data without its Length field.
func AppendChannelDatagram(b []byte, withID bool, id uint64, data []byte) []byte {
	if withID {
		b = varint.AppendVarint(b, id)
	}
	b = AppendDatagramHeader(b, len(data), false)
	return append(b, data...)
}

// SplitChannelDatagram splits a channel datagram that starts with a Pipe
// Stream ID into that ID and the frame after it; ok is false when the ID
// is truncated.
func SplitChannelDatagram(b []byte) (id uint64, frame []byte, ok bool) {
	id, n := varint.ConsumeVarint(b)
	if n == 0 {
		return 0, nil, false
	}
	return id, b[n:], true
}

// DecodeChannelFrame decodes the frame of a channel datagram, which must be
// exactly one DATAGRAM frame of type 0x30; anything else is a
// PROTOCOL_VIOLATION. f's data aliases b.
func DecodeChannelFrame(b []byte, f *Frame) *Error {
	if len(b) == 0 || b[0] != byte(TypeDatagram) {
		return ErrProto("channel datagram is not one DATAGRAM frame without Length")
	}
	n, err := DecodeFrame(b, f)
	if err != nil {
		return &Error{Code: ProtocolViolation, FrameType: f.Type, Reason: err.Reason}
	}
	if n != len(b) || f.Kind != KindDatagram {
		return ErrProto("channel datagram is not one DATAGRAM frame without Length")
	}
	return nil
}
