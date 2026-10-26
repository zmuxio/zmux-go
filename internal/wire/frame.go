package wire

// Frame type codepoints (REGISTRY.md Section 3).
const (
	TypePadding           uint64 = 0x00
	TypeResetStream       uint64 = 0x04
	TypeStopSending       uint64 = 0x05
	TypeStream            uint64 = 0x08 // 0x08-0x0f
	TypeMaxData           uint64 = 0x10
	TypeMaxStreamData     uint64 = 0x11
	TypeMaxStreamsBidi    uint64 = 0x12
	TypeMaxStreamsUni     uint64 = 0x13
	TypeDataBlocked       uint64 = 0x14
	TypeStreamDataBlocked uint64 = 0x15
	TypeStreamsBlockedBi  uint64 = 0x16
	TypeStreamsBlockedUni uint64 = 0x17
	TypeCloseTransport    uint64 = 0x1c
	TypeCloseApp          uint64 = 0x1d
	TypeResetStreamAt     uint64 = 0x24
	TypeDatagram          uint64 = 0x30
	TypeDatagramLen       uint64 = 0x31
	TypeTransportParams   uint64 = 0x3f5153300d0a0d0a
	TypePingRequest       uint64 = 0x348c67529ef8c7bd
	TypePingResponse      uint64 = 0x348c67529ef8c7be
	TypeGoAway            uint64 = 0x1911f43e
	TypePriorityUpdate    uint64 = 0x073d08cf
	TypePipeJoin          uint64 = 0x1a24de36
	TypePipeAck           uint64 = 0x0b2b3eeb
	TypePipeLane          uint64 = 0x3a05a1c9
	TypePipePing          uint64 = 0x269b43e6
	TypePipeAbandon       uint64 = 0x23b35b74
	TypePipeStatus        uint64 = 0x227b2811
)

// STREAM frame type bits.
const (
	StreamBitFin = 0x01
	StreamBitLen = 0x02
	StreamBitOff = 0x04
)

// Kind classifies a decoded frame.
type Kind uint8

const (
	KindPadding Kind = iota
	KindStream
	KindResetStream
	KindResetStreamAt
	KindStopSending
	KindMaxData
	KindMaxStreamData
	KindMaxStreams
	KindDataBlocked
	KindStreamDataBlocked
	KindStreamsBlocked
	KindClose
	KindDatagram
	KindTransportParams
	KindPing
	KindGoAway
	KindPriorityUpdate
	KindPipeJoin
	KindPipeAck
	KindPipeLane
	KindPipePing
	KindPipeAbandon
	KindPipeStatus
)

var kindNames = [...]string{"PADDING", "STREAM", "RESET_STREAM", "RESET_STREAM_AT", "STOP_SENDING",
	"MAX_DATA", "MAX_STREAM_DATA", "MAX_STREAMS", "DATA_BLOCKED", "STREAM_DATA_BLOCKED",
	"STREAMS_BLOCKED", "CONNECTION_CLOSE", "DATAGRAM", "QX_TRANSPORT_PARAMETERS", "QX_PING",
	"GOAWAY", "PRIORITY_UPDATE", "PIPE_JOIN", "PIPE_ACK", "PIPE_LANE", "PIPE_PING",
	"PIPE_ABANDON", "PIPE_STATUS"}

func (k Kind) String() string { return kindNames[k] }

// AckEliciting reports whether a frame of this kind elicits a PIPE_ACK.
func (k Kind) AckEliciting() bool {
	return k != KindPadding && k != KindPipeAck && k != KindClose
}

// Join is a decoded PIPE_JOIN frame (MULTIPATH.md Section 5.1).
type Join struct {
	Locator      []byte
	PipeID       uint64
	Dir          uint8 // 0 up, 1 down, 2 bidi
	CarrierBound bool
	Standby      bool
	FateGroup    uint64
	Priority     uint64
	Token        []byte // 16 bytes
	// MACLen is the number of bytes from Type through Priority (the MAC input).
	MACLen int
}

// Frame is one decoded frame. Byte fields alias the decoded buffer.
type Frame struct {
	Kind      Kind
	Type      uint64 // wire frame type
	TypeLen   int    // bytes of the type field
	StreamID  uint64
	Offset    uint64
	Data      []byte // STREAM, DATAGRAM data; CONNECTION_CLOSE reason; QX_TP block
	Fin       bool
	HasOff    bool
	HasLen    bool
	Code      uint64
	FinalSize uint64
	Reliable  uint64
	Value     uint64 // MAX_*, *_BLOCKED limit, QX_PING seq, PRIORITY_UPDATE seq
	Bidi      bool
	App       bool
	ErrFrame  uint64 // CONNECTION_CLOSE frame type
	Response  bool
	MaxBidi   uint64
	MaxUni    uint64
	Urgency   uint8
	Incr      bool
	PipeID    uint64
	Lane      uint64
	LaneKey   []byte
	Seq       uint64
	Status    uint64
	Priority  uint64
	Join      Join
	Count     int    // PADDING run length, PIPE_ACK entry count
	Entries   []byte // PIPE_ACK entries, validated
}

// AckEntry is one PIPE_ACK entry.
type AckEntry struct{ PipeID, Lane, Position, Delay uint64 }

// AckIter iterates validated PIPE_ACK entries without allocating.
type AckIter struct{ b []byte }

// Iter returns an iterator over the frame's PIPE_ACK entries.
func (f *Frame) Iter() AckIter { return AckIter{f.Entries} }

// Next returns the next entry.
func (it *AckIter) Next() (AckEntry, bool) {
	var e AckEntry
	if len(it.b) == 0 {
		return e, false
	}
	var n int
	e.PipeID, n = ReadVarint(it.b)
	it.b = it.b[n:]
	e.Lane, n = ReadVarint(it.b)
	it.b = it.b[n:]
	e.Position, n = ReadVarint(it.b)
	it.b = it.b[n:]
	e.Delay, n = ReadVarint(it.b)
	it.b = it.b[n:]
	return e, true
}

type reader struct {
	b   []byte
	i   int
	bad bool
}

func (r *reader) varint() uint64 {
	if r.bad {
		return 0
	}
	v, n := ReadVarint(r.b[r.i:])
	if n == 0 {
		r.bad = true
		return 0
	}
	r.i += n
	return v
}

func (r *reader) byte() byte {
	if r.bad || r.i >= len(r.b) {
		r.bad = true
		return 0
	}
	c := r.b[r.i]
	r.i++
	return c
}

func (r *reader) bytes(n uint64) []byte {
	if r.bad || n > uint64(len(r.b)-r.i) {
		r.bad = true
		return nil
	}
	s := r.b[r.i : r.i+int(n)]
	r.i += int(n)
	return s
}

const max60 = 1 << 60

// DecodeFrame decodes the frame at the start of b, which holds the rest of
// the record. It returns the frame length. A non-minimal frame type is
// reported as PROTOCOL_VIOLATION only after the frame decoded completely
// (SPEC.md Section 10.3 precedence).
func DecodeFrame(b []byte, f *Frame) (int, *Error) {
	*f = Frame{}
	t, tn := ReadVarint(b)
	if tn == 0 {
		return 0, ErrFrame("truncated frame type")
	}
	f.Type, f.TypeLen = t, tn
	r := reader{b: b, i: tn}
	switch {
	case t == TypePadding:
		f.Kind = KindPadding
		n := 1
		for n < len(b) && b[n] == 0 {
			n++
		}
		f.Count = n
		r.i = n
	case t >= 0x08 && t <= 0x0f:
		f.Kind = KindStream
		f.StreamID = r.varint()
		if t&StreamBitOff != 0 {
			f.HasOff = true
			f.Offset = r.varint()
		}
		f.Fin = t&StreamBitFin != 0
		if t&StreamBitLen != 0 {
			f.HasLen = true
			f.Data = r.bytes(r.varint())
		} else if !r.bad {
			f.Data = b[r.i:]
			r.i = len(b)
		}
		if !r.bad && f.Offset+uint64(len(f.Data)) > MaxVarint {
			return 0, ErrFrame("stream offset plus length above 2^62-1")
		}
	case t == TypeResetStream:
		f.Kind = KindResetStream
		f.StreamID, f.Code, f.FinalSize = r.varint(), r.varint(), r.varint()
	case t == TypeResetStreamAt:
		f.Kind = KindResetStreamAt
		f.StreamID, f.Code, f.FinalSize, f.Reliable = r.varint(), r.varint(), r.varint(), r.varint()
		if !r.bad && f.Reliable > f.FinalSize {
			return 0, ErrFrame("reliable size above final size")
		}
	case t == TypeStopSending:
		f.Kind = KindStopSending
		f.StreamID, f.Code = r.varint(), r.varint()
	case t == TypeMaxData:
		f.Kind = KindMaxData
		f.Value = r.varint()
	case t == TypeMaxStreamData:
		f.Kind = KindMaxStreamData
		f.StreamID, f.Value = r.varint(), r.varint()
	case t == TypeMaxStreamsBidi || t == TypeMaxStreamsUni:
		f.Kind = KindMaxStreams
		f.Bidi = t == TypeMaxStreamsBidi
		f.Value = r.varint()
		if !r.bad && f.Value > max60 {
			return 0, ErrFrame("MAX_STREAMS above 2^60")
		}
	case t == TypeDataBlocked:
		f.Kind = KindDataBlocked
		f.Value = r.varint()
	case t == TypeStreamDataBlocked:
		f.Kind = KindStreamDataBlocked
		f.StreamID, f.Value = r.varint(), r.varint()
	case t == TypeStreamsBlockedBi || t == TypeStreamsBlockedUni:
		f.Kind = KindStreamsBlocked
		f.Bidi = t == TypeStreamsBlockedBi
		f.Value = r.varint()
		if !r.bad && f.Value > max60 {
			return 0, ErrFrame("STREAMS_BLOCKED above 2^60")
		}
	case t == TypeCloseTransport || t == TypeCloseApp:
		f.Kind = KindClose
		f.App = t == TypeCloseApp
		f.Code = r.varint()
		if !f.App {
			f.ErrFrame = r.varint()
		}
		f.Data = r.bytes(r.varint())
	case t == TypeDatagram || t == TypeDatagramLen:
		f.Kind = KindDatagram
		if t == TypeDatagramLen {
			f.HasLen = true
			f.Data = r.bytes(r.varint())
		} else {
			f.Data = b[r.i:]
			r.i = len(b)
		}
	case t == TypeTransportParams:
		f.Kind = KindTransportParams
		f.Data = r.bytes(r.varint())
	case t == TypePingRequest || t == TypePingResponse:
		f.Kind = KindPing
		f.Response = t == TypePingResponse
		f.Value = r.varint()
	case t == TypeGoAway:
		f.Kind = KindGoAway
		f.MaxBidi, f.MaxUni, f.Code = r.varint(), r.varint(), r.varint()
		if !r.bad && (f.MaxBidi > max60 || f.MaxUni > max60) {
			return 0, ErrFrame("GOAWAY watermark above 2^60")
		}
	case t == TypePriorityUpdate:
		f.Kind = KindPriorityUpdate
		f.StreamID, f.Seq = r.varint(), r.varint()
		p := r.byte()
		if !r.bad {
			if p&0x0f != 0 {
				return 0, ErrFrame("PRIORITY_UPDATE reserved bits")
			}
			f.Urgency, f.Incr = p>>5, p&0x10 != 0
		}
	case t == TypePipeJoin:
		f.Kind = KindPipeJoin
		if err := decodeJoin(&r, &f.Join); err != nil {
			return 0, err
		}
	case t == TypePipeAck:
		f.Kind = KindPipeAck
		cnt := r.varint()
		if !r.bad && cnt == 0 {
			return 0, ErrFrame("PIPE_ACK without entries")
		}
		start := r.i
		var pp, pl uint64
		for i := uint64(0); i < cnt && !r.bad; i++ {
			p, l := r.varint(), r.varint()
			r.varint()
			r.varint()
			if !r.bad && i > 0 && (p < pp || p == pp && l <= pl) {
				return 0, ErrFrame("PIPE_ACK entries not increasing")
			}
			pp, pl = p, l
		}
		if !r.bad {
			f.Count = int(cnt)
			f.Entries = b[start:r.i]
		}
	case t == TypePipeLane:
		f.Kind = KindPipeLane
		f.LaneKey = r.bytes(16)
		f.PipeID, f.Lane = r.varint(), r.varint()
	case t == TypePipePing:
		f.Kind = KindPipePing
	case t == TypePipeAbandon:
		f.Kind = KindPipeAbandon
		f.PipeID, f.Code = r.varint(), r.varint()
	case t == TypePipeStatus:
		f.Kind = KindPipeStatus
		f.PipeID, f.Seq, f.Status, f.Priority = r.varint(), r.varint(), r.varint(), r.varint()
		if !r.bad && f.Status > 1 {
			return 0, ErrFrame("PIPE_STATUS status")
		}
	default:
		return 0, &Error{Code: FrameEncodingError, FrameType: t, Reason: "unknown or prohibited frame type"}
	}
	if r.bad {
		return 0, &Error{Code: FrameEncodingError, FrameType: t, Reason: "truncated " + f.Kind.String()}
	}
	if tn != VarintLen(t) {
		return r.i, &Error{Code: ProtocolViolation, FrameType: t, Reason: "frame type not minimally encoded"}
	}
	return r.i, nil
}

func decodeJoin(r *reader, j *Join) *Error {
	ll := r.byte()
	if !r.bad && (ll == 0 || ll > 16) {
		return ErrFrame("PIPE_JOIN locator length")
	}
	j.Locator = r.bytes(uint64(ll))
	j.PipeID = r.varint()
	fl := r.byte()
	if !r.bad {
		if fl&0xf0 != 0 || fl&0x03 == 3 {
			return ErrFrame("PIPE_JOIN flags")
		}
		j.Dir, j.CarrierBound, j.Standby = fl&3, fl&0x04 != 0, fl&0x08 != 0
	}
	j.FateGroup = r.varint()
	j.Priority = r.varint()
	j.MACLen = r.i
	j.Token = r.bytes(16)
	if !r.bad && r.i > 64 {
		return ErrFrame("PIPE_JOIN longer than 64 bytes")
	}
	return nil
}
