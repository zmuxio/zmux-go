package wire

// Gates says which frames the receiver accepts in a session (SPEC.md 4.2,
// 5.5): features in use, and unilateral announcements the receiver made.
type Gates struct {
	ResetStreamAt bool   // the receiver advertised reset_stream_at
	MaxDatagram   uint64 // the receiver's max_datagram_frame_size; 0 = none
	GoAway        bool
	Priority      bool
	Multipath     bool
	PipeAck       bool
	Lanes         bool // lanes available toward the receiver
}

// AllGates accepts every frame (with unbounded datagrams).
var AllGates = Gates{ResetStreamAt: true, MaxDatagram: MaxVarint, GoAway: true, Priority: true,
	Multipath: true, PipeAck: true, Lanes: true}

// CheckGate returns the error for a decoded frame whose feature is not in use
// (SPEC.md 4.1 rule 3, RFC 9221 Section 3 for DATAGRAM). recordLeft is the
// number of bytes the frame occupied, for the DATAGRAM size check.
func CheckGate(f *Frame, g *Gates) *Error {
	ok := true
	switch f.Kind {
	case KindResetStreamAt:
		ok = g.ResetStreamAt
	case KindDatagram:
		if g.MaxDatagram == 0 {
			return &Error{Code: ProtocolViolation, FrameType: f.Type, Reason: "DATAGRAM not advertised"}
		}
		// The frame size counts the whole frame (RFC 9221 Section 3).
		size := uint64(1 + len(f.Data))
		if f.HasLen {
			size += uint64(VarintLen(uint64(len(f.Data))))
		}
		if size > g.MaxDatagram {
			return &Error{Code: ProtocolViolation, FrameType: f.Type, Reason: "DATAGRAM larger than advertised"}
		}
	case KindGoAway:
		ok = g.GoAway
	case KindPriorityUpdate:
		ok = g.Priority
	case KindPipeJoin, KindPipePing, KindPipeAbandon, KindPipeStatus:
		ok = g.Multipath
	case KindPipeAck:
		ok = g.Multipath || g.PipeAck
	case KindPipeLane:
		ok = g.Lanes
	}
	if !ok {
		return &Error{Code: FrameEncodingError, FrameType: f.Type, Reason: f.Kind.String() + " not in use"}
	}
	return nil
}
