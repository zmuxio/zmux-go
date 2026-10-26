package wire

import "bytes"

// Transport parameter identifiers (REGISTRY.md Section 4).
const (
	ParamMaxIdleTimeout       uint64 = 0x01
	ParamInitialMaxData       uint64 = 0x04
	ParamMaxStreamDataBidiLoc uint64 = 0x05
	ParamMaxStreamDataBidiRem uint64 = 0x06
	ParamMaxStreamDataUni     uint64 = 0x07
	ParamMaxStreamsBidi       uint64 = 0x08
	ParamMaxStreamsUni        uint64 = 0x09
	ParamResetStreamAt        uint64 = 0x1d
	ParamMaxDatagramFrameSize uint64 = 0x20
	ParamMaxRecordSize        uint64 = 0x0571c59429cd0845
	ParamRole                 uint64 = 0x17240207
	ParamGoAway               uint64 = 0x04528a5f
	ParamPreamble             uint64 = 0x0439e07a
	ParamPriority             uint64 = 0x31e7139d
	ParamDatagramFlows        uint64 = 0x0af391ed
	ParamMultipath            uint64 = 0x30dc2978
	ParamAckThreshold         uint64 = 0x2762c7e5
	ParamPipeAck              uint64 = 0x34b47621
	ParamMaxAckDelay          uint64 = 0x17fa1ed6
	ParamMaxLanes             uint64 = 0x27856ef0
	ParamResume               uint64 = 0x0d5778ce
	ParamAppProtocols         uint64 = 0x3d4f9c2a8b1e6075 // the qmux crate's hook
)

// DefaultMaxRecordSize is the limit when max_record_size is absent.
const DefaultMaxRecordSize = 16382

// Secret flags of zmux_multipath (MULTIPATH.md 3.1).
const (
	SecretExporter = 0x01
	SecretPSK      = 0x04
	SecretInsecure = 0x08
)

// Roles of zmux_role.
const (
	RoleInitiator = 0
	RoleResponder = 1
	RoleAuto      = 2
)

// Has bits for Params.Has.
const (
	HasIdle uint32 = 1 << iota
	HasMaxData
	HasBidiLocal
	HasBidiRemote
	HasUni
	HasStreamsBidi
	HasStreamsUni
	HasResetAt
	HasDatagram
	HasRecordSize
	HasRole
	HasGoAway
	HasPreamble
	HasPriority
	HasFlows
	HasMultipath
	HasAckThreshold
	HasPipeAck
	HasAckDelay
	HasLanes
	HasResume
	HasAppProtocols
)

// HasAnyZmux are the zmux parameters whose presence identifies a zmux peer.
const HasAnyZmux = HasRole | HasGoAway | HasPreamble | HasPriority | HasFlows | HasMultipath |
	HasAckThreshold | HasPipeAck | HasAckDelay | HasLanes | HasResume

// Params is a set of transport parameters.
type Params struct {
	Has                  uint32
	MaxIdleTimeout       uint64 // ms
	InitialMaxData       uint64
	MaxStreamDataBidiLoc uint64
	MaxStreamDataBidiRem uint64
	MaxStreamDataUni     uint64
	MaxStreamsBidi       uint64
	MaxStreamsUni        uint64
	MaxDatagramFrameSize uint64
	MaxRecordSize        uint64 // DefaultMaxRecordSize when absent
	Role                 uint64
	RoleNonce            uint64
	Preamble             uint64
	SecretFlags          uint8
	SessionNonce         [16]byte
	Locator              []byte
	AckThreshold         uint64
	MaxAckDelay          uint64 // ms
	MaxLanes             uint64
	LaneKey              [16]byte
	ResumeGrace          uint64 // ms
	AppProtocols         [][]byte

	// GREASE parameters to send (encoding only).
	GreaseID    uint64
	GreaseValue []byte
	HasGrease   bool
}

// IsZmux reports whether the parameters identify a zmux endpoint.
func (p *Params) IsZmux() bool { return p.Has&HasAnyZmux != 0 }

func isProhibited(id uint64) bool {
	return id == 0x00 || id == 0x02 || id == 0x03 || (id >= 0x0a && id <= 0x10)
}

var paramBits = map[uint64]uint32{
	ParamMaxIdleTimeout: HasIdle, ParamInitialMaxData: HasMaxData, ParamMaxStreamDataBidiLoc: HasBidiLocal,
	ParamMaxStreamDataBidiRem: HasBidiRemote, ParamMaxStreamDataUni: HasUni, ParamMaxStreamsBidi: HasStreamsBidi,
	ParamMaxStreamsUni: HasStreamsUni, ParamResetStreamAt: HasResetAt, ParamMaxDatagramFrameSize: HasDatagram,
	ParamMaxRecordSize: HasRecordSize, ParamRole: HasRole, ParamGoAway: HasGoAway, ParamPreamble: HasPreamble,
	ParamPriority: HasPriority, ParamDatagramFlows: HasFlows, ParamMultipath: HasMultipath,
	ParamAckThreshold: HasAckThreshold, ParamPipeAck: HasPipeAck, ParamMaxAckDelay: HasAckDelay,
	ParamMaxLanes: HasLanes, ParamResume: HasResume, ParamAppProtocols: HasAppProtocols,
}

// DecodeParams decodes and validates a parameter block (SPEC.md 5.2-5.5,
// MULTIPATH.md 3, RESUME.md 3, EXTENSIONS.md 2, 4.1). Unknown parameters are
// ignored. It allocates only for the locator and application protocols.
func DecodeParams(b []byte, p *Params) *Error {
	*p = Params{MaxRecordSize: DefaultMaxRecordSize}
	// Duplicate detection: known parameters use the Has bits; unknown ones
	// are compared against the IDs seen before (bounded by the block size).
	r := reader{b: b}
	var seenUnknown []uint64
	for r.i < len(b) {
		id := r.varint()
		v := r.bytes(r.varint())
		if r.bad {
			return ErrParam("truncated parameter block")
		}
		if isProhibited(id) {
			return ErrParam("prohibited QUIC parameter")
		}
		bit, known := paramBits[id]
		if known {
			if p.Has&bit != 0 {
				return ErrParam("duplicate parameter")
			}
			p.Has |= bit
		} else {
			for _, s := range seenUnknown {
				if s == id {
					return ErrParam("duplicate parameter")
				}
			}
			seenUnknown = append(seenUnknown, id)
			continue
		}
		if err := p.decodeOne(id, v); err != nil {
			return err
		}
	}
	return p.validate()
}

func varintValue(v []byte) (uint64, *Error) {
	x, n := ReadVarint(v)
	if n == 0 || n != len(v) {
		return 0, ErrParam("malformed varint parameter")
	}
	return x, nil
}

func (p *Params) decodeOne(id uint64, v []byte) *Error {
	var err *Error
	switch id {
	case ParamResetStreamAt, ParamGoAway, ParamPriority, ParamDatagramFlows, ParamPipeAck:
		if len(v) != 0 {
			return ErrParam("non-empty flag parameter")
		}
	case ParamMaxIdleTimeout:
		p.MaxIdleTimeout, err = varintValue(v)
	case ParamInitialMaxData:
		p.InitialMaxData, err = varintValue(v)
	case ParamMaxStreamDataBidiLoc:
		p.MaxStreamDataBidiLoc, err = varintValue(v)
	case ParamMaxStreamDataBidiRem:
		p.MaxStreamDataBidiRem, err = varintValue(v)
	case ParamMaxStreamDataUni:
		p.MaxStreamDataUni, err = varintValue(v)
	case ParamMaxStreamsBidi:
		p.MaxStreamsBidi, err = varintValue(v)
		if err == nil && p.MaxStreamsBidi > max60 {
			err = ErrParam("initial_max_streams_bidi above 2^60")
		}
	case ParamMaxStreamsUni:
		p.MaxStreamsUni, err = varintValue(v)
		if err == nil && p.MaxStreamsUni > max60 {
			err = ErrParam("initial_max_streams_uni above 2^60")
		}
	case ParamMaxDatagramFrameSize:
		p.MaxDatagramFrameSize, err = varintValue(v)
	case ParamMaxRecordSize:
		p.MaxRecordSize, err = varintValue(v)
		if err == nil && p.MaxRecordSize < DefaultMaxRecordSize {
			err = ErrParam("max_record_size below 16382")
		}
	case ParamRole:
		x, n := ReadVarint(v)
		if n == 0 || x > 2 {
			return ErrParam("zmux_role")
		}
		p.Role = x
		rest := v[n:]
		if x == RoleAuto {
			if len(rest) != 8 {
				return ErrParam("zmux_role nonce")
			}
			for _, c := range rest {
				p.RoleNonce = p.RoleNonce<<8 | uint64(c)
			}
		} else if len(rest) != 0 {
			return ErrParam("zmux_role extra bytes")
		}
	case ParamPreamble:
		p.Preamble, err = varintValue(v)
	case ParamMultipath:
		if len(v) < 18 {
			return ErrParam("zmux_multipath too short")
		}
		p.SecretFlags = v[0]
		copy(p.SessionNonce[:], v[1:17])
		ll := int(v[17])
		if ll > 16 || len(v) != 18+ll {
			return ErrParam("zmux_multipath locator")
		}
		p.Locator = bytes.Clone(v[18:])
	case ParamAckThreshold:
		p.AckThreshold, err = varintValue(v)
		if err == nil && p.AckThreshold < 1024 {
			err = ErrParam("zmux_ack_threshold below 1024")
		}
	case ParamMaxAckDelay:
		p.MaxAckDelay, err = varintValue(v)
		if err == nil && p.MaxAckDelay >= 1<<14 {
			err = ErrParam("zmux_max_ack_delay out of range")
		}
	case ParamMaxLanes:
		x, n := ReadVarint(v)
		if n == 0 || x == 0 || len(v)-n != 16 {
			return ErrParam("zmux_max_lanes")
		}
		p.MaxLanes = x
		copy(p.LaneKey[:], v[n:])
	case ParamResume:
		p.ResumeGrace, err = varintValue(v)
		if err == nil && p.ResumeGrace == 0 {
			err = ErrParam("zmux_resume grace 0")
		}
	case ParamAppProtocols:
		for len(v) > 0 {
			l := int(v[0])
			if 1+l > len(v) {
				return ErrParam("application_protocols")
			}
			p.AppProtocols = append(p.AppProtocols, bytes.Clone(v[1:1+l]))
			v = v[1+l:]
		}
	}
	return err
}

func (p *Params) validate() *Error {
	ackish := p.Has&(HasMultipath|HasPipeAck) != 0
	if p.Has&(HasAckThreshold|HasAckDelay|HasLanes) != 0 && !ackish {
		return ErrParam("companion parameter without zmux_multipath or zmux_pipe_ack")
	}
	if p.Has&HasResume != 0 && p.Has&HasMultipath == 0 {
		return ErrParam("zmux_resume without zmux_multipath")
	}
	if p.Has&HasPreamble != 0 {
		if p.Has&HasResetAt == 0 {
			return ErrParam("zmux_preamble without reset_stream_at")
		}
		if p.Preamble < 1 || p.Preamble > p.MaxRecordSize-32 ||
			p.Preamble > p.MaxStreamDataBidiRem || p.Preamble > p.MaxStreamDataUni {
			return ErrParam("zmux_preamble out of range")
		}
	}
	return nil
}

func appendParam(b []byte, id uint64, v []byte) []byte {
	b = AppendVarint(b, id)
	b = AppendVarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendVarintParam(b []byte, id, v uint64) []byte {
	b = AppendVarint(b, id)
	b = AppendVarint(b, uint64(VarintLen(v)))
	return AppendVarint(b, v)
}

// AppendParams encodes the parameters present in p.Has.
func AppendParams(b []byte, p *Params) []byte {
	h := p.Has
	if h&HasIdle != 0 {
		b = appendVarintParam(b, ParamMaxIdleTimeout, p.MaxIdleTimeout)
	}
	if h&HasMaxData != 0 {
		b = appendVarintParam(b, ParamInitialMaxData, p.InitialMaxData)
	}
	if h&HasBidiLocal != 0 {
		b = appendVarintParam(b, ParamMaxStreamDataBidiLoc, p.MaxStreamDataBidiLoc)
	}
	if h&HasBidiRemote != 0 {
		b = appendVarintParam(b, ParamMaxStreamDataBidiRem, p.MaxStreamDataBidiRem)
	}
	if h&HasUni != 0 {
		b = appendVarintParam(b, ParamMaxStreamDataUni, p.MaxStreamDataUni)
	}
	if h&HasStreamsBidi != 0 {
		b = appendVarintParam(b, ParamMaxStreamsBidi, p.MaxStreamsBidi)
	}
	if h&HasStreamsUni != 0 {
		b = appendVarintParam(b, ParamMaxStreamsUni, p.MaxStreamsUni)
	}
	if h&HasResetAt != 0 {
		b = appendParam(b, ParamResetStreamAt, nil)
	}
	if h&HasDatagram != 0 {
		b = appendVarintParam(b, ParamMaxDatagramFrameSize, p.MaxDatagramFrameSize)
	}
	if h&HasRecordSize != 0 {
		b = appendVarintParam(b, ParamMaxRecordSize, p.MaxRecordSize)
	}
	if h&HasRole != 0 {
		v := AppendVarint(nil, p.Role)
		if p.Role == RoleAuto {
			for i := 7; i >= 0; i-- {
				v = append(v, byte(p.RoleNonce>>(8*i)))
			}
		}
		b = appendParam(b, ParamRole, v)
	}
	if h&HasGoAway != 0 {
		b = appendParam(b, ParamGoAway, nil)
	}
	if h&HasPreamble != 0 {
		b = appendVarintParam(b, ParamPreamble, p.Preamble)
	}
	if h&HasPriority != 0 {
		b = appendParam(b, ParamPriority, nil)
	}
	if h&HasFlows != 0 {
		b = appendParam(b, ParamDatagramFlows, nil)
	}
	if h&HasMultipath != 0 {
		v := make([]byte, 0, 18+len(p.Locator))
		v = append(v, p.SecretFlags)
		v = append(v, p.SessionNonce[:]...)
		v = append(v, byte(len(p.Locator)))
		v = append(v, p.Locator...)
		b = appendParam(b, ParamMultipath, v)
	}
	if h&HasAckThreshold != 0 {
		b = appendVarintParam(b, ParamAckThreshold, p.AckThreshold)
	}
	if h&HasPipeAck != 0 {
		b = appendParam(b, ParamPipeAck, nil)
	}
	if h&HasAckDelay != 0 {
		b = appendVarintParam(b, ParamMaxAckDelay, p.MaxAckDelay)
	}
	if h&HasLanes != 0 {
		v := AppendVarint(nil, p.MaxLanes)
		v = append(v, p.LaneKey[:]...)
		b = appendParam(b, ParamMaxLanes, v)
	}
	if h&HasResume != 0 {
		b = appendVarintParam(b, ParamResume, p.ResumeGrace)
	}
	if h&HasAppProtocols != 0 {
		var v []byte
		for _, a := range p.AppProtocols {
			v = append(v, byte(len(a)))
			v = append(v, a...)
		}
		b = appendParam(b, ParamAppProtocols, v)
	}
	if p.HasGrease {
		b = appendParam(b, p.GreaseID, p.GreaseValue)
	}
	return b
}
