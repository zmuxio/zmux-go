package wire

// Preamble TLV types (EXTENSIONS.md 4.3).
const (
	TLVOpenInfo    uint64 = 0x01
	TLVPriority    uint64 = 0x02
	TLVPipeCarrier uint64 = 0x05
	TLVAppFirst    uint64 = 0x1000
	TLVAppLast     uint64 = 0x3fff
)

// IsGreaseTLV reports whether t is a GREASE TLV type (0x1f*N+0x21).
func IsGreaseTLV(t uint64) bool { return t >= 0x21 && (t-0x21)%0x1f == 0 }

// IsGreaseParam reports whether id is a GREASE parameter (31*N+27).
func IsGreaseParam(id uint64) bool { return id >= 27 && (id-27)%31 == 0 }

// IsAppTLV reports whether t is an application TLV type.
func IsAppTLV(t uint64) bool { return t >= TLVAppFirst && t <= TLVAppLast && !IsGreaseTLV(t) }

// TLV is one application TLV.
type TLV struct {
	Type  uint64
	Value []byte
}

// Preamble is a parsed stream preamble; byte fields alias the input.
type Preamble struct {
	Size        int // Length field plus Length
	OpenInfo    []byte
	HasOpenInfo bool
	HasPriority bool
	Urgency     uint8
	Incr        bool
	PipeCarrier bool
	App         []TLV
}

// ParsePreamble parses a preamble from the data of the STREAM frame at
// offset 0 alone (EXTENSIONS.md 4.4 rule 1). Every violation is a
// PROTOCOL_VIOLATION. App TLVs are appended to dst.App's storage.
func ParsePreamble(data []byte, limit uint64, dst *Preamble) *Error {
	app := dst.App[:0]
	*dst = Preamble{}
	l, n := ReadVarint(data)
	if n == 0 {
		return ErrProto("preamble Length incomplete")
	}
	size := uint64(n) + l
	if size > uint64(len(data)) {
		return ErrProto("preamble longer than its frame")
	}
	if size > limit {
		return ErrProto("preamble above the limit")
	}
	dst.Size = int(size)
	r := reader{b: data[n:size]}
	for r.i < len(r.b) {
		t := r.varint()
		v := r.bytes(r.varint())
		if r.bad {
			return ErrProto("truncated preamble TLV")
		}
		switch {
		case t == TLVOpenInfo:
			if dst.HasOpenInfo {
				return ErrProto("second OPEN_INFO")
			}
			dst.HasOpenInfo, dst.OpenInfo = true, v
		case t == TLVPriority:
			if dst.HasPriority {
				return ErrProto("second PRIORITY")
			}
			if len(v) != 1 || v[0]&0x0f != 0 {
				return ErrProto("bad PRIORITY TLV")
			}
			dst.HasPriority, dst.Urgency, dst.Incr = true, v[0]>>5, v[0]&0x10 != 0
		case t == TLVPipeCarrier:
			if dst.PipeCarrier {
				return ErrProto("second PIPE_CARRIER")
			}
			if len(v) != 0 {
				return ErrProto("bad PIPE_CARRIER TLV")
			}
			dst.PipeCarrier = true
		case IsAppTLV(t):
			app = append(app, TLV{t, v})
		}
	}
	dst.App = app
	return nil
}

func appendTLV(b []byte, t uint64, v []byte) []byte {
	b = AppendVarint(b, t)
	b = AppendVarint(b, uint64(len(v)))
	return append(b, v...)
}

// PreambleSpec describes a preamble to encode. TLVs are written in
// increasing type order with the GREASE TLV last (TRACES.md 3.3).
type PreambleSpec struct {
	OpenInfo    []byte
	HasOpenInfo bool
	HasPriority bool
	Urgency     uint8
	Incr        bool
	PipeCarrier bool
	App         []TLV
	Grease      *TLV
}

// BodyLen is the length of the TLVs.
func (s *PreambleSpec) BodyLen() int {
	n := 0
	tl := func(t uint64, l int) { n += VarintLen(t) + VarintLen(uint64(l)) + l }
	if s.HasOpenInfo {
		tl(TLVOpenInfo, len(s.OpenInfo))
	}
	if s.HasPriority {
		tl(TLVPriority, 1)
	}
	if s.PipeCarrier {
		tl(TLVPipeCarrier, 0)
	}
	for _, a := range s.App {
		tl(a.Type, len(a.Value))
	}
	if s.Grease != nil {
		tl(s.Grease.Type, len(s.Grease.Value))
	}
	return n
}

// Size is the encoded preamble size.
func (s *PreambleSpec) Size() int {
	l := s.BodyLen()
	return VarintLen(uint64(l)) + l
}

// AppendPreamble encodes the preamble.
func AppendPreamble(b []byte, s *PreambleSpec) []byte {
	b = AppendVarint(b, uint64(s.BodyLen()))
	if s.HasOpenInfo {
		b = appendTLV(b, TLVOpenInfo, s.OpenInfo)
	}
	if s.HasPriority {
		b = appendTLV(b, TLVPriority, []byte{PriorityByte(s.Urgency, s.Incr)})
	}
	if s.PipeCarrier {
		b = appendTLV(b, TLVPipeCarrier, nil)
	}
	for _, a := range s.App {
		b = appendTLV(b, a.Type, a.Value)
	}
	if s.Grease != nil {
		b = appendTLV(b, s.Grease.Type, s.Grease.Value)
	}
	return b
}
