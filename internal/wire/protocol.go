package wire

import "fmt"

const (
	Magic                          = "ZMUX"
	PrefaceVersion          byte   = 1
	ProtoVersion            uint64 = 1
	MaxPrefaceSettingsBytes        = 4096
	MaxVarint62             uint64 = (1 << 62) - 1
)

type Role byte

const (
	RoleInitiator Role = 0
	RoleResponder Role = 1
	RoleAuto      Role = 2
)

func (r Role) String() string {
	switch r {
	case RoleInitiator:
		return "initiator"
	case RoleResponder:
		return "responder"
	case RoleAuto:
		return "auto"
	default:
		return fmt.Sprintf("role(%d)", byte(r))
	}
}

func (r Role) Valid() bool {
	return r == RoleInitiator || r == RoleResponder || r == RoleAuto
}

type SchedulerHint uint64

const (
	SchedulerUnspecifiedOrBalanced SchedulerHint = 0
	SchedulerLatency               SchedulerHint = 1
	SchedulerBalancedFair          SchedulerHint = 2
	SchedulerBulkThroughput        SchedulerHint = 3
	SchedulerGroupFair             SchedulerHint = 4
)

type Capabilities uint64

const (
	CapabilityPriorityHints         Capabilities = 1 << 0
	CapabilityStreamGroups          Capabilities = 1 << 1
	CapabilityMultilinkBasicRetired Capabilities = 1 << 2
	CapabilityMultilinkBasic                     = CapabilityMultilinkBasicRetired
	CapabilityPriorityUpdate        Capabilities = 1 << 3
	CapabilityOpenMetadata          Capabilities = 1 << 4
)

func (c Capabilities) Has(bit Capabilities) bool { return c&bit != 0 }

func (c Capabilities) SupportsOpenMetadataCarriage() bool {
	return c.Has(CapabilityOpenMetadata)
}

func (c Capabilities) SupportsPriorityUpdateCarriage() bool {
	return c.Has(CapabilityPriorityUpdate)
}

func (c Capabilities) CanCarryOpenInfo() bool {
	return c.SupportsOpenMetadataCarriage()
}

func (c Capabilities) CanCarryPriorityOnOpen() bool {
	return c.SupportsOpenMetadataCarriage() && c.Has(CapabilityPriorityHints)
}

func (c Capabilities) CanCarryGroupOnOpen() bool {
	return c.SupportsOpenMetadataCarriage() && c.Has(CapabilityStreamGroups)
}

func (c Capabilities) CanCarryPriorityInUpdate() bool {
	return c.SupportsPriorityUpdateCarriage() && c.Has(CapabilityPriorityHints)
}

func (c Capabilities) CanCarryGroupInUpdate() bool {
	return c.SupportsPriorityUpdateCarriage() && c.Has(CapabilityStreamGroups)
}

func (c Capabilities) HasPeerVisiblePrioritySemantics() bool {
	return c.CanCarryPriorityOnOpen() || c.CanCarryPriorityInUpdate()
}

func (c Capabilities) HasPeerVisibleGroupSemantics() bool {
	return c.CanCarryGroupOnOpen() || c.CanCarryGroupInUpdate()
}

type EXTSubtype uint64

const (
	EXTPriorityUpdate     EXTSubtype = 1
	ExtMLReadyRetired     EXTSubtype = 2
	ExtMLAttachRetired    EXTSubtype = 3
	ExtMLAttachAckRetired EXTSubtype = 4
	ExtMLDrainReqRetired  EXTSubtype = 5
	ExtMLDrainAckRetired  EXTSubtype = 6
)

func (s EXTSubtype) String() string {
	switch s {
	case EXTPriorityUpdate:
		return "PRIORITY_UPDATE"
	case ExtMLReadyRetired:
		return "ML_READY"
	case ExtMLAttachRetired:
		return "ML_ATTACH"
	case ExtMLAttachAckRetired:
		return "ML_ATTACH_ACK"
	case ExtMLDrainReqRetired:
		return "ML_DRAIN_REQ"
	case ExtMLDrainAckRetired:
		return "ML_DRAIN_ACK"
	default:
		return fmt.Sprintf("ext_subtype(%d)", uint64(s))
	}
}

type ErrorCode uint64

const (
	CodeNoError            ErrorCode = 0
	CodeProtocol           ErrorCode = 1
	CodeFlowControl        ErrorCode = 2
	CodeStreamLimit        ErrorCode = 3
	CodeRefusedStream      ErrorCode = 4
	CodeStreamState        ErrorCode = 5
	CodeStreamClosed       ErrorCode = 6
	CodeSessionClosing     ErrorCode = 7
	CodeCancelled          ErrorCode = 8
	CodeIdleTimeout        ErrorCode = 9
	CodeFrameSize          ErrorCode = 10
	CodeUnsupportedVersion ErrorCode = 11
	CodeRoleConflict       ErrorCode = 12
	CodeInternal           ErrorCode = 13
)

func (c ErrorCode) String() string {
	switch c {
	case CodeNoError:
		return "NO_ERROR"
	case CodeProtocol:
		return "PROTOCOL"
	case CodeFlowControl:
		return "FLOW_CONTROL"
	case CodeStreamLimit:
		return "STREAM_LIMIT"
	case CodeRefusedStream:
		return "REFUSED_STREAM"
	case CodeStreamState:
		return "STREAM_STATE"
	case CodeStreamClosed:
		return "STREAM_CLOSED"
	case CodeSessionClosing:
		return "SESSION_CLOSING"
	case CodeCancelled:
		return "CANCELLED"
	case CodeIdleTimeout:
		return "IDLE_TIMEOUT"
	case CodeFrameSize:
		return "FRAME_SIZE"
	case CodeUnsupportedVersion:
		return "UNSUPPORTED_VERSION"
	case CodeRoleConflict:
		return "ROLE_CONFLICT"
	case CodeInternal:
		return "INTERNAL"
	default:
		return fmt.Sprintf("error_code(%d)", uint64(c))
	}
}

type FrameType byte

const (
	FrameTypeDATA        FrameType = 1
	FrameTypeMAXDATA     FrameType = 2
	FrameTypeStopSending FrameType = 3
	FrameTypePING        FrameType = 4
	FrameTypePONG        FrameType = 5
	FrameTypeBLOCKED     FrameType = 6
	FrameTypeRESET       FrameType = 7
	FrameTypeABORT       FrameType = 8
	FrameTypeGOAWAY      FrameType = 9
	FrameTypeCLOSE       FrameType = 10
	FrameTypeEXT         FrameType = 11
)

func (t FrameType) Valid() bool {
	return t >= FrameTypeDATA && t <= FrameTypeEXT
}

func (t FrameType) String() string {
	switch t {
	case FrameTypeDATA:
		return "DATA"
	case FrameTypeMAXDATA:
		return "MAX_DATA"
	case FrameTypeStopSending:
		return "STOP_SENDING"
	case FrameTypePING:
		return "PING"
	case FrameTypePONG:
		return "PONG"
	case FrameTypeBLOCKED:
		return "BLOCKED"
	case FrameTypeRESET:
		return "RESET"
	case FrameTypeABORT:
		return "ABORT"
	case FrameTypeGOAWAY:
		return "GOAWAY"
	case FrameTypeCLOSE:
		return "CLOSE"
	case FrameTypeEXT:
		return "EXT"
	default:
		return fmt.Sprintf("frame_type(%d)", byte(t))
	}
}

const (
	FrameFlagOpenMetadata byte = 0x20
	FrameFlagFIN          byte = 0x40
)

type Frame struct {
	Length   uint64
	Type     FrameType
	Flags    byte
	StreamID uint64
	Payload  []byte
}

func (f Frame) Code() byte {
	return byte(f.Type) | f.Flags
}

type StreamMetadataType uint64

const (
	MetadataStreamPriority StreamMetadataType = 1
	MetadataStreamGroup    StreamMetadataType = 2
	MetadataOpenInfo       StreamMetadataType = 3
)

type DIAGType uint64

const (
	DIAGDebugText          DIAGType = 1
	DIAGRetryAfterMillis   DIAGType = 2
	DIAGOffendingStreamID  DIAGType = 3
	DIAGOffendingFrameType DIAGType = 4
)

type SettingID uint64

const (
	SettingInitialMaxStreamDataBidiLocallyOpened SettingID = 1
	SettingInitialMaxStreamDataBidiPeerOpened    SettingID = 2
	SettingInitialMaxStreamDataUni               SettingID = 3
	SettingInitialMaxData                        SettingID = 4
	SettingMaxIncomingStreamsBidi                SettingID = 5
	SettingMaxIncomingStreamsUni                 SettingID = 6
	SettingMaxFramePayload                       SettingID = 7
	SettingIdleTimeoutMillis                     SettingID = 8
	SettingKeepaliveHintMillis                   SettingID = 9
	SettingMaxControlPayloadBytes                SettingID = 10
	SettingMaxExtensionPayloadBytes              SettingID = 11
	SettingSchedulerHints                        SettingID = 12
)

type Settings struct {
	InitialMaxStreamDataBidiLocallyOpened uint64
	InitialMaxStreamDataBidiPeerOpened    uint64
	InitialMaxStreamDataUni               uint64
	InitialMaxData                        uint64
	MaxIncomingStreamsBidi                uint64
	MaxIncomingStreamsUni                 uint64
	MaxFramePayload                       uint64
	IdleTimeoutMillis                     uint64
	KeepaliveHintMillis                   uint64
	MaxControlPayloadBytes                uint64
	MaxExtensionPayloadBytes              uint64
	SchedulerHints                        SchedulerHint
}

func DefaultSettings() Settings {
	return Settings{
		InitialMaxStreamDataBidiLocallyOpened: 65536,
		InitialMaxStreamDataBidiPeerOpened:    65536,
		InitialMaxStreamDataUni:               65536,
		InitialMaxData:                        262144,
		MaxIncomingStreamsBidi:                256,
		MaxIncomingStreamsUni:                 256,
		MaxFramePayload:                       16384,
		IdleTimeoutMillis:                     0,
		KeepaliveHintMillis:                   0,
		MaxControlPayloadBytes:                4096,
		MaxExtensionPayloadBytes:              4096,
		SchedulerHints:                        SchedulerUnspecifiedOrBalanced,
	}
}

type Limits struct {
	MaxFramePayload          uint64
	MaxControlPayloadBytes   uint64
	MaxExtensionPayloadBytes uint64
}

func (s Settings) Limits() Limits {
	return Limits{
		MaxFramePayload:          s.MaxFramePayload,
		MaxControlPayloadBytes:   s.MaxControlPayloadBytes,
		MaxExtensionPayloadBytes: s.MaxExtensionPayloadBytes,
	}
}
