package zmux

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

const (
	Magic                   = wire.Magic
	PrefaceVersion          = wire.PrefaceVersion
	ProtoVersion            = wire.ProtoVersion
	MaxPrefaceSettingsBytes = wire.MaxPrefaceSettingsBytes
	MaxVarint62             = wire.MaxVarint62
)

type Role = wire.Role

const (
	RoleInitiator = wire.RoleInitiator
	RoleResponder = wire.RoleResponder
	RoleAuto      = wire.RoleAuto
)

type SchedulerHint = wire.SchedulerHint

const (
	SchedulerUnspecifiedOrBalanced = wire.SchedulerUnspecifiedOrBalanced
	SchedulerLatency               = wire.SchedulerLatency
	SchedulerBalancedFair          = wire.SchedulerBalancedFair
	SchedulerBulkThroughput        = wire.SchedulerBulkThroughput
	SchedulerGroupFair             = wire.SchedulerGroupFair
)

func SchedulerHintFromCode(code uint64) SchedulerHint {
	return wire.SchedulerHintFromCode(code)
}

type Capabilities = wire.Capabilities

const (
	CapabilityPriorityHints  = wire.CapabilityPriorityHints
	CapabilityStreamGroups   = wire.CapabilityStreamGroups
	CapabilityPriorityUpdate = wire.CapabilityPriorityUpdate
	CapabilityOpenMetadata   = wire.CapabilityOpenMetadata
)

type SettingID = wire.SettingID

const (
	SettingInitialMaxStreamDataBidiLocallyOpened = wire.SettingInitialMaxStreamDataBidiLocallyOpened
	SettingInitialMaxStreamDataBidiPeerOpened    = wire.SettingInitialMaxStreamDataBidiPeerOpened
	SettingInitialMaxStreamDataUni               = wire.SettingInitialMaxStreamDataUni
	SettingInitialMaxData                        = wire.SettingInitialMaxData
	SettingMaxIncomingStreamsBidi                = wire.SettingMaxIncomingStreamsBidi
	SettingMaxIncomingStreamsUni                 = wire.SettingMaxIncomingStreamsUni
	SettingMaxFramePayload                       = wire.SettingMaxFramePayload
	SettingMaxControlPayloadBytes                = wire.SettingMaxControlPayloadBytes
	SettingMaxExtensionPayloadBytes              = wire.SettingMaxExtensionPayloadBytes
	SettingSchedulerHints                        = wire.SettingSchedulerHints
	SettingPingPaddingKey                        = wire.SettingPingPaddingKey
	SettingPrefacePadding                        = wire.SettingPrefacePadding
)

type Settings = wire.Settings

func DefaultSettings() Settings {
	return wire.DefaultSettings()
}

type FrameType = wire.FrameType

const (
	FrameTypeDATA        = wire.FrameTypeDATA
	FrameTypeMAXDATA     = wire.FrameTypeMAXDATA
	FrameTypeStopSending = wire.FrameTypeStopSending
	FrameTypePING        = wire.FrameTypePING
	FrameTypePONG        = wire.FrameTypePONG
	FrameTypeBLOCKED     = wire.FrameTypeBLOCKED
	FrameTypeRESET       = wire.FrameTypeRESET
	FrameTypeABORT       = wire.FrameTypeABORT
	FrameTypeGOAWAY      = wire.FrameTypeGOAWAY
	FrameTypeCLOSE       = wire.FrameTypeCLOSE
	FrameTypeEXT         = wire.FrameTypeEXT
)

const (
	FrameFlagOpenMetadata = wire.FrameFlagOpenMetadata
	FrameFlagFIN          = wire.FrameFlagFIN
)

type StreamMetadataType = wire.StreamMetadataType

const (
	MetadataStreamPriority = wire.MetadataStreamPriority
	MetadataStreamGroup    = wire.MetadataStreamGroup
	MetadataOpenInfo       = wire.MetadataOpenInfo
)

type DIAGType = wire.DIAGType

const (
	DIAGDebugText          = wire.DIAGDebugText
	DIAGRetryAfterMillis   = wire.DIAGRetryAfterMillis
	DIAGOffendingStreamID  = wire.DIAGOffendingStreamID
	DIAGOffendingFrameType = wire.DIAGOffendingFrameType
)

type EXTSubtype = wire.EXTSubtype

const (
	EXTPriorityUpdate = wire.EXTPriorityUpdate
)

type ErrorCode = wire.ErrorCode

const (
	CodeNoError            = wire.CodeNoError
	CodeProtocol           = wire.CodeProtocol
	CodeFlowControl        = wire.CodeFlowControl
	CodeStreamLimit        = wire.CodeStreamLimit
	CodeRefusedStream      = wire.CodeRefusedStream
	CodeStreamState        = wire.CodeStreamState
	CodeStreamClosed       = wire.CodeStreamClosed
	CodeSessionClosing     = wire.CodeSessionClosing
	CodeCancelled          = wire.CodeCancelled
	CodeIdleTimeout        = wire.CodeIdleTimeout
	CodeFrameSize          = wire.CodeFrameSize
	CodeUnsupportedVersion = wire.CodeUnsupportedVersion
	CodeRoleConflict       = wire.CodeRoleConflict
	CodeInternal           = wire.CodeInternal
)

type Limits = wire.Limits

type Preface = wire.Preface
type Negotiated = wire.Negotiated

// Config controls session establishment and runtime behavior.
//
// Prefer starting from DefaultConfig(). The zero value is not the default:
// Role zero is RoleInitiator, and defaults also enable padding and keepalive.
type Config struct {
	Role            Role
	TieBreakerNonce uint64
	MinProto        uint64
	MaxProto        uint64
	Capabilities    Capabilities
	Settings        Settings
	NonceSource     io.Reader
	// PrefacePadding adds one ignored settings TLV to the local preface.
	PrefacePadding bool
	// PrefacePaddingMinBytes is the lower padding length bound. Zero uses the default.
	PrefacePaddingMinBytes uint64
	// PrefacePaddingMaxBytes is the upper padding length bound. Zero uses the default.
	PrefacePaddingMaxBytes uint64
	// KeepaliveInterval bounds idle time before an automatic PING.
	// Zero disables automatic keepalive.
	KeepaliveInterval time.Duration
	// KeepaliveMaxPingInterval bounds time between local PINGs while keepalive is enabled.
	// Zero disables this extra cap.
	KeepaliveMaxPingInterval time.Duration
	// KeepaliveTimeout bounds how long a keepalive PING may remain unanswered.
	// Zero uses an adaptive default.
	KeepaliveTimeout time.Duration
	// PingPadding pads local PINGs and recognized PONG replies.
	// It also advertises a per-session PingPaddingKey.
	PingPadding bool
	// PingPaddingMinBytes is the lower PING/PONG padding bound.
	// For PING, it includes the fixed 8-byte tag. Zero uses the default.
	PingPaddingMinBytes uint64
	// PingPaddingMaxBytes is the upper PING/PONG padding bound.
	// For PING, it includes the fixed 8-byte tag. Zero uses the default.
	PingPaddingMaxBytes uint64

	// SessionMemoryCap overrides the tracked-session-memory cap. Zero uses the default.
	SessionMemoryCap uint64
	// PerStreamQueuedDataHWM overrides the per-stream queued-data high watermark.
	// Zero uses the default.
	PerStreamQueuedDataHWM uint64
	// SessionQueuedDataHWM overrides the session-wide queued-data high watermark.
	// Zero uses the default.
	SessionQueuedDataHWM uint64
	// UrgentQueuedBytesCap overrides the urgent/control lane cap. Zero uses the default.
	UrgentQueuedBytesCap uint64
	// PendingControlBytesBudget overrides the coalesced control byte budget.
	// Zero uses the default.
	PendingControlBytesBudget uint64
	// PendingPriorityBytesBudget overrides the coalesced advisory byte budget.
	// Zero uses the default.
	PendingPriorityBytesBudget uint64
	// RetainedOpenInfoBytesBudget overrides the retained open_info byte budget.
	// Zero uses the default.
	RetainedOpenInfoBytesBudget uint64
	// RetainedPeerReasonBytesBudget overrides the retained peer reason-text byte budget.
	// Zero uses the default.
	RetainedPeerReasonBytesBudget uint64
	// AggregateLateDataCap overrides aggregate late-data accounting. Zero uses the default.
	AggregateLateDataCap uint64
	// AcceptBacklogLimit overrides the visible accept backlog count. Zero uses the default.
	AcceptBacklogLimit int
	// AcceptBacklogBytesLimit overrides visible accept backlog bytes. Zero uses the default.
	AcceptBacklogBytesLimit uint64
	// TombstoneLimit overrides the retained tombstone count. Zero uses the default.
	TombstoneLimit int
	// MarkerOnlyUsedStreamLimit overrides retained marker-only used-stream entries.
	// Zero uses the derived default.
	MarkerOnlyUsedStreamLimit int
	// AbuseWindow overrides the local anti-abuse accounting window. Zero uses the default.
	AbuseWindow time.Duration
	// HiddenAbortChurnWindow overrides hidden abort churn detection. Zero uses the default.
	HiddenAbortChurnWindow time.Duration
	// HiddenAbortChurnThreshold overrides hidden open-then-abort churn. Zero uses the default.
	HiddenAbortChurnThreshold uint32
	// VisibleTerminalChurnWindow overrides visible terminal churn detection.
	// Zero uses the default.
	VisibleTerminalChurnWindow time.Duration
	// VisibleTerminalChurnThreshold overrides visible open-then-reset/abort churn.
	// Zero uses the default.
	VisibleTerminalChurnThreshold uint32
	// InboundControlFrameBudget overrides the inbound control-frame window.
	// Zero uses the default.
	InboundControlFrameBudget uint32
	// InboundControlBytesBudget overrides the inbound control-byte window.
	// Zero uses the default.
	InboundControlBytesBudget uint64
	// InboundExtFrameBudget overrides the inbound EXT-frame window. Zero uses the default.
	InboundExtFrameBudget uint32
	// InboundExtBytesBudget overrides the inbound EXT-byte window. Zero uses the default.
	InboundExtBytesBudget uint64
	// InboundMixedFrameBudget overrides the mixed control/EXT frame window.
	// Zero uses the default.
	InboundMixedFrameBudget uint32
	// InboundMixedBytesBudget overrides the mixed control/EXT byte window.
	// Zero uses the default.
	InboundMixedBytesBudget uint64
	// NoOpControlFloodThreshold overrides the mixed no-op control threshold.
	// Zero uses the default.
	NoOpControlFloodThreshold uint32
	// NoOpMaxDataFloodThreshold overrides the no-op MAX_DATA threshold.
	// Zero uses the default.
	NoOpMaxDataFloodThreshold uint32
	// NoOpBlockedFloodThreshold overrides the no-op BLOCKED threshold. Zero uses the default.
	NoOpBlockedFloodThreshold uint32
	// NoOpZeroDataFloodThreshold overrides the zero-length DATA threshold.
	// Zero uses the default.
	NoOpZeroDataFloodThreshold uint32
	// NoOpPriorityUpdateFloodThreshold overrides the no-op PRIORITY_UPDATE threshold.
	// Zero uses the default.
	NoOpPriorityUpdateFloodThreshold uint32
	// GroupRebucketChurnThreshold overrides repeated stream_group rebucketing.
	// Zero uses the default.
	GroupRebucketChurnThreshold uint32
	// InboundPingFloodThreshold overrides the inbound PING flood threshold.
	// Zero uses the default.
	InboundPingFloodThreshold uint32
	// StopSendingGracefulDrainWindow bounds graceful drain after peer STOP_SENDING.
	// Zero uses the default.
	StopSendingGracefulDrainWindow time.Duration
	// StopSendingGracefulTailCap bounds DATA|FIN tail after peer STOP_SENDING.
	// Zero uses the default.
	StopSendingGracefulTailCap uint64
	// GracefulCloseDrainTimeout bounds Close waiting for graceful drain.
	// Zero uses the default.
	GracefulCloseDrainTimeout time.Duration

	// EventHandler receives lightweight connection/stream lifecycle notifications.
	EventHandler EventHandler
}

// OpenOptions carries optional open-time inputs for a new stream.
//
// InitialPriority and InitialGroup may remain local sender-policy hints when
// the corresponding peer-visible carriage is unavailable. OpenInfo is
// peer-visible open-time metadata and is rejected if it cannot be carried on
// the opening frame.
type OpenOptions struct {
	InitialPriority *uint64
	InitialGroup    *uint64
	OpenInfo        []byte
}

const (
	defaultIdleKeepaliveInterval    = time.Minute
	defaultKeepaliveMaxPingInterval = 5 * time.Minute
	defaultPrefacePaddingMinBytes   = 16
	defaultPrefacePaddingMaxBytes   = 256
	defaultPingPaddingMinBytes      = 16
	defaultPingPaddingMaxBytes      = 64
	defaultCapabilities             = CapabilityOpenMetadata | CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups
)

var (
	defaultConfigMu       sync.RWMutex
	defaultConfigTemplate = builtinDefaultConfig()
)

func builtinDefaultConfig() Config {
	return Config{
		Role:                     RoleAuto,
		MinProto:                 ProtoVersion,
		MaxProto:                 ProtoVersion,
		Capabilities:             defaultCapabilities,
		Settings:                 DefaultSettings(),
		NonceSource:              rand.Reader,
		PrefacePadding:           true,
		PingPadding:              true,
		KeepaliveInterval:        defaultIdleKeepaliveInterval,
		KeepaliveMaxPingInterval: defaultKeepaliveMaxPingInterval,
	}
}

// DefaultConfig returns a copy of the process-wide default configuration
// template.
func DefaultConfig() *Config {
	defaultConfigMu.RLock()
	out := defaultConfigTemplate
	defaultConfigMu.RUnlock()
	return &out
}

// ConfigureDefaultConfig mutates the process-wide default configuration
// template used by DefaultConfig and by constructors called with nil Config.
//
// Call it during process initialization before creating sessions. Existing
// sessions are not affected. Concurrent calls are race-safe; last write wins.
//
// Per-session random fields are not retained in the template: TieBreakerNonce
// and Settings.PingPaddingKey are cleared after fn returns so each session can
// generate fresh values.
func ConfigureDefaultConfig(fn func(*Config)) {
	if fn == nil {
		return
	}
	defaultConfigMu.RLock()
	out := defaultConfigTemplate
	defaultConfigMu.RUnlock()

	fn(&out)

	out = sanitizeDefaultConfigTemplate(out)
	defaultConfigMu.Lock()
	defaultConfigTemplate = out
	defaultConfigMu.Unlock()
}

// ResetDefaultConfig restores the built-in process-wide default configuration
// template.
func ResetDefaultConfig() {
	defaultConfigMu.Lock()
	defaultConfigTemplate = builtinDefaultConfig()
	defaultConfigMu.Unlock()
}

func sanitizeDefaultConfigTemplate(out Config) Config {
	out = normalizeConfigDefaults(out)
	out.TieBreakerNonce = 0
	out.Settings.PingPaddingKey = 0
	return out
}

func normalizeConfigDefaults(out Config) Config {
	if out.MinProto == 0 {
		out.MinProto = ProtoVersion
	}
	if out.MaxProto == 0 {
		out.MaxProto = ProtoVersion
	}
	if out.Settings == (Settings{}) {
		out.Settings = DefaultSettings()
	} else {
		defaults := DefaultSettings()
		if out.Settings.MaxFramePayload == 0 {
			out.Settings.MaxFramePayload = defaults.MaxFramePayload
		}
		if out.Settings.MaxControlPayloadBytes == 0 {
			out.Settings.MaxControlPayloadBytes = defaults.MaxControlPayloadBytes
		}
		if out.Settings.MaxExtensionPayloadBytes == 0 {
			out.Settings.MaxExtensionPayloadBytes = defaults.MaxExtensionPayloadBytes
		}
	}
	if out.NonceSource == nil {
		out.NonceSource = rand.Reader
	}
	return out
}

func cloneConfig(cfg *Config) Config {
	if cfg == nil {
		return *DefaultConfig()
	}
	return normalizeConfigDefaults(*cfg)
}

func (c Config) LocalPreface() (Preface, error) {
	if !c.Role.Valid() {
		return Preface{}, wireError(CodeProtocol, "build preface", errInvalidRole)
	}

	nonce := c.TieBreakerNonce
	switch c.Role {
	case RoleInitiator, RoleResponder:
		nonce = 0
	case RoleAuto:
		if nonce == 0 {
			var err error
			nonce, err = randomVarint62(c.NonceSource)
			if err != nil {
				return Preface{}, wireError(CodeInternal, "build preface", err)
			}
		}
	}

	settings := c.Settings
	if c.PingPadding {
		if settings.PingPaddingKey == 0 {
			var err error
			settings.PingPaddingKey, err = randomVarint62(c.NonceSource)
			if err != nil {
				return Preface{}, wireError(CodeInternal, "build preface", err)
			}
		}
	} else {
		settings.PingPaddingKey = 0
	}

	return Preface{
		PrefaceVersion:  PrefaceVersion,
		Role:            c.Role,
		TieBreakerNonce: nonce,
		MinProto:        c.MinProto,
		MaxProto:        c.MaxProto,
		Capabilities:    c.Capabilities,
		Settings:        settings,
	}, nil
}

func marshalLocalPrefacePayload(local Preface, cfg Config) ([]byte, error) {
	if !cfg.PrefacePadding {
		return local.MarshalBinary()
	}
	padding, err := randomPrefacePadding(cfg.NonceSource, local.Settings, cfg.PrefacePaddingMinBytes, cfg.PrefacePaddingMaxBytes)
	if err != nil {
		return nil, wireError(CodeInternal, "build preface padding", err)
	}
	return wire.MarshalPrefaceWithSettingsPadding(local, padding)
}

func randomPrefacePadding(r io.Reader, settings Settings, configuredMin, configuredMax uint64) ([]byte, error) {
	maxPayload, err := maxPrefacePaddingPayloadBytes(settings, configuredMax)
	if err != nil || maxPayload == 0 {
		return nil, err
	}
	minPayload := configuredMin
	if minPayload == 0 {
		minPayload = defaultPrefacePaddingMinBytes
	}
	if minPayload > maxPayload {
		minPayload = maxPayload
	}
	paddingLen := minPayload
	if span := maxPayload - minPayload + 1; span > 1 {
		n, err := randomUint64n(r, span)
		if err != nil {
			return nil, err
		}
		paddingLen += n
	}
	padding := make([]byte, int(paddingLen))
	if _, err := io.ReadFull(randomReader(r), padding); err != nil {
		return nil, err
	}
	return padding, nil
}

func maxPrefacePaddingPayloadBytes(settings Settings, configuredMax uint64) (uint64, error) {
	settingsBuf, err := wire.MarshalSettingsTLV(settings)
	if err != nil {
		return 0, err
	}
	if len(settingsBuf) >= MaxPrefaceSettingsBytes {
		return 0, nil
	}
	maxPayload := configuredMax
	if maxPayload == 0 {
		maxPayload = defaultPrefacePaddingMaxBytes
	}
	remaining := uint64(MaxPrefaceSettingsBytes - len(settingsBuf))
	if maxPayload > remaining {
		maxPayload = remaining
	}
	typeLen, err := wire.VarintLen(uint64(SettingPrefacePadding))
	if err != nil {
		return 0, err
	}
	var low uint64
	high := maxPayload
	for low < high {
		candidate := low + (high-low+1)/2
		lenLen, err := wire.VarintLen(candidate)
		if err != nil {
			return 0, err
		}
		overhead := uint64(typeLen + lenLen)
		if overhead <= remaining && candidate <= remaining-overhead {
			low = candidate
		} else {
			high = candidate - 1
		}
	}
	return low, nil
}

func randomUint64n(r io.Reader, n uint64) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	if n > MaxVarint62+1 {
		return 0, fmt.Errorf("random range %d exceeds 62-bit source range", n)
	}
	limit := ((MaxVarint62 + 1) / n) * n
	for {
		v, err := randomUint62(r)
		if err != nil {
			return 0, err
		}
		if v < limit {
			return v % n, nil
		}
	}
}

func randomReader(r io.Reader) io.Reader {
	if r == nil {
		return rand.Reader
	}
	return r
}

func randomVarint62(r io.Reader) (uint64, error) {
	for {
		v, err := randomUint62(r)
		if err != nil {
			return 0, err
		}
		if v != 0 {
			return v, nil
		}
	}
}

func randomUint62(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(randomReader(r), buf[:]); err != nil {
		return 0, err
	}
	return (uint64(buf[0]&0x3f) << 56) |
		(uint64(buf[1]) << 48) |
		(uint64(buf[2]) << 40) |
		(uint64(buf[3]) << 32) |
		(uint64(buf[4]) << 24) |
		(uint64(buf[5]) << 16) |
		(uint64(buf[6]) << 8) |
		uint64(buf[7]), nil
}

func ParsePreface(data []byte) (Preface, error) {
	return wire.ParsePreface(data)
}

func ReadPreface(r io.Reader) (Preface, error) {
	return wire.ReadPreface(r)
}

func NegotiatePrefaces(local, peer Preface) (Negotiated, error) {
	return wire.NegotiatePrefaces(local, peer)
}

func marshalSettingsTLV(s Settings) ([]byte, error) {
	return wire.MarshalSettingsTLV(s)
}

// Claim identifies one repository-defined standardized claim string from the
// zmux document set.
type Claim string

const (
	ClaimWireV1                 Claim = "zmux-wire-v1"
	ClaimAPISemanticsProfileV1  Claim = "zmux-api-semantics-profile-v1"
	ClaimStreamAdapterProfileV1 Claim = "zmux-stream-adapter-profile-v1"
	ClaimOpenMetadata           Claim = "zmux-open_metadata"
	ClaimPriorityUpdate         Claim = "zmux-priority_update"
)

// ImplementationProfile identifies one repository-defined implementation
// profile name from the zmux document set.
type ImplementationProfile string

const (
	ProfileV1          ImplementationProfile = "zmux-v1"
	ProfileReferenceV1 ImplementationProfile = "zmux-reference-profile-v1"
)

// ConformanceSuite identifies one repository-defined local conformance suite
// selection bucket derived from CONFORMANCE.md.
type ConformanceSuite string

const (
	SuiteCoreWireInteroperability     ConformanceSuite = "core-wire-interoperability"
	SuiteInvalidInputHandling         ConformanceSuite = "invalid-input-handling"
	SuiteExtensionTolerance           ConformanceSuite = "extension-tolerance"
	SuiteCoreStreamLifecycle          ConformanceSuite = "core-stream-lifecycle"
	SuiteCoreFlowControl              ConformanceSuite = "core-flow-control"
	SuiteCoreSessionLifecycle         ConformanceSuite = "core-session-lifecycle"
	SuiteOpenMetadata                 ConformanceSuite = "open_metadata"
	SuitePriorityUpdate               ConformanceSuite = "priority_update"
	SuitePriorityHintsAndStreamGroups ConformanceSuite = "priority-hints-and-stream-groups"
	SuiteV1ProfileCompatibility       ConformanceSuite = "v1-profile-compatibility"
	SuiteAPISemanticsProfile          ConformanceSuite = "api-semantics-profile"
	SuiteStreamAdapterProfile         ConformanceSuite = "stream-adapter-profile"
	SuiteReferenceProfileClaimGate    ConformanceSuite = "reference-profile-claim-gate"
	SuiteReferenceQualityBehaviors    ConformanceSuite = "reference-quality-behaviors"
)

var knownClaims = []Claim{
	ClaimWireV1,
	ClaimAPISemanticsProfileV1,
	ClaimStreamAdapterProfileV1,
	ClaimOpenMetadata,
	ClaimPriorityUpdate,
}

var knownProfiles = []ImplementationProfile{
	ProfileV1,
	ProfileReferenceV1,
}

var knownConformanceSuites = []ConformanceSuite{
	SuiteCoreWireInteroperability,
	SuiteInvalidInputHandling,
	SuiteExtensionTolerance,
	SuiteCoreStreamLifecycle,
	SuiteCoreFlowControl,
	SuiteCoreSessionLifecycle,
	SuiteOpenMetadata,
	SuitePriorityUpdate,
	SuitePriorityHintsAndStreamGroups,
	SuiteV1ProfileCompatibility,
	SuiteAPISemanticsProfile,
	SuiteStreamAdapterProfile,
	SuiteReferenceProfileClaimGate,
	SuiteReferenceQualityBehaviors,
}

var claimAcceptanceChecklist = map[Claim][]string{
	ClaimWireV1: {
		"pass core wire interoperability",
		"pass invalid-input handling",
		"pass extension-tolerance behavior",
	},
	ClaimOpenMetadata: {
		"satisfy zmux-wire-v1",
		"negotiate open_metadata",
		"accept valid DATA|OPEN_METADATA on first opening DATA",
		"reject unnegotiated or misplaced OPEN_METADATA",
		"ignore unknown metadata TLVs",
		"drop duplicate singleton metadata while preserving the enclosing DATA",
	},
	ClaimPriorityUpdate: {
		"satisfy zmux-wire-v1",
		"negotiate priority_update",
		"process stream_priority and stream_group",
		"ignore open_info inside PRIORITY_UPDATE",
		"ignore unknown advisory TLVs",
		"ignore duplicate singleton advisory updates as one dropped update",
	},
	ClaimAPISemanticsProfileV1: {
		"document and implement the repository-default semantic operation families from API_SEMANTICS.md, including full local close helper, graceful send-half completion, read-side stop, send-side reset, whole-stream abort, structured error surfacing, open/cancel behavior, and accept visibility rules",
		"document whether the binding exposes a stream-style convenience profile, a full-control protocol surface, or both",
		"exact API spellings are not required",
	},
	ClaimStreamAdapterProfileV1: {
		"satisfy the stream-adapter subset from API_SEMANTICS.md, including bidirectional/unidirectional open and accept mapping",
		"provide one consistent convenience mapping or fuller documented control layer or both",
		"document limits/non-goals",
	},
}

var profileAcceptanceChecklist = map[ImplementationProfile][]string{
	ProfileV1: {
		"satisfy zmux-wire-v1",
		"interoperate on explicit-role and role=auto establishment",
		"pass core stream-lifecycle scenarios",
		"pass core flow-control scenarios",
		"pass core session-lifecycle scenarios",
		"satisfy every currently active same-version optional surface in this repository",
		"negotiate and handle open_metadata, priority_update, priority_hints, and stream_groups correctly",
	},
	ProfileReferenceV1: {
		"satisfy zmux-v1",
		"satisfy the repository-defined reference-profile claim gate",
		"preserve the documented repository-default sender, memory, liveness, API, and scheduling behavior closely enough for release claims",
	},
}

var referenceProfileClaimGate = []string{
	"repository-default stream-style CloseRead() emits STOP_SENDING(CANCELLED) when that convenience profile is exposed, while fuller control surfaces MAY additionally expose caller-selected codes and diagnostics for STOP_SENDING, RESET, and ABORT",
	"repository-default Close() acts as a full local close helper",
	"repository-default Close() on a unidirectional stream silently ignores the locally absent direction rather than failing solely because that half does not exist",
	"each exposed API surface keeps one documented primary spelling per operation family, with any extra convenience spellings documented as wrappers over the same semantic action rather than as distinct lifecycle operations",
	"before session-ready, repository-default sender behavior emits only the local preface and a fatal establishment CLOSE, and emits none of new-stream DATA, stream-scoped control, ordinary session-scoped control, or EXT",
	"repository-default sender and receiver memory rules enforce the documented hidden-state, provisional-open, and late-tail bounds",
	"repository-default liveness rules keep at most one outstanding protocol PING and do not treat weak local signals as strong progress",
}

var claimRequiredSuites = map[Claim][]ConformanceSuite{
	ClaimWireV1: {
		SuiteCoreWireInteroperability,
		SuiteInvalidInputHandling,
		SuiteExtensionTolerance,
	},
	ClaimOpenMetadata: {
		SuiteCoreWireInteroperability,
		SuiteInvalidInputHandling,
		SuiteExtensionTolerance,
		SuiteOpenMetadata,
	},
	ClaimPriorityUpdate: {
		SuiteCoreWireInteroperability,
		SuiteInvalidInputHandling,
		SuiteExtensionTolerance,
		SuitePriorityUpdate,
	},
	ClaimAPISemanticsProfileV1: {
		SuiteAPISemanticsProfile,
	},
	ClaimStreamAdapterProfileV1: {
		SuiteStreamAdapterProfile,
	},
}

var profileRequiredSuites = map[ImplementationProfile][]ConformanceSuite{
	ProfileV1: {
		SuiteCoreWireInteroperability,
		SuiteInvalidInputHandling,
		SuiteExtensionTolerance,
		SuiteCoreStreamLifecycle,
		SuiteCoreFlowControl,
		SuiteCoreSessionLifecycle,
		SuiteOpenMetadata,
		SuitePriorityUpdate,
		SuitePriorityHintsAndStreamGroups,
		SuiteV1ProfileCompatibility,
	},
	ProfileReferenceV1: {
		SuiteCoreWireInteroperability,
		SuiteInvalidInputHandling,
		SuiteExtensionTolerance,
		SuiteCoreStreamLifecycle,
		SuiteCoreFlowControl,
		SuiteCoreSessionLifecycle,
		SuiteOpenMetadata,
		SuitePriorityUpdate,
		SuitePriorityHintsAndStreamGroups,
		SuiteV1ProfileCompatibility,
		SuiteAPISemanticsProfile,
		SuiteStreamAdapterProfile,
		SuiteReferenceProfileClaimGate,
		SuiteReferenceQualityBehaviors,
	},
}

var coreModuleTargetClaims = []Claim{
	ClaimWireV1,
	ClaimAPISemanticsProfileV1,
	ClaimOpenMetadata,
	ClaimPriorityUpdate,
}

var coreModuleTargetProfiles = []ImplementationProfile{
	ProfileV1,
}

var coreModuleTargetSuites = mergeRequiredSuites(coreModuleTargetClaims, coreModuleTargetProfiles)

func copyStrings(in []string) []string {
	return append([]string(nil), in...)
}

func copySuites(in []ConformanceSuite) []ConformanceSuite {
	return append([]ConformanceSuite(nil), in...)
}

func mergeRequiredSuites(claims []Claim, profiles []ImplementationProfile) []ConformanceSuite {
	seen := make(map[ConformanceSuite]struct{}, len(knownConformanceSuites))
	for _, claim := range claims {
		for _, suite := range claimRequiredSuites[claim] {
			seen[suite] = struct{}{}
		}
	}
	for _, profile := range profiles {
		for _, suite := range profileRequiredSuites[profile] {
			seen[suite] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]ConformanceSuite, 0, len(seen))
	for _, suite := range knownConformanceSuites {
		if _, ok := seen[suite]; ok {
			out = append(out, suite)
		}
	}
	return out
}

// KnownClaims returns the repository-defined claim names recognized by this
// package. The returned slice is a copy and may be modified by the caller.
func KnownClaims() []Claim {
	return append([]Claim(nil), knownClaims...)
}

// KnownImplementationProfiles returns the repository-defined implementation
// profile names recognized by this package. The returned slice is a copy and
// may be modified by the caller.
func KnownImplementationProfiles() []ImplementationProfile {
	return append([]ImplementationProfile(nil), knownProfiles...)
}

// KnownConformanceSuites returns the repository-defined local conformance
// suite selection buckets recognized by this package.
func KnownConformanceSuites() []ConformanceSuite {
	return copySuites(knownConformanceSuites)
}

// Valid reports whether the claim matches one of the repository-defined claim
// names in the current zmux document set.
func (c Claim) Valid() bool {
	switch c {
	case ClaimWireV1, ClaimAPISemanticsProfileV1, ClaimStreamAdapterProfileV1, ClaimOpenMetadata, ClaimPriorityUpdate:
		return true
	default:
		return false
	}
}

// Valid reports whether the implementation profile matches one of the
// repository-defined implementation-profile names in the current zmux document
// set.
func (p ImplementationProfile) Valid() bool {
	switch p {
	case ProfileV1, ProfileReferenceV1:
		return true
	default:
		return false
	}
}

// Valid reports whether the suite matches one of the repository-defined local
// conformance suite selection buckets.
func (s ConformanceSuite) Valid() bool {
	switch s {
	case SuiteCoreWireInteroperability,
		SuiteInvalidInputHandling,
		SuiteExtensionTolerance,
		SuiteCoreStreamLifecycle,
		SuiteCoreFlowControl,
		SuiteCoreSessionLifecycle,
		SuiteOpenMetadata,
		SuitePriorityUpdate,
		SuitePriorityHintsAndStreamGroups,
		SuiteV1ProfileCompatibility,
		SuiteAPISemanticsProfile,
		SuiteStreamAdapterProfile,
		SuiteReferenceProfileClaimGate,
		SuiteReferenceQualityBehaviors:
		return true
	default:
		return false
	}
}

// Claims returns the repository-defined claim bundle associated with the given
// implementation profile. The returned slice is a copy and may be modified by
// the caller.
func (p ImplementationProfile) Claims() []Claim {
	switch p {
	case ProfileV1:
		return []Claim{
			ClaimWireV1,
			ClaimOpenMetadata,
			ClaimPriorityUpdate,
		}
	case ProfileReferenceV1:
		return []Claim{
			ClaimWireV1,
			ClaimAPISemanticsProfileV1,
			ClaimStreamAdapterProfileV1,
			ClaimOpenMetadata,
			ClaimPriorityUpdate,
		}
	default:
		return nil
	}
}

// AcceptanceChecklist returns the repository-defined minimum acceptance
// checklist for the claim. The returned slice is a copy and may be modified by
// the caller.
func (c Claim) AcceptanceChecklist() []string {
	return copyStrings(claimAcceptanceChecklist[c])
}

// AcceptanceChecklist returns the repository-defined minimum acceptance
// checklist for the implementation profile. The returned slice is a copy and
// may be modified by the caller.
func (p ImplementationProfile) AcceptanceChecklist() []string {
	return copyStrings(profileAcceptanceChecklist[p])
}

// ReferenceProfileClaimGate returns the repository-defined reference-profile
// gate conditions from the current zmux document set. The returned slice is a
// copy and may be modified by the caller.
func ReferenceProfileClaimGate() []string {
	return copyStrings(referenceProfileClaimGate)
}

// RequiredConformanceSuites returns the repository-defined local suite
// selection buckets needed to substantiate the claim.
func (c Claim) RequiredConformanceSuites() []ConformanceSuite {
	return copySuites(claimRequiredSuites[c])
}

// RequiredConformanceSuites returns the repository-defined local suite
// selection buckets needed to substantiate the implementation profile.
func (p ImplementationProfile) RequiredConformanceSuites() []ConformanceSuite {
	return copySuites(profileRequiredSuites[p])
}

// ReleaseCertificationGate returns the repository-defined pass/fail local
// suite selection gate for the implementation profile.
func (p ImplementationProfile) ReleaseCertificationGate() []ConformanceSuite {
	return p.RequiredConformanceSuites()
}

// CoreModuleTargetClaims returns the repository-defined conformance claims
// targeted by the core zmux module. The returned slice is a copy and may be
// modified by the caller.
func CoreModuleTargetClaims() []Claim {
	return append([]Claim(nil), coreModuleTargetClaims...)
}

// CoreModuleTargetImplementationProfiles returns the repository-defined
// implementation profiles targeted by the core zmux module. The returned slice
// is a copy and may be modified by the caller.
func CoreModuleTargetImplementationProfiles() []ImplementationProfile {
	return append([]ImplementationProfile(nil), coreModuleTargetProfiles...)
}

// CoreModuleTargetSuites returns the ordered union of conformance suites needed
// to substantiate the core zmux module's target claims and profiles. The
// returned slice is a copy and may be modified by the caller.
func CoreModuleTargetSuites() []ConformanceSuite {
	return copySuites(coreModuleTargetSuites)
}

type Frame = wire.Frame
type dataPayload = wire.DataPayload
type streamMetadata = wire.ParsedStreamMetadata
type goAwayPayload = wire.GoAwayPayload
type TLV = wire.TLV

var (
	maxRetainedReadFrameBytes = retainedReadFrameBufferLimit(DefaultSettings().MaxFramePayload)
	defaultNormalizedLimits   = normalizeLimits(Limits{})
)

func ParseFrame(src []byte, limits Limits) (Frame, int, error) {
	return wire.ParseFrame(src, limits)
}

func AppendTLV(dst []byte, typ uint64, value []byte) ([]byte, error) {
	return wire.AppendTLV(dst, typ, value)
}

func ParseTLVs(src []byte) ([]TLV, error) {
	return wire.ParseTLVs(src)
}

func VarintLen(v uint64) (int, error) {
	return wire.VarintLen(v)
}

func AppendVarint(dst []byte, v uint64) ([]byte, error) {
	return wire.AppendVarint(dst, v)
}

func EncodeVarint(v uint64) ([]byte, error) {
	return wire.EncodeVarint(v)
}

func ParseVarint(src []byte) (uint64, int, error) {
	return wire.ParseVarint(src)
}

func ReadVarint(r io.ByteReader) (uint64, int, error) {
	return wire.ReadVarint(r)
}

func ReadFrame(r io.Reader, limits Limits) (Frame, error) {
	return wire.ReadFrame(r, limits)
}

func readFrameBuffered(r io.Reader, limits Limits, dst []byte) (Frame, []byte, *wire.FrameReadBufferHandle, error) {
	return wire.ReadFrameBuffered(r, limits, dst)
}

func releaseReadFrameBuffer(buf []byte, handle *wire.FrameReadBufferHandle) {
	wire.ReleaseReadFrameBuffer(buf, handle)
}

func retainedReadFrameBufferLimit(maxFramePayload uint64) int {
	if maxFramePayload == 0 {
		maxFramePayload = DefaultSettings().MaxFramePayload
	}
	maxInt := int(^uint(0) >> 1)
	if maxFramePayload > uint64(maxInt-maxEncodedFrameOverhead) {
		return maxInt
	}
	return int(maxFramePayload) + maxEncodedFrameOverhead
}

func retainReadFrameBufferCapped(buf []byte, maxRetained int) []byte {
	if maxRetained <= 0 || cap(buf) > maxRetained {
		return nil
	}
	return buf[:0]
}

func retainReadFrameBuffer(buf []byte) []byte {
	return retainReadFrameBufferCapped(buf, maxRetainedReadFrameBytes)
}

func retainReadFrameBufferForPayloadLimit(buf []byte, maxFramePayload uint64) []byte {
	return retainReadFrameBufferCapped(buf, retainedReadFrameBufferLimit(maxFramePayload))
}

func normalizeLimits(limits Limits) Limits {
	return wire.NormalizeLimits(limits)
}

func validateFrame(f Frame, limits Limits, inbound bool) error {
	return wire.ValidateFrame(f, limits, inbound)
}

func frameSizeError(op string, err error) error {
	return wire.FrameSizeError(op, err)
}

func parseExtFrame(payload []byte) (EXTSubtype, []byte, bool) {
	subtype, n, err := ParseVarint(payload)
	if err != nil {
		return 0, nil, false
	}
	return EXTSubtype(subtype), payload[n:], true
}

func parseDataPayload(payload []byte, flags byte) (dataPayload, error) {
	return wire.ParseDataPayload(payload, flags)
}

func parseDataPayloadView(payload []byte, flags byte) (dataPayload, error) {
	return wire.ParseDataPayloadView(payload, flags)
}

func parseMetadataVarint(value []byte) (uint64, error) {
	return wire.ParseMetadataVarint(value)
}

func parseErrorPayload(payload []byte) (uint64, string, error) {
	return wire.ParseErrorPayload(payload)
}

func parseGOAWAYPayload(payload []byte) (goAwayPayload, error) {
	return wire.ParseGOAWAYPayload(payload)
}

func buildOpenMetadataPrefix(caps Capabilities, opts OpenOptions, maxFramePayload uint64) ([]byte, error) {
	return buildOpenMetadataPrefixFromCurrent(caps, opts.InitialPriority, opts.InitialGroup, opts.OpenInfo, maxFramePayload)
}

func buildOpenMetadataPrefixFromCurrent(
	caps Capabilities,
	priority *uint64,
	group *uint64,
	openInfo []byte,
	maxFramePayload uint64,
) ([]byte, error) {
	return wire.BuildOpenMetadataPrefix(caps, priority, group, openInfo, maxFramePayload)
}

func appendOpenMetadataPrefix(
	dst []byte,
	caps Capabilities,
	priority *uint64,
	group *uint64,
	openInfo []byte,
	maxFramePayload uint64,
) ([]byte, error) {
	return wire.AppendOpenMetadataPrefix(dst, caps, priority, group, openInfo, maxFramePayload)
}

func buildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code uint64, reason string) ([]byte, error) {
	return wire.BuildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code, reason)
}

func validateGoAwayWatermarkForDirection(streamID uint64, bidi bool) error {
	if streamID == 0 {
		return nil
	}
	if streamID > wire.MaxVarint62 {
		return fmt.Errorf("stream %d exceeds varint62 range for GOAWAY watermark", streamID)
	}
	if state.StreamIsBidi(streamID) != bidi {
		return fmt.Errorf("stream %d has wrong direction for GOAWAY watermark", streamID)
	}
	return nil
}

func validateGoAwayWatermarkCreator(owner Role, streamID uint64) error {
	if streamID == 0 {
		return nil
	}
	if !state.StreamIsLocal(owner, streamID) {
		return fmt.Errorf("stream %d is not creatable by role %s", streamID, owner)
	}
	return nil
}

func buildCodePayload(code uint64, reason string, maxPayload uint64) ([]byte, error) {
	payload, err := EncodeVarint(code)
	if err != nil {
		return nil, err
	}
	return appendDebugTextTLVCapped(payload, reason, maxPayload), nil
}

func clampVarint62(v uint64) uint64 {
	if v > MaxVarint62 {
		return MaxVarint62
	}
	return v
}

func encodeClampedVarint62(v uint64) []byte {
	b, _ := EncodeVarint(clampVarint62(v))
	return b
}

func clampedVarintLen62(v uint64) uint64 {
	n, _ := VarintLen(clampVarint62(v))
	return uint64(n)
}
