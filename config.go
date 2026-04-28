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

type Capabilities = wire.Capabilities

const (
	CapabilityPriorityHints         = wire.CapabilityPriorityHints
	CapabilityStreamGroups          = wire.CapabilityStreamGroups
	CapabilityMultilinkBasicRetired = wire.CapabilityMultilinkBasicRetired
	CapabilityMultilinkBasic        = wire.CapabilityMultilinkBasic
	CapabilityPriorityUpdate        = wire.CapabilityPriorityUpdate
	CapabilityOpenMetadata          = wire.CapabilityOpenMetadata
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
	SettingIdleTimeoutMillis                     = wire.SettingIdleTimeoutMillis
	SettingKeepaliveHintMillis                   = wire.SettingKeepaliveHintMillis
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
	EXTPriorityUpdate     = wire.EXTPriorityUpdate
	ExtMLReadyRetired     = wire.ExtMLReadyRetired
	ExtMLAttachRetired    = wire.ExtMLAttachRetired
	ExtMLAttachAckRetired = wire.ExtMLAttachAckRetired
	ExtMLDrainReqRetired  = wire.ExtMLDrainReqRetired
	ExtMLDrainAckRetired  = wire.ExtMLDrainAckRetired
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
// Prefer starting from DefaultConfig(). The zero value is not the repository
// default configuration because Role zero is RoleInitiator and the repository
// defaults also enable low-frequency idle keepalive probing plus a slower RTT
// sampling cap.
type Config struct {
	Role            Role
	TieBreakerNonce uint64
	MinProto        uint64
	MaxProto        uint64
	Capabilities    Capabilities
	Settings        Settings
	NonceSource     io.Reader
	// PrefacePadding appends one random ignored setting TLV to the local
	// establishment preface to vary its encoded length. It does not change
	// negotiated settings and is intended for transports that already provide
	// confidentiality, such as TLS, but still expose record-size patterns.
	PrefacePadding bool
	// PrefacePaddingMinBytes bounds the lower end of the random padding value
	// length when PrefacePadding is enabled. Zero uses the repository default.
	PrefacePaddingMinBytes uint64
	// PrefacePaddingMaxBytes bounds the upper end of the random padding value
	// length when PrefacePadding is enabled. Zero uses the repository default;
	// values above MaxPrefaceSettingsBytes are clamped to the remaining settings
	// TLV budget. If the effective minimum exceeds the effective maximum, the
	// runtime uses the maximum.
	PrefacePaddingMaxBytes uint64
	// KeepaliveInterval bounds how long inbound or outbound transport activity
	// may stay idle before the runtime probes with PING. Zero disables automatic
	// keepalive entirely, including KeepaliveMaxPingInterval.
	KeepaliveInterval time.Duration
	// KeepaliveMaxPingInterval bounds how long the runtime may go without
	// sending any local PING while keepalive is enabled. It is useful for
	// occasional RTT sampling even on continuously busy sessions. Zero disables
	// this extra cap and relies only on directional idle probing.
	KeepaliveMaxPingInterval time.Duration
	// KeepaliveTimeout bounds how long an outstanding keepalive ping may remain
	// unanswered before the session is aborted. When left at zero, the runtime
	// derives an adaptive default from the keepalive interval and observed RTT
	// within bounded caps so very high latency links remain usable.
	KeepaliveTimeout time.Duration
	// PingPadding adds an 8-byte padding tag plus random opaque bytes to local
	// PING frames, and appends random opaque bytes to recognized PONG replies
	// to vary liveness frame lengths. It also advertises a per-session
	// PingPaddingKey in the local preface. It does not change Ping(ctx, echo)
	// API behavior.
	PingPadding bool
	// PingPaddingMinBytes bounds the lower end of the extra PING payload bytes
	// and PONG suffix bytes when PingPadding is enabled. For PING, this total
	// includes the fixed 8-byte tag. Zero uses the repository default.
	PingPaddingMinBytes uint64
	// PingPaddingMaxBytes bounds the upper end of the extra PING payload bytes
	// and PONG suffix bytes when PingPadding is enabled. For PING, this total
	// includes the fixed 8-byte tag. Zero uses the repository default; values
	// above the negotiated control payload limit are clamped. If the effective
	// minimum exceeds the effective maximum, the runtime uses the maximum.
	PingPaddingMaxBytes uint64

	// SessionMemoryCap overrides the repository-default tracked-session-memory
	// hard cap. Zero uses the repository default.
	SessionMemoryCap uint64
	// PerStreamQueuedDataHWM overrides the repository-default per-stream
	// ordinary queued-data high watermark. Zero uses the repository default.
	PerStreamQueuedDataHWM uint64
	// SessionQueuedDataHWM overrides the repository-default session-wide
	// ordinary queued-data high watermark. Zero uses the repository default.
	SessionQueuedDataHWM uint64
	// UrgentQueuedBytesCap overrides the repository-default urgent/control lane
	// hard cap. Zero uses the repository default.
	UrgentQueuedBytesCap uint64
	// PendingControlBytesBudget overrides the repository-default coalesced
	// pending control-plane byte budget. Zero uses the repository default.
	PendingControlBytesBudget uint64
	// PendingPriorityBytesBudget overrides the repository-default coalesced
	// pending advisory byte budget. Zero uses the repository default.
	PendingPriorityBytesBudget uint64
	// RetainedOpenInfoBytesBudget overrides the repository-default retained
	// open_info byte budget. Zero uses the repository default.
	RetainedOpenInfoBytesBudget uint64
	// RetainedPeerReasonBytesBudget overrides the repository-default retained
	// peer reason-text byte budget. Zero uses the repository default.
	RetainedPeerReasonBytesBudget uint64
	// AggregateLateDataCap overrides the repository-default aggregate late-data
	// accounting cap. Zero uses the repository default.
	AggregateLateDataCap uint64
	// AcceptBacklogLimit overrides the repository-default visible accept backlog
	// count cap. Zero uses the repository default.
	AcceptBacklogLimit int
	// AcceptBacklogBytesLimit overrides the repository-default visible accept
	// backlog byte cap. Zero uses the repository default.
	AcceptBacklogBytesLimit uint64
	// TombstoneLimit overrides the repository-default retained tombstone count
	// limit. Zero uses the repository default.
	TombstoneLimit int
	// MarkerOnlyUsedStreamLimit overrides the repository-default maximum count
	// of retained marker-only used-stream entries after tombstones have been
	// compacted or reaped. Zero uses the repository-default derived cap.
	MarkerOnlyUsedStreamLimit int
	// AbuseWindow overrides the repository-default local anti-abuse accounting
	// window. Zero uses the repository default.
	AbuseWindow time.Duration
	// HiddenAbortChurnWindow overrides the repository-default hidden abort churn
	// detection window. Zero uses the repository default.
	HiddenAbortChurnWindow time.Duration
	// HiddenAbortChurnThreshold overrides the repository-default hidden
	// open-then-abort churn threshold. Zero uses the repository default.
	HiddenAbortChurnThreshold uint32
	// VisibleTerminalChurnWindow overrides the repository-default visible churn
	// detection window. Zero uses the repository default.
	VisibleTerminalChurnWindow time.Duration
	// VisibleTerminalChurnThreshold overrides the repository-default visible
	// open-then-reset/abort churn threshold. Zero uses the repository default.
	VisibleTerminalChurnThreshold uint32
	// InboundControlFrameBudget overrides the repository-default inbound
	// control-frame rolling window budget. Zero uses the repository default.
	InboundControlFrameBudget uint32
	// InboundControlBytesBudget overrides the repository-default inbound
	// control-byte rolling window budget. Zero uses the repository default.
	InboundControlBytesBudget uint64
	// InboundExtFrameBudget overrides the repository-default inbound EXT-frame
	// rolling window budget. Zero uses the repository default.
	InboundExtFrameBudget uint32
	// InboundExtBytesBudget overrides the repository-default inbound EXT-byte
	// rolling window budget. Zero uses the repository default.
	InboundExtBytesBudget uint64
	// InboundMixedFrameBudget overrides the repository-default mixed
	// control/EXT frame rolling window budget. Zero uses the repository default.
	InboundMixedFrameBudget uint32
	// InboundMixedBytesBudget overrides the repository-default mixed
	// control/EXT byte rolling window budget. Zero uses the repository default.
	InboundMixedBytesBudget uint64
	// NoOpControlFloodThreshold overrides the repository-default mixed no-op
	// control threshold. Zero uses the repository default.
	NoOpControlFloodThreshold uint32
	// NoOpMaxDataFloodThreshold overrides the repository-default no-op MAX_DATA
	// threshold. Zero uses the repository default.
	NoOpMaxDataFloodThreshold uint32
	// NoOpBlockedFloodThreshold overrides the repository-default no-op BLOCKED
	// threshold. Zero uses the repository default.
	NoOpBlockedFloodThreshold uint32
	// NoOpZeroDataFloodThreshold overrides the repository-default zero-length
	// DATA threshold. Zero uses the repository default.
	NoOpZeroDataFloodThreshold uint32
	// NoOpPriorityUpdateFloodThreshold overrides the repository-default no-op
	// PRIORITY_UPDATE threshold. Zero uses the repository default.
	NoOpPriorityUpdateFloodThreshold uint32
	// GroupRebucketChurnThreshold overrides the repository-default repeated
	// effective stream_group rebucketing threshold within the local
	// anti-abuse window when the negotiated scheduler baseline is group_fair.
	// Zero uses the repository default.
	GroupRebucketChurnThreshold uint32
	// InboundPingFloodThreshold overrides the repository-default inbound PING
	// flood threshold. Zero uses the repository default.
	InboundPingFloodThreshold uint32
	// StopSendingGracefulDrainWindow overrides the repository-default bounded
	// graceful-drain admission window used after peer STOP_SENDING before
	// falling back to RESET(CANCELLED). Zero uses the repository default. When
	// left unset, the runtime may widen the effective window from observed RTT
	// within bounded caps so high latency links do not spuriously reset streams.
	StopSendingGracefulDrainWindow time.Duration
	// StopSendingGracefulTailCap overrides the repository-default maximum
	// unavoidable tail, in bytes, that may still converge via DATA|FIN after
	// peer STOP_SENDING. Zero uses the repository default.
	StopSendingGracefulTailCap uint64
	// GracefulCloseDrainTimeout overrides the repository-default bounded
	// graceful-close drain wait before Close reports timeout to the caller.
	// This bounds how long Close will keep waiting for already-visible local
	// streams and provisionals to converge after GOAWAY, so it should usually be
	// tuned from application shutdown behavior rather than raw RTT alone. Zero
	// uses the repository default; when left unset, the runtime may widen the
	// effective wait from observed RTT within bounded caps while keeping the
	// default tuned for ordinary moderate-latency links.
	GracefulCloseDrainTimeout time.Duration

	// EventHandler receives lightweight connection/stream lifecycle notifications.
	EventHandler EventHandler
}

// OpenOptions carries optional repository-default open-time inputs for a new
// stream.
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
		Capabilities:             0,
		Settings:                 DefaultSettings(),
		NonceSource:              rand.Reader,
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
// sessions are not affected. Concurrent calls are race-safe, but the last
// completed update wins rather than merging independent edits.
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
	for maxPayload > 0 {
		lenLen, err := wire.VarintLen(maxPayload)
		if err != nil {
			return 0, err
		}
		overhead := uint64(typeLen + lenLen)
		if overhead <= remaining && maxPayload <= remaining-overhead {
			return maxPayload, nil
		}
		maxPayload--
	}
	return 0, nil
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
	"repository-default liveness rules keep at most one outstanding protocol PING and does not treat weak local signals as strong progress",
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

func copyStrings(in []string) []string {
	return append([]string(nil), in...)
}

func copySuites(in []ConformanceSuite) []ConformanceSuite {
	return append([]ConformanceSuite(nil), in...)
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
