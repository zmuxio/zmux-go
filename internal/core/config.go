package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// Role is the role argument of session creation.
type Role uint8

const (
	RoleInitiator Role = iota
	RoleResponder
	RoleAuto
)

// Policy names a built-in scheduling policy.
type Policy uint8

const (
	PolicyAffinity Policy = iota
	PolicySessionAffinity
	PolicySpreadETA
	PolicySpreadRoundRobin
	PolicySpreadRateWeighted
	PolicyCustom
)

// Config is the resolved configuration of a session. The public package
// fills it from zmux.Config; the trace runner from a profile. Durations
// are nanoseconds; Never-like values are math.MaxInt64.
type Config struct {
	// Local is exactly what the endpoint sends (GREASE included).
	Local wire.Params

	HandshakeTimeout Duration
	KeepAlive        Duration // 0 = off; capped at half the effective idle timeout

	// UniTarget: see StagedUni.
	UniTarget uint64

	MaxStreamWindow Duration // unused placeholder for symmetry
	StreamWindowCap uint64   // 0 = auto (64 MiB floor + budget)
	ConnWindowCap   uint64   // 0 = auto (96 MiB floor + budget)
	LatencyBudget   Duration // B of auto-tuning
	MemoryBudget    uint64   // 0 = unlimited
	Budget          *Budget  // shared listener budget; replaces MemoryBudget

	SendBuffer    uint64
	SendBufferMax uint64 // 0 = auto

	DgramSendQ    int
	DgramRecvQ    int
	DgramStreamQ  int
	DgramRecvSize uint64

	MaxPipes     int
	Headroom     int
	ReplayWindow int
	Resume       Duration

	SpreadChunk   int
	SuspectFloor  Duration
	DeadTimeout   Duration // 0 = default
	Probation     Duration // <0 = default
	OverflowDelay Duration
	StandbyProbe  Duration
	ProbeInterval Duration
	ProbeJitter   float64 // 0 <= j < 1 (MULTIPATH.md 7.1); 0: a fixed schedule

	PSK      []byte
	Exporter []byte // TLS exporter of pipe 0 (E flag asserted by the application)

	// EarlyRemembered are the peer parameters remembered from an earlier
	// session: a client sends early streams in carrier early data under
	// them (SPEC.md 6.1.2); nil: none.
	EarlyRemembered *wire.Params

	ShapeLimits shaping.Limits

	// Test hooks (TRACES.md 3.3); never set from production configuration.
	Test TestHooks

	// The one-byte fields, together at the end for the layout, in the
	// order of the groups above.

	Role Role
	// TransportRole is the carrier role used when role negotiation is not in
	// use (who dialled the bootstrap carrier).
	TransportRole Role

	// StagedUni: initial_max_streams_uni is a staged value raised to
	// UniTarget on the first AcceptUni (SPEC.md 8.5 rule 4).
	StagedUni bool

	NoAutotune bool // fixed windows (tests)
	// WindowPredicate: session window growth waits for the application's
	// predicate (the driver runs it).
	WindowPredicate bool

	Datagrams bool
	// DgramRecvLimited: received datagrams wait for the driver's receive
	// limiter before they are queued.
	DgramRecvLimited bool

	Policy     Policy
	Redundancy bool

	EarlyZmuxPeer   bool
	EarlyReplayable bool
}

// TestHooks are the trace runner's deterministic values.
type TestHooks struct {
	Enabled      bool
	GreaseTLV    *wire.TLV
	PingSeqStart uint64
	RTT          Duration
	PipeRTT      map[uint64]Duration
	PipeCap      int
	// Seed seeds the session's random source of non-secret draws (probe
	// jitter); 0: a random seed. Honoured without Enabled.
	Seed uint64
}

// DefaultConfig returns the default configuration with the local
// parameters a zmux endpoint sends (SPEC.md 5.3, 5.5).
func DefaultConfig() Config {
	c := Config{
		Role:             RoleInitiator,
		TransportRole:    RoleInitiator,
		HandshakeTimeout: DefaultHandshakeTimeout,
		KeepAlive:        DefaultKeepAlive,
		StagedUni:        true,
		UniTarget:        256,
		LatencyBudget:    100 * Microsecond,
		MemoryBudget:     1 << 30,
		SendBuffer:       4 << 20,
		DgramSendQ:       64,
		DgramRecvQ:       128,
		DgramStreamQ:     32,
		DgramRecvSize:    1 << 20,
		MaxPipes:         1,
		Headroom:         8,
		SpreadChunk:      16384,
		SuspectFloor:     250 * Millisecond,
		Probation:        -1,
		OverflowDelay:    Second,
		StandbyProbe:     Second,
		ProbeInterval:    15 * Second,
		ProbeJitter:      DefaultProbeJitter,
		Redundancy:       true,
	}
	p := &c.Local
	p.Has = wire.HasIdle | wire.HasMaxData | wire.HasBidiLocal | wire.HasBidiRemote | wire.HasUni |
		wire.HasStreamsBidi | wire.HasStreamsUni | wire.HasResetAt | wire.HasRecordSize |
		wire.HasGoAway | wire.HasPreamble | wire.HasPriority | wire.HasPipeAck | wire.HasLanes
	p.MaxIdleTimeout = uint64(DefaultIdleTimeout / Millisecond)
	p.InitialMaxData = 1 << 20
	p.MaxStreamDataBidiLoc = 256 << 10
	p.MaxStreamDataBidiRem = 256 << 10
	p.MaxStreamDataUni = 256 << 10
	p.MaxStreamsBidi = 256
	p.MaxStreamsUni = 16
	p.MaxRecordSize = wire.MaxRecordSize
	p.Preamble = 4096
	p.MaxLanes = 258
	return c
}
