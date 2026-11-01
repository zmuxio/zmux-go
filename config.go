package zmux

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// Session defaults, which the zero values of Config fields select.
const (
	// DefaultKeepAlivePeriod is the default Config.KeepAlivePeriod.
	DefaultKeepAlivePeriod = time.Duration(core.DefaultKeepAlive)
	// DefaultMaxIdleTimeout is the default Config.MaxIdleTimeout.
	DefaultMaxIdleTimeout = time.Duration(core.DefaultIdleTimeout)
	// DefaultProbeJitter is the default SchedulerParams.ProbeJitter.
	DefaultProbeJitter = core.DefaultProbeJitter
	// MaxRedialBackoff caps the wait between Config.Redial calls, the
	// built-in backoff and RedialPolicy's DialAfter alike.
	MaxRedialBackoff = 30 * time.Second
	// DefaultMaxBackoff is the default PipePool.MaxBackoff of MaintainPipes.
	DefaultMaxBackoff = 60 * time.Second
)

// DefaultMaxRecordSize is the max_record_size in effect while it is
// absent (16382 bytes): records are built at most this large before the
// peer's parameters arrive, and always on a TLS carrier.
const DefaultMaxRecordSize = wire.DefaultMaxRecordSize

// Config configures a session; nil *Config means all defaults. Its fields
// follow the Settings rules of the package documentation: the zero value
// selects the stated default, a negative timeout is no timeout, other
// negative values are invalid (ErrInvalidConfig) unless the field gives
// them a meaning, and the Disable and No fields switch features off.
type Config struct {
	// HandshakeTimeout bounds establishment and join confirmation. Zero
	// is 10 s; a negative value is no timeout.
	HandshakeTimeout time.Duration
	// MaxIdleTimeout is max_idle_timeout, the time without inbound
	// activity after which the session closes. Zero is 30 s; a negative
	// value announces no idle timeout; a positive one is at least 1 ms.
	MaxIdleTimeout time.Duration
	// KeepAlivePeriod sends a QX_PING when the session has had no activity
	// this long. Zero is 15 s; a negative value turns keep-alive off, as
	// for net.Dialer.KeepAlive; a positive one is at least 1 s. The period in effect is capped at half the
	// effective idle timeout, and each wait is drawn from [k x (1 - j), k]
	// with j the probe jitter (SchedulerParams.ProbeJitter). On, an idle
	// session stays open while the peer answers and closes within the
	// idle timeout once its carrier dies; off, the idle timeout ends any
	// session without application activity.
	KeepAlivePeriod time.Duration

	// InitialStreamReceiveWindow is the initial receive window of a
	// stream. Zero is 256 KiB; otherwise at least 4 KiB.
	InitialStreamReceiveWindow uint64
	// MaxStreamReceiveWindow caps the auto-tuning of a stream's window.
	// Zero is automatic: a 64 MiB floor that grows within the memory
	// budget.
	MaxStreamReceiveWindow uint64
	// LatencyBudget is the B of window auto-tuning. Zero is 100 us (tuning
	// needs a positive budget); a negative value is invalid.
	LatencyBudget time.Duration
	// InitialConnectionReceiveWindow is the initial session window. Zero
	// is 1 MiB; otherwise at least 16 KiB.
	InitialConnectionReceiveWindow uint64
	// MaxConnectionReceiveWindow caps the session window. Zero is
	// automatic: a 96 MiB floor.
	MaxConnectionReceiveWindow uint64
	// MemoryBudget bounds the memory of a session without a listener.
	// Zero is 1 GiB; NoMemoryLimit is unlimited; another negative value
	// is invalid.
	MemoryBudget int64

	// MaxIncomingStreams is initial_max_streams_bidi, the bidirectional
	// streams the peer may open at first. Zero is 256; otherwise at least
	// 4; granting none takes NoIncomingStreams; a negative value is
	// invalid.
	MaxIncomingStreams int64
	// NoIncomingStreams grants the peer no bidirectional stream.
	NoIncomingStreams bool
	// MaxIncomingUniStreams is initial_max_streams_uni. Zero is a staged
	// credit: 16, raised to 256 on the first AcceptUniStream; a negative
	// value is invalid.
	MaxIncomingUniStreams int64
	// NoIncomingUniStreams grants the peer no unidirectional stream.
	NoIncomingUniStreams bool

	// EnableDatagrams announces max_datagram_frame_size and datagram flows.
	EnableDatagrams bool
	// DatagramSendQueue, DatagramReceiveQueue, DatagramStreamQueue (per
	// stream) and DatagramReceiveBytes size the datagram queues. Zero is
	// 64, 128, 32 and 1 MiB (an empty queue would drop every datagram); a
	// negative value is invalid.
	DatagramSendQueue    int
	DatagramReceiveQueue int
	DatagramStreamQueue  int
	DatagramReceiveBytes int

	// SendBuffer starts the written-but-unreleased byte budget. Zero is
	// 4 MiB. Multipath sessions grow it to the retransmit budget, within
	// MaxSendBuffer.
	SendBuffer uint64
	// MaxSendBuffer fixes its growth maximum (at least SendBuffer). Zero is
	// automatic: a 64 MiB floor, above it within the memory budget.
	MaxSendBuffer uint64

	// MaxPipes is the attachments this endpoint aims for. Zero or 1 is
	// multipath off; 8 is recommended when multipath is used.
	MaxPipes int
	// PipeHeadroom is H of the attachment cap. Zero is 8.
	PipeHeadroom int
	// ReplayWindow is W. Zero is max(64, 2*(MaxPipes+H)); a smaller
	// window is raised to MaxPipes+H.
	ReplayWindow int
	// MaxLanes is the lanes accepted per inbound pipe. Zero is 258; 1
	// accepts the stream lane only.
	MaxLanes int

	// ResumeTimeout enables resume with this grace period. Zero is no
	// resume.
	ResumeTimeout time.Duration

	// Scheduler is the session's scheduling policy (nil: Affinity).
	Scheduler Scheduler
	// SchedulerParams tune the scheduler and failure detection.
	SchedulerParams SchedulerParams
	// ShapingLimits bound what a Shaper adds (SetShaper); a negative
	// duration or a MaxPaddingRatio outside [0, 1] is invalid.
	ShapingLimits shaping.Limits
	// Shaper is installed when the session is created, as by SetShaper
	// (nil: no shaping): every outbound pipe of the session, the
	// bootstrap pipe of an accepted session and joined pipes included, is
	// shaped from its first record. SetShaper replaces it.
	Shaper ShaperFactory

	// PipeAuth selects the join secret (required for multipath).
	PipeAuth PipeAuth

	// EarlyData enables early data (SPEC.md 6.1.2).
	EarlyData EarlyData

	// DatagramLimiter polices datagrams in each direction:
	// a datagram passes when TryReady allows its whole payload, and is
	// then charged; otherwise it is dropped and counted.
	DatagramLimiter DatagramLimiter

	// Tracer receives session events on a goroutine of its own.
	Tracer *Tracer
	// Credential is presented on the carrier of a client session by the
	// functions that open one with this Config: Client, Dial and DialTLS
	// and the dialers of zmuxquic, zmuxws and zmuxh3 (nil: none). Pipes
	// opened otherwise, for AddPipe or Redial, present theirs where they
	// are dialed (PresentPipe, the adapters' options).
	Credential Credential
	// Redial re-establishes pipes after failures (resume): it is called
	// as soon as the session lacks a direction or suspends, delayed only
	// by the wait since its previous call for that direction. It keeps
	// one pipe per direction; a session that keeps several pipes is
	// maintained with Conn.MaintainPipes and a PipePool, which also
	// replaces failed pipes, and leaves Redial nil.
	Redial func(ctx context.Context, need Direction) (PipeIO, error)
	// RedialPolicy, when set, is asked before each Redial call; DialAfter
	// is the wait from the previous call to the next (at most 30 s; 0: the
	// built-in backoff), and ProbeInterval and ProbeJitter apply to the
	// pipe a call yields. Count and Retire are not used. A decision that
	// fails PoolDecision.Validate is rejected: the call goes ahead as with
	// the zero decision and the error is reported to DecisionRejected.
	RedialPolicy PoolPolicy
	// DecisionRejected, when set, receives the PoolDecision.Validate error
	// (ErrInvalidConfig) of each decision of RedialPolicy the session
	// rejected, on the goroutine that calls Redial and outside every lock;
	// it must not block, and a panic in it is contained. Redialling goes
	// on whether it is set or not.
	DecisionRejected func(err error)
	// AllowConnectionWindowIncrease approves session window growth.
	AllowConnectionWindowIncrease func(delta uint64) bool
	// Clock is the session's time source (nil: the host's monotonic
	// clock): every session timer (handshake, idle, keep-alive, probes,
	// resume grace, retransmission, shaping and pacing), the RTT and rate
	// estimates, stream deadlines and the Redial and PipePool waits run on
	// it, so a fake clock drives them deterministically. What stays on
	// the host clock is listed in the package documentation (Time).
	Clock timeshim.Clock
}

// DatagramLimiter holds the limiters of the two datagram directions.
type DatagramLimiter struct {
	Send, Receive Limiter
}

// SchedulerParams tune multipath scheduling and failure detection, under
// the rules of Config.
type SchedulerParams struct {
	// SuspectFloor is the least time before a pipe without
	// acknowledgements turns suspect. Zero is 250 ms; otherwise at least
	// 50 ms.
	SuspectFloor time.Duration
	// DeadTimeout is the time after which a suspect pipe fails. Zero is
	// derived from the RTT; otherwise at least 1 s.
	DeadTimeout time.Duration
	// Probation is the base of a pipe's probation. Zero is the default
	// (from the RTT and the pipe's history); a negative value is invalid.
	Probation time.Duration
	// OverflowDelay is how long higher tiers stay saturated before a
	// lower tier takes data. Zero is 1 s; a negative value is invalid.
	OverflowDelay time.Duration
	// StandbyProbe is the probe interval of standby pipes while the
	// session carries traffic. Zero is 1 s; a negative value turns these
	// standby probes off (standby pipes are then probed at the probe
	// interval), as for net.KeepAliveConfig.Interval; otherwise at least
	// 1 s.
	StandbyProbe time.Duration
	// ProbeInterval is the probe interval k of idle pipe pairs, measured
	// to the carrier. Zero is 15 s; a negative value turns these idle
	// probes off, as for net.KeepAliveConfig.Idle, which leaves the
	// PIPE_PING of an answer, the standby probes and the probes of
	// uplinks and downlinks; otherwise at least 1 s.
	ProbeInterval time.Duration
	// ProbeJitter is the probe jitter j, from 0 to below 1 (MULTIPATH.md
	// 7.1): each probe waits a uniform draw from [k_eff x (1 - j), k_eff],
	// the first one of pipe 0 from [0, k_eff]. Zero is 0.25; a jitter of 0
	// takes NoProbeJitter.
	ProbeJitter float64
	// SpreadChunk is the unit the spreading policies split data into. Zero
	// is 16 KiB; a negative value is invalid.
	SpreadChunk int
	// NoProbation turns probation off; Probation must then be zero.
	NoProbation bool
	// NoProbeJitter makes j 0, the fixed schedule.
	NoProbeJitter bool
	// NoRedundancy turns redundancy off.
	NoRedundancy bool
}

// EarlyData lets a client send stream data before the server's transport
// parameters arrive (SPEC.md 6.1.2). With either form, Start, Client and
// Dial return at once and ConnectionState().Established stays false until
// the parameters arrive.
//
// ZmuxPeer: the client knows the server is a zmux endpoint and writes its
// first streams on the bootstrap carrier right away, within the floor
// values every zmux endpoint grants. Nothing is replayable.
//
// Replayable with Remembered: the client sends its first streams in the
// carrier's own early data (QUIC 0-RTT through zmuxquic.Dial), under the
// parameters remembered from an earlier session with the same server
// (Conn.RememberedParameters). When the carrier refuses its early data,
// the session sends those streams again after the handshake, without
// telling the application. Carrier early data can be replayed by an
// attacker, so the server sets Replayable as well: its streams that
// arrived in early data then report Stream.Replayable, and a server that
// must not act on a replayed request calls Stream.Reject on it; the client
// gets a *StreamError for which IsRetryable is true, and opens the stream
// again on the same session.
//
// When the server cannot take the early data at all (it is not a zmux
// endpoint, or it grants no bidirectional stream), it processes none of it
// and the session ends with *EarlyDataRejectedError: every call on the
// session and its streams that is waiting or comes later (Read, Write,
// AcceptStream, Wait, ...) returns it. errors.Is(err, ErrEarlyDataRejected)
// and IsRetryable report it; retry on a new session without EarlyData,
// since the cause persists. Remembered parameters above what the server
// grants now end the session with a *TransportError
// (TransportParameterError) instead: forget them.
type EarlyData struct {
	// ZmuxPeer declares the peer a zmux endpoint (client only).
	ZmuxPeer bool
	// Replayable allows carrier early data (TLS 1.3, QUIC 0-RTT): on a
	// client together with Remembered, on a server to accept and mark it.
	Replayable bool
	// Remembered are the peer's parameters from an earlier session, as
	// Conn.RememberedParameters returned them (client only). Store them per
	// server and only with the application's consent.
	Remembered []byte
}

// PipeAuth selects the secret sources of joins (MULTIPATH.md 10.1).
type PipeAuth struct {
	// PSK is a pre-shared key of at least 16 bytes.
	PSK []byte
	// TLS asserts that pipe 0's TLS 1.3 handshake terminates at the peer:
	// the join secret is derived from its exporter, read from the
	// carrier's ConnectionState, as a TLS or a zmuxws WebSocket carrier
	// has it. A carrier whose TLS this process does not terminate (none,
	// or ended at a proxy) has no exporter, and the session fails to
	// start. An intermediary that terminates TLS on both sides leaves the
	// ends with different exporters, so that joins fail: the binding to
	// the exporter, or a client that pins the server, is what proves the
	// TLS end to end, not the presence of TLS.
	TLS bool
	// AllowInsecure allows joins without a secret (tests, protected
	// networks). It must not be combined with PSK.
	AllowInsecure bool
}

// Clone returns a shallow copy (nil means defaults): slices such as
// PipeAuth.PSK and EarlyData.Remembered and the function fields are shared.
func (c *Config) Clone() *Config {
	if c == nil {
		return &Config{}
	}
	cp := *c
	return &cp
}

// clock is the configured Clock; c may be nil.
func (c *Config) clock() timeshim.Clock {
	if c == nil {
		return nil
	}
	return c.Clock
}

// noTimeout stands for "no timeout" (a negative timeout setting): long
// enough never to pass, short enough to add to any clock reading.
const noTimeout = 100 * 365 * 24 * time.Hour

// timeout resolves a timeout setting: 0 is def, a negative value none.
func timeout(v, def time.Duration) time.Duration {
	switch {
	case v < 0:
		return noTimeout
	case v == 0:
		return def
	}
	return v
}

// handshakeTimeout is the effective HandshakeTimeout; c may be nil.
func (c *Config) handshakeTimeout() time.Duration {
	if c == nil {
		return time.Duration(core.DefaultHandshakeTimeout)
	}
	return timeout(c.HandshakeTimeout, time.Duration(core.DefaultHandshakeTimeout))
}

func dur(d, def time.Duration) core.Duration {
	if d == 0 {
		d = def
	}
	return core.Duration(d)
}

// NoMemoryLimit as a MemoryBudget (Config or ListenerConfig) leaves the
// memory unlimited; it is the only value that lifts a size limit.
const NoMemoryLimit int64 = -1

// checkProbeInterval reports a probe interval that is neither 0 (the
// default), negative (off) nor at least 1 s.
func checkProbeInterval(v time.Duration, what string) error {
	if v > 0 && v < time.Second {
		return fmt.Errorf("%w: %s %v neither 0 nor at least 1 s", ErrInvalidConfig, what, v)
	}
	return nil
}

// checkProbeJitter reports a probe jitter outside [0, 1).
func checkProbeJitter(v float64, what string) error {
	if !(v >= 0 && v < 1) { // NaN included
		return fmt.Errorf("%w: %s %v outside [0, 1)", ErrInvalidConfig, what, v)
	}
	return nil
}

// checkLimits reports shaping limits outside their ranges: a negative
// duration, or a MaxPaddingRatio that is not a number from 0 to 1.
func checkLimits(l *shaping.Limits) error {
	if !(l.MaxPaddingRatio >= 0 && l.MaxPaddingRatio <= 1) { // NaN included
		return fmt.Errorf("%w: ShapingLimits.MaxPaddingRatio %v outside [0, 1]", ErrInvalidConfig, l.MaxPaddingRatio)
	}
	if l.MaxPauseTotal < 0 || l.MaxDelay < 0 {
		return fmt.Errorf("%w: negative ShapingLimits duration", ErrInvalidConfig)
	}
	return nil
}

// coreConfig validates c and resolves it for the core.
func (c *Config) coreConfig(role core.Role, transport core.Role) (core.Config, error) {
	if c == nil {
		return (&Config{}).coreConfig(role, transport)
	}
	cc := core.DefaultConfig()
	cc.Role, cc.TransportRole = role, transport
	l := &cc.Local
	p := &c.SchedulerParams
	if c.LatencyBudget < 0 || c.MemoryBudget < 0 && c.MemoryBudget != NoMemoryLimit || c.MaxIncomingStreams < 0 || c.MaxIncomingUniStreams < 0 ||
		c.DatagramSendQueue < 0 || c.DatagramReceiveQueue < 0 || c.DatagramStreamQueue < 0 || c.DatagramReceiveBytes < 0 ||
		c.MaxPipes < 0 || c.PipeHeadroom < 0 || c.ReplayWindow < 0 || c.MaxLanes < 0 || c.ResumeTimeout < 0 ||
		p.Probation < 0 || p.OverflowDelay < 0 ||
		p.SpreadChunk < 0 {
		return cc, fmt.Errorf("%w: negative Config count, size or duration", ErrInvalidConfig)
	}
	cc.HandshakeTimeout = core.Duration(c.handshakeTimeout())
	switch {
	case c.MaxIdleTimeout < 0:
		l.Has &^= wire.HasIdle
	case c.MaxIdleTimeout > 0 && c.MaxIdleTimeout < time.Millisecond:
		// The parameter is in milliseconds and its 0 means none.
		return cc, fmt.Errorf("%w: MaxIdleTimeout %v below 1 ms", ErrInvalidConfig, c.MaxIdleTimeout)
	case c.MaxIdleTimeout > 0:
		l.MaxIdleTimeout = uint64(c.MaxIdleTimeout / time.Millisecond)
	}
	lv := c.liveness()
	clv, err := lv.core()
	if err != nil {
		return cc, err
	}
	cc.SetLiveness(clv)
	if v := c.InitialStreamReceiveWindow; v != 0 {
		if v < 4096 {
			return cc, fmt.Errorf("%w: InitialStreamReceiveWindow below 4 KiB", ErrInvalidConfig)
		}
		l.MaxStreamDataBidiLoc, l.MaxStreamDataBidiRem, l.MaxStreamDataUni = v, v, v
	}
	if v := c.InitialConnectionReceiveWindow; v != 0 {
		if v < 16384 {
			return cc, fmt.Errorf("%w: InitialConnectionReceiveWindow below 16 KiB", ErrInvalidConfig)
		}
		l.InitialMaxData = v
	}
	cc.StreamWindowCap = c.MaxStreamReceiveWindow
	cc.ConnWindowCap = c.MaxConnectionReceiveWindow
	if c.LatencyBudget > 0 {
		cc.LatencyBudget = core.Duration(c.LatencyBudget)
	}
	switch {
	case c.MemoryBudget == NoMemoryLimit:
		cc.MemoryBudget = 0 // unlimited
	case c.MemoryBudget > 0:
		cc.MemoryBudget = uint64(c.MemoryBudget)
	}
	switch {
	case c.NoIncomingStreams:
		l.MaxStreamsBidi = 0
	case c.MaxIncomingStreams > 0:
		if c.MaxIncomingStreams < 4 {
			return cc, fmt.Errorf("%w: MaxIncomingStreams must be 0 or at least 4", ErrInvalidConfig)
		}
		l.MaxStreamsBidi = uint64(c.MaxIncomingStreams)
	}
	switch {
	case c.NoIncomingUniStreams:
		cc.StagedUni, l.MaxStreamsUni = false, 0
	case c.MaxIncomingUniStreams > 0:
		cc.StagedUni, l.MaxStreamsUni = false, uint64(c.MaxIncomingUniStreams)
	}
	// The preamble limit: 4096, or the smallest initial window and a
	// quarter of the session window when smaller, at least 1024.
	pre := min(uint64(4096), l.MaxStreamDataBidiRem, l.MaxStreamDataUni, l.InitialMaxData/4)
	l.Preamble = max(pre, 1024)
	if c.EnableDatagrams {
		cc.Datagrams = true
		l.Has |= wire.HasDatagram | wire.HasFlows
		l.MaxDatagramFrameSize = 65535
	}
	setInt := func(dst *int, v int) {
		if v > 0 {
			*dst = v
		}
	}
	setInt(&cc.DgramSendQ, c.DatagramSendQueue)
	setInt(&cc.DgramRecvQ, c.DatagramReceiveQueue)
	setInt(&cc.DgramStreamQ, c.DatagramStreamQueue)
	if c.DatagramReceiveBytes > 0 {
		cc.DgramRecvSize = uint64(c.DatagramReceiveBytes)
	}
	if c.SendBuffer > 0 {
		cc.SendBuffer = c.SendBuffer
	}
	cc.SendBufferMax = max(c.MaxSendBuffer, cc.SendBuffer)
	if c.MaxSendBuffer == 0 {
		cc.SendBufferMax = 0
	}
	if c.MaxLanes > 0 {
		l.MaxLanes = uint64(c.MaxLanes)
	}
	if _, err := rand.Read(l.LaneKey[:]); err != nil {
		return cc, err
	}
	if c.MaxPipes > 0 {
		cc.MaxPipes = c.MaxPipes
	}
	if c.PipeHeadroom > 0 {
		cc.Headroom = c.PipeHeadroom
	}
	cc.ReplayWindow = c.ReplayWindow
	cc.Resume = core.Duration(c.ResumeTimeout)
	switch {
	case p.NoProbation && p.Probation != 0:
		return cc, fmt.Errorf("%w: Probation with NoProbation", ErrInvalidConfig)
	case p.NoProbation:
		cc.Probation = 0
	case p.Probation != 0:
		cc.Probation = core.Duration(p.Probation)
	}
	cc.OverflowDelay = dur(p.OverflowDelay, time.Second)
	cc.Redundancy = !p.NoRedundancy
	if p.SpreadChunk > 0 {
		cc.SpreadChunk = p.SpreadChunk
	}
	if err := checkLimits(&c.ShapingLimits); err != nil {
		return cc, err
	}
	cc.ShapeLimits = c.ShapingLimits
	if c.Scheduler != nil {
		cc.Policy = c.Scheduler.policy()
	}
	a := &c.PipeAuth
	if a.AllowInsecure && len(a.PSK) > 0 {
		return cc, fmt.Errorf("%w: AllowInsecure with a PSK", ErrInvalidConfig)
	}
	if len(a.PSK) > 0 && len(a.PSK) < 16 {
		return cc, fmt.Errorf("%w: PSK shorter than 16 bytes", ErrInvalidConfig)
	}
	cc.PSK = a.PSK
	if cc.MaxPipes > 1 || cc.Resume > 0 {
		if !a.TLS && len(a.PSK) == 0 && !a.AllowInsecure {
			return cc, fmt.Errorf("%w: multipath needs PipeAuth", ErrInvalidConfig)
		}
		l.Has |= wire.HasMultipath
		if a.TLS {
			l.SecretFlags |= wire.SecretExporter
		}
		if len(a.PSK) > 0 {
			l.SecretFlags |= wire.SecretPSK
		}
		if a.AllowInsecure {
			l.SecretFlags |= wire.SecretInsecure
		}
		if _, err := rand.Read(l.SessionNonce[:]); err != nil {
			return cc, err
		}
		if cc.Resume > 0 {
			l.Has |= wire.HasResume
			l.ResumeGrace = uint64(max(1, cc.Resume/core.Millisecond))
		}
	}
	if role == core.RoleAuto {
		l.Has |= wire.HasRole
		l.Role = wire.RoleAuto
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return cc, err
		}
		for _, x := range b {
			l.RoleNonce = l.RoleNonce<<8 | uint64(x)
		}
	}
	// GREASE transport parameter (SPEC.md 5.2 rule 6).
	var g [3]byte
	if _, err := rand.Read(g[:]); err != nil {
		return cc, err
	}
	n := uint64(g[0])<<8 | uint64(g[1])
	l.GreaseID = 31*n + 27
	l.GreaseValue = make([]byte, int(g[2])%17)
	_, _ = rand.Read(l.GreaseValue)
	cc.DgramRecvLimited = c.DatagramLimiter.Receive != nil
	cc.EarlyZmuxPeer = c.EarlyData.ZmuxPeer
	cc.EarlyReplayable = c.EarlyData.Replayable
	cc.EarlyRemembered = decodeRemembered(c.EarlyData.Remembered)
	return cc, nil
}
