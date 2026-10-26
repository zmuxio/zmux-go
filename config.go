package zmux

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// Config configures a session (API.md Section 6). The zero value of every
// field selects its default; nil *Config means all defaults.
type Config struct {
	// HandshakeTimeout bounds establishment and join confirmation (10 s).
	HandshakeTimeout time.Duration
	// MaxIdleTimeout is max_idle_timeout (30 s); negative disables it.
	MaxIdleTimeout time.Duration
	// KeepAlivePeriod sends a QX_PING when the session is idle this long
	// (0: off). It must be below the effective idle timeout.
	KeepAlivePeriod time.Duration

	// InitialStreamReceiveWindow (256 KiB, at least 4 KiB).
	InitialStreamReceiveWindow uint64
	// MaxStreamReceiveWindow caps auto-tuning (0: auto, a 64 MiB floor
	// that grows within the memory budget).
	MaxStreamReceiveWindow uint64
	// LatencyBudget is the B of window auto-tuning (100 us).
	LatencyBudget time.Duration
	// InitialConnectionReceiveWindow (1 MiB, at least 16 KiB).
	InitialConnectionReceiveWindow uint64
	// MaxConnectionReceiveWindow (0: auto, a 96 MiB floor).
	MaxConnectionReceiveWindow uint64
	// MemoryBudget of a session without a listener (1 GiB); negative is
	// unlimited.
	MemoryBudget int64

	// MaxIncomingStreams is initial_max_streams_bidi (256); negative is 0.
	MaxIncomingStreams int64
	// MaxIncomingUniStreams: 0 = staged credit (16, raised to 256 on the
	// first AcceptUniStream); negative = explicit 0; positive = that value.
	MaxIncomingUniStreams int64

	// EnableDatagrams announces max_datagram_frame_size and datagram flows.
	EnableDatagrams bool
	// Datagram queue sizes (64 / 128 / 32 per stream / 1 MiB).
	DatagramSendQueue    int
	DatagramReceiveQueue int
	DatagramStreamQueue  int
	DatagramReceiveBytes int

	// SendBuffer bounds written-but-unreleased bytes (4 MiB).
	SendBuffer uint64
	// MaxSendBuffer bounds its growth in multipath sessions (0: auto).
	MaxSendBuffer uint64

	// MaxPipes is the attachments this endpoint aims for (1: multipath
	// off; 8 recommended when multipath is used).
	MaxPipes int
	// PipeHeadroom is H of the attachment cap (8).
	PipeHeadroom int
	// ReplayWindow is W (default max(64, 2*(MaxPipes+H))).
	ReplayWindow int
	// MaxLanes is the lanes accepted per inbound pipe (258; 1 disables).
	MaxLanes int

	// ResumeTimeout enables resume with this grace period.
	ResumeTimeout time.Duration

	// Scheduler is the session's scheduling policy (nil: Affinity).
	Scheduler Scheduler
	// SchedulerParams tune the scheduler and failure detection.
	SchedulerParams SchedulerParams

	// PipeAuth selects the join secret (required for multipath).
	PipeAuth PipeAuth

	// EarlyData enables early data (SPEC.md 6.1.2).
	EarlyData EarlyData

	// DatagramLimiter polices datagrams in each direction (API.md 5.6):
	// a datagram passes when TryReady allows its whole payload, and is
	// then charged; otherwise it is dropped and counted.
	DatagramLimiter DatagramLimiter

	// Tracer receives session events on a goroutine of its own.
	Tracer *Tracer
	// Redial re-establishes pipes after failures (resume).
	Redial func(ctx context.Context, need Direction) (PipeIO, error)
	// AllowConnectionWindowIncrease approves session window growth.
	AllowConnectionWindowIncrease func(delta uint64) bool
}

// DatagramLimiter holds the limiters of the two datagram directions.
type DatagramLimiter struct {
	Send, Receive Limiter
}

// SchedulerParams are the scheduler parameters of API.md Section 6.
type SchedulerParams struct {
	SuspectFloor  time.Duration // 250 ms, at least 50 ms
	DeadTimeout   time.Duration // 0: default
	Probation     *time.Duration
	OverflowDelay time.Duration // 1 s
	StandbyProbe  time.Duration // 1 s; negative: the probe interval
	ProbeInterval time.Duration // 15 s; negative: off
	NoRedundancy  bool          // redundancy "off"
	SpreadChunk   int           // 16 KiB
}

// EarlyData enables early data.
type EarlyData struct {
	// ZmuxPeer declares the peer a zmux endpoint: data written before its
	// parameters arrive goes out under the floor, behind the fence.
	ZmuxPeer bool
	// Replayable allows carrier early data (TLS 1.3, QUIC 0-RTT).
	Replayable bool
	// Remembered are the peer's parameters from an earlier session
	// (Conn.RememberedParameters): a client sends its first streams in the
	// carrier's early data under them.
	Remembered []byte
}

// PipeAuth selects the secret sources of joins (MULTIPATH.md 10.1).
type PipeAuth struct {
	// TLS asserts that pipe 0's TLS 1.3 handshake terminates at the peer.
	TLS bool
	// PSK is a pre-shared key of at least 16 bytes.
	PSK []byte
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

func dur(d, def time.Duration) core.Duration {
	if d == 0 {
		d = def
	}
	return core.Duration(d)
}

// coreConfig validates c and resolves it for the core.
func (c *Config) coreConfig(role core.Role, transport core.Role) (core.Config, error) {
	if c == nil {
		c = &Config{}
	}
	cc := core.DefaultConfig()
	cc.Role, cc.TransportRole = role, transport
	l := &cc.Local
	if c.HandshakeTimeout != 0 {
		cc.HandshakeTimeout = core.Duration(c.HandshakeTimeout)
	}
	switch {
	case c.MaxIdleTimeout < 0:
		l.Has &^= wire.HasIdle
	case c.MaxIdleTimeout > 0:
		l.MaxIdleTimeout = uint64(c.MaxIdleTimeout / time.Millisecond)
	}
	cc.KeepAlive = core.Duration(c.KeepAlivePeriod)
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
	case c.MemoryBudget < 0:
		cc.MemoryBudget = 0
	case c.MemoryBudget > 0:
		cc.MemoryBudget = uint64(c.MemoryBudget)
	}
	switch {
	case c.MaxIncomingStreams < 0:
		l.MaxStreamsBidi = 0
	case c.MaxIncomingStreams > 0:
		if c.MaxIncomingStreams < 4 {
			return cc, fmt.Errorf("%w: MaxIncomingStreams must be 0 or at least 4", ErrInvalidConfig)
		}
		l.MaxStreamsBidi = uint64(c.MaxIncomingStreams)
	}
	switch {
	case c.MaxIncomingUniStreams < 0:
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
	p := &c.SchedulerParams
	if p.SuspectFloor != 0 {
		if p.SuspectFloor < 50*time.Millisecond {
			return cc, fmt.Errorf("%w: SuspectFloor below 50 ms", ErrInvalidConfig)
		}
		cc.SuspectFloor = core.Duration(p.SuspectFloor)
	}
	if p.DeadTimeout != 0 {
		if p.DeadTimeout < time.Second {
			return cc, fmt.Errorf("%w: DeadTimeout below 1 s", ErrInvalidConfig)
		}
		cc.DeadTimeout = core.Duration(p.DeadTimeout)
	}
	if p.Probation != nil {
		cc.Probation = core.Duration(*p.Probation)
	}
	cc.OverflowDelay = dur(p.OverflowDelay, time.Second)
	switch {
	case p.StandbyProbe < 0:
		cc.StandbyProbe = 0
	case p.StandbyProbe > 0:
		cc.StandbyProbe = core.Duration(p.StandbyProbe)
	}
	switch {
	case p.ProbeInterval < 0:
		cc.ProbeInterval = 0
	case p.ProbeInterval > 0:
		cc.ProbeInterval = core.Duration(p.ProbeInterval)
	}
	cc.Redundancy = !p.NoRedundancy
	if p.SpreadChunk > 0 {
		cc.SpreadChunk = p.SpreadChunk
	}
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
	l.HasGrease, l.GreaseID = true, 31*n+27
	l.GreaseValue = make([]byte, int(g[2])%17)
	_, _ = rand.Read(l.GreaseValue)
	cc.DgramRecvLimited = c.DatagramLimiter.Receive != nil
	cc.EarlyZmuxPeer = c.EarlyData.ZmuxPeer
	cc.EarlyReplayable = c.EarlyData.Replayable
	cc.EarlyRemembered = decodeRemembered(c.EarlyData.Remembered)
	_ = math.MaxInt64
	return cc, nil
}
