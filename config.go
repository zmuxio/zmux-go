package zmux

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/setting"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// Session defaults, which the zero values of Config fields select.
const (
	// DefaultKeepAlivePeriod is the default Liveness.KeepAlivePeriod.
	DefaultKeepAlivePeriod = time.Duration(core.DefaultKeepAlive)
	// DefaultMaxIdleTimeout is the default Config.MaxIdleTimeout.
	DefaultMaxIdleTimeout = time.Duration(core.DefaultIdleTimeout)
	// DefaultProbeJitter is the default Liveness.ProbeJitter.
	DefaultProbeJitter = core.DefaultProbeJitter
	// DefaultMaxLanes is the default Config.MaxLanes, the lanes accepted
	// per inbound pipe.
	DefaultMaxLanes = core.DefaultMaxLanes
	// DefaultRecoveryRatio is the default CapacityFloor.RecoveryRatio.
	DefaultRecoveryRatio = core.DefaultRecoveryRatio
	// DefaultMinInterval is the default QualityBand.MinInterval.
	DefaultMinInterval = time.Duration(core.DefaultMinInterval)
	// DefaultFirstRecordTimeout is the default ListenerConfig.FirstRecordTimeout,
	// and the time HandleLane waits for a lane's header.
	DefaultFirstRecordTimeout = time.Duration(core.DefaultFirstRecordTimeout)
	// DefaultJoinTimeout is the default Config.JoinTimeout.
	DefaultJoinTimeout = time.Duration(core.DefaultHandshakeTimeout)
	// DefaultMinBackoff is the default Backoff.Min, the first wait of the
	// redial backoff of the Redial hook (Config.RedialBackoff) and of each
	// pool (PipePool.Backoff); each wait doubles up to the maximum.
	DefaultMinBackoff = time.Second
	// DefaultMaxBackoff is the default Backoff.Max, the longest wait.
	DefaultMaxBackoff = 60 * time.Second
	// DefaultBackoffJitter is the default Backoff.Jitter: each wait is
	// reduced by a random fraction of up to it.
	DefaultBackoffJitter = 0.2
)

// MaxStreamCredit is the largest stream count credit the encoding allows,
// 2^60 (RFC 9000 Section 4.6): the value that grants the peer as many
// streams as the protocol can count, in effect no limit. It is the upper
// end of the range of Config.MaxIncomingStreams and MaxIncomingUniStreams
// and of Conn.RaiseIncomingStreams; those settings have no unlimited
// setting of their own, and their negative value stays "none".
const MaxStreamCredit int64 = 1 << 60

// DefaultMaxRecordSize is the max_record_size in effect while it is
// absent (16382 bytes): records are built at most this large before the
// peer's parameters arrive, and always on a TLS carrier.
const DefaultMaxRecordSize = wire.DefaultMaxRecordSize

// Config configures a session; nil *Config means all defaults. Its fields
// follow the Settings rules of the package documentation: the zero value
// selects the stated default, a negative value is the off, none, no
// timeout or unlimited setting where the field says so and invalid
// (ErrInvalidConfig) otherwise, and a Switch turns off a feature that is on
// by default. A value out of range is an error, never corrected.
type Config struct {
	// HandshakeTimeout bounds establishment: the peer's transport
	// parameters must arrive within it. Zero is 10 s; a negative value is
	// no timeout. It does not bound joins (JoinTimeout).
	HandshakeTimeout time.Duration
	// JoinTimeout is the deadline of every join: a pipe this endpoint adds
	// (AddPipe without a context deadline, Redial, PipePool), a pipe or
	// lane the peer adds until it is confirmed, a join held for the join
	// keys and a carrier stalled after a rejected join. Zero is
	// DefaultJoinTimeout (10 s); it cannot be no timeout, so a negative
	// value is invalid, and so is one above the maximum join deadline of 60 s,
	// on which epoch retention rests. It is independent of HandshakeTimeout.
	JoinTimeout time.Duration
	// EpochPeriod, EpochJoins, EpochAfterFirstPipe and EpochAfterResume
	// are the policy of session epochs, the key exchanges that evolve the
	// session secret. A multipath session advances its epoch after EpochPeriod
	// in one epoch, after EpochJoins joins in it, once when its first Sealed or
	// Bound pipe exists if its secret comes from a TLS exporter or a pre-shared
	// key (EpochAfterFirstPipe), and once after a suspension ends
	// (EpochAfterResume). Zero EpochPeriod and EpochJoins are the defaults,
	// every hour and every 65536 joins; a negative one turns that trigger
	// off. The two switches are on by default; SwitchOff turns theirs off.
	EpochPeriod         time.Duration
	EpochJoins          int
	EpochAfterFirstPipe Switch
	EpochAfterResume    Switch
	// EnableDatagrams announces max_datagram_frame_size and datagram flows.
	EnableDatagrams bool
	// ShapingPreciseTimer meets shaping deadlines in place: the session's
	// timer is armed ahead by the host timer's observed lateness, at most
	// 2 ms, and its callback yields in a loop until the deadline, which costs
	// CPU time up to that lead on one goroutine per deadline. Off: deadlines
	// are met as the host timer allows.
	ShapingPreciseTimer bool
	// MaxIdleTimeout is max_idle_timeout, the time without inbound
	// activity after which the session closes. Zero is 30 s; a negative
	// value announces no idle timeout; a positive one is at least 1 ms.
	MaxIdleTimeout time.Duration
	// Liveness are the keep-alive, failure detection and probe settings
	// of the session and the defaults of its pipes; Conn.SetLiveness
	// replaces them while the session runs.
	Liveness Liveness

	// InitialStreamReceiveWindow is the initial receive window of a
	// stream, in bytes. Zero is 256 KiB; otherwise at least 4 KiB; a
	// negative value is invalid.
	InitialStreamReceiveWindow int64
	// MaxStreamReceiveWindow caps the auto-tuning of a stream's window, in
	// bytes. Zero is automatic: a 64 MiB floor that grows within the memory
	// budget; a negative value is invalid.
	MaxStreamReceiveWindow int64
	// LatencyBudget is the B of window auto-tuning. Zero is 100 us (tuning
	// needs a positive budget); a negative value is invalid.
	LatencyBudget time.Duration
	// InitialConnectionReceiveWindow is the initial session window, in
	// bytes. Zero is 1 MiB; otherwise at least 16 KiB; a negative value is
	// invalid.
	InitialConnectionReceiveWindow int64
	// MaxConnectionReceiveWindow caps the session window, in bytes. Zero
	// is automatic: a 96 MiB floor; a negative value is invalid.
	MaxConnectionReceiveWindow int64
	// MemoryBudget bounds the memory of a session without a listener, in
	// bytes. Zero is 1 GiB; a negative value is unlimited.
	MemoryBudget int64

	// MaxIncomingStreams is initial_max_streams_bidi, the bidirectional
	// streams the peer may open at first. Zero is 256; a negative value
	// grants none; otherwise at least 4 and at most MaxStreamCredit.
	MaxIncomingStreams int64
	// MaxIncomingUniStreams is initial_max_streams_uni. Zero is a staged
	// credit: 16, raised to 256 on the first AcceptUniStream or HandleUniStreams; a negative
	// value grants none; otherwise at most MaxStreamCredit.
	MaxIncomingUniStreams int64

	// DatagramSendQueue, DatagramReceiveQueue, DatagramStreamQueue (per
	// stream) and DatagramReceiveBytes size the datagram queues. Zero is
	// 64, 128, 32 and 1 MiB (an empty queue would drop every datagram); a
	// negative value is invalid. DatagramReceiveBytes bounds the session
	// queue and the datagrams held for streams not opened yet, not the
	// stream queues.
	DatagramSendQueue    int
	DatagramReceiveQueue int
	DatagramStreamQueue  int
	DatagramReceiveBytes int64
	// DatagramStreamBytes bounds the payload bytes of each stream's
	// datagram queue, per direction of receipt, beside DatagramStreamQueue:
	// a datagram that would exceed either drops the oldest of that queue
	// until both hold, each counted as dropped; a single datagram larger
	// than it is kept alone. Zero is 512 KiB; a negative value is
	// unlimited, leaving the count as the only bound. DatagramReceiveBytes
	// does not include the stream queues, so with the defaults a session's
	// received datagrams are bounded by about MaxIncomingStreams x
	// DatagramStreamBytes (256 x 512 KiB) beside it.
	DatagramStreamBytes int64
	// DatagramQueueDelay is the target queueing delay of every datagram
	// queue (the send queue, the session receive queue and each stream's):
	// once the datagrams leaving a queue have waited longer than it for
	// 100 ms, CoDel (RFC 8289) drops some as they leave, more the longer
	// the standing delay lasts, each counted as dropped; a datagram alone
	// in its queue is never dropped for its delay. The count and byte
	// bounds stay as memory bounds. Zero is 20 ms; a negative value turns
	// the delay bound off.
	DatagramQueueDelay time.Duration

	// SendBuffer starts the written-but-unreleased byte budget, in bytes.
	// Zero is 4 MiB; a negative value is invalid. Multipath sessions grow
	// it to the retransmit budget, within SendBufferMax.
	SendBuffer int64
	// SendBufferMax fixes its growth maximum, in bytes; one below
	// SendBuffer (or its default), or a negative one, is invalid. Zero is
	// automatic: a 64 MiB floor, above it within the memory budget.
	SendBufferMax int64

	// MaxPipes is the attachments this endpoint aims for. Zero or 1 is
	// multipath off; 8 is recommended when multipath is used.
	MaxPipes int
	// PipeHeadroom is H of the attachment cap. Zero is 8.
	PipeHeadroom int
	// ReplayWindow is W, the join replay window. Zero is
	// max(64, 2*(MaxPipes+H)); a window below MaxPipes+H is invalid.
	ReplayWindow int
	// MaxLanes is the lanes accepted per inbound pipe. Zero is
	// DefaultMaxLanes (258); 1
	// accepts the stream lane only.
	MaxLanes int

	// ResumeTimeout enables resume with this grace period, at least 1 ms
	// (a fraction of a millisecond is rounded up). Zero is no resume; a
	// negative value is invalid. Resume needs a secret source (PipeAuth),
	// and a server session a Listener; a client should have a redial
	// source (Redial or MaintainPipes).
	ResumeTimeout time.Duration

	// Scheduler is the session's scheduling policy (nil: SchedulerAffinity).
	Scheduler Scheduler
	// SchedulerParams tune the scheduler and failure detection.
	SchedulerParams SchedulerParams
	// ShapingLimits are the application's bounds on shaping; an account field
	// above its maximum is invalid.
	ShapingLimits shaping.Limits
	// ShapingBudget gives the shaping configuration's budget, called once per
	// session outside every lock; nil: none. It cannot widen ShapingLimits.
	ShapingBudget func() shaping.Budget
	// HandshakeShaper is asked for the plan of each Join message this session
	// writes; nil: none.
	HandshakeShaper shaping.HandshakeShaper
	// Shaper is installed when the session is created, as by SetShaper
	// (nil: no shaping): every outbound pipe of the session, the
	// bootstrap pipe of an accepted session and joined pipes included, is
	// shaped from its first record. SetShaper replaces it.
	Shaper ShaperFactory

	// PipeAuth configures the source of the session secret. Multipath
	// (MaxPipes above 1) and resume need one: a PSK, a deferred PSK or the
	// exporter; on a secure first pipe the secure carrier is the source and
	// PipeAuth sets none of them. Without a source Start fails with
	// ErrInvalidConfig before a byte is written. A source is never dropped
	// because the peer did not offer it: flags that differ close the
	// session.
	PipeAuth PipeAuth
	// RecordPolicy chooses the record mode of each carrier this endpoint joins; nil is
	// always Sealed. PipeOptions.RecordMode wins over it.
	RecordPolicy RecordPolicy
	// AllowRecordMode is the acceptor's check of a Hello's mode; nil passes only Sealed.
	AllowRecordMode AllowRecordMode
	// AuthorizeJoin is an acceptor's further check of a join; nil passes all.
	AuthorizeJoin func(c *Conn, pipe PipeID) bool
	// JoinOutcome reports each authorized incoming join and each outgoing
	// carrier exactly once. A nil error means peer attachment. Calls can
	// overlap and run outside session locks on goroutines independent of
	// Conn.Wait. NextEvent and HandleEvents release a successful pipe's
	// events after its hook returns. Tracer may deliver earlier and drop
	// events. A hook can close and wait for its session. Draining its own
	// held event keys requires returning from the hook first.
	JoinOutcome func(c *Conn, pipe PipeID, err error)
	// JoinEarlyHello is the size of a Hello's first batch in bytes: 0 takes
	// 8 KiB; a negative value sends no first batch. Larger batches outgrow
	// the first congestion window and delay both the Hello and the Reply.
	JoinEarlyHello int

	// EarlyData enables early data: stream data before the peer's
	// transport parameters arrive.
	EarlyData EarlyData

	// DatagramLimiter polices datagrams in each direction: a datagram
	// passes when TryAcquire(n, n) grants its payload of n bytes (refunded
	// when it is then not queued); otherwise it is dropped and counted.
	DatagramLimiter DatagramLimiter

	// Tracer receives session events on a goroutine of its own, through a
	// backlog of 256 events per session that is not configurable.
	Tracer *Tracer
	// Logger receives what the application would otherwise not see: a
	// warning when the last pipe of a direction is removed while streams
	// are open, a warning with EventResumeUnattended, and the failures of
	// host callbacks that zmux contains (a panic in a hook), and the pool
	// policy decisions rejected without a DecisionRejected callback. It is called
	// outside every lock and must not block; nil logs nothing. What the
	// events and the Tracer report is not logged again.
	Logger *slog.Logger
	// CarrierCredential is presented on the carrier of a client session by the
	// functions that open one with this Config: Client, Dial and DialTLS
	// and the dialers of zmuxquic, zmuxws and zmuxh3 (nil: none). Pipes
	// opened otherwise, for AddPipe or Redial, present theirs where they
	// are dialed (PresentPipe, the adapters' options).
	CarrierCredential CarrierCredential
	// Redial re-establishes pipes after failures (resume): it is called
	// as soon as the session lacks a direction or suspends, delayed only
	// by the redial backoff since its previous call and the join refusal
	// backoff a refused join started (the longer
	// of the two). It keeps one pipe per direction; a session that keeps
	// several pipes is maintained with Conn.MaintainPipes and a PipePool,
	// which also replaces failed pipes, and leaves Redial nil.
	// A host can explicitly request one advised replacement with
	// Conn.RequestPipeReplacement. It uses the same callback with its
	// own backoff history. An outage cancels a pending replacement and
	// can call Redial and RedialPolicy while the replacement unwinds.
	Redial func(ctx context.Context, need Direction) (PipeIO, error)
	// RedialBackoff is the backoff of Redial: after a failed call the wait,
	// counted from the start of the previous call, starts at Min and
	// doubles up to Max, each wait reduced by a random fraction of up to
	// Jitter. The zero value is the default (1 s, 60 s, 0.2). The pools of
	// MaintainPipes follow the same rule with state of their own
	// (PipePool.Backoff).
	RedialBackoff Backoff
	// RedialPolicy, when set, is asked before each Redial call; DialAfter
	// is the wait from the previous call to the next (at most
	// RedialBackoff.Max; 0: the backoff), and Liveness applies to the pipe a
	// call yields. Count and Retire are not used. A
	// Decide that returns an error or panics, or a decision that fails
	// PoolDecision.Validate, is rejected: the call goes ahead as with the
	// zero decision and the error is reported to DecisionRejected.
	RedialPolicy PoolPolicy
	// DecisionRejected, when set, receives the error of each look of
	// RedialPolicy the session rejected, on the goroutine that calls Redial and outside every lock;
	// it must not block, and a panic in it is contained. Unset, the error
	// is logged to Logger. Redialling goes on either way.
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

// SchedulerParams tune multipath scheduling, under the rules of Config.
type SchedulerParams struct {
	// Probation is the base of a pipe's probation. Zero is the default
	// (from the RTT and the pipe's history); a negative value turns
	// probation off. PipeOptions.Probation overrides it per pipe.
	Probation time.Duration
	// OverflowDelay is how long higher tiers stay saturated before a
	// lower tier takes data. Zero is 1 s; a negative value is invalid.
	OverflowDelay time.Duration
	// SpreadChunk is the unit the spreading policies split data into, in
	// bytes. Zero is 16 KiB; explicit sizes are from 1 to 2^31 - 1.
	SpreadChunk int64
	// Redundancy sends the data of urgent stream directions (urgency 0
	// to 2) and the first chunk after an idle period redundantly on a
	// second pipe. It is on by default; SwitchOff turns it off.
	Redundancy Switch
}

// EarlyData lets a client send stream data before the server's transport
// parameters arrive. With either form, Start, Client and
// Dial return at once and ConnectionState().Established stays false until
// the parameters arrive.
//
// ZmuxPeer: the client knows the server is a zmux endpoint and writes its
// first streams on the bootstrap carrier right away, within the floor
// values every zmux endpoint grants. Nothing is replayable, unless the
// carrier itself sends early data (its W half is an EarlyClientCarrier
// whose SendsEarlyData is true, such as a transform whose first message
// carries the first flight; not QUIC without 0-RTT): the
// streams then travel in it, wait for its verdict and are sent again after
// a rejection, as with Remembered below, and the client consents to replay
// with Replayable (else Start fails with ErrInvalidConfig).
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
	// Remembered are the peer's parameters from an earlier session, as
	// Conn.RememberedParameters returned them (client only). Store them per
	// server and only with the application's consent.
	Remembered []byte
	// ZmuxPeer declares the peer a zmux endpoint (client only; with
	// RoleAuto, where no side is the client, it is invalid).
	ZmuxPeer bool
	// Replayable allows carrier early data, which the peer may see
	// replayed (TLS 1.3, QUIC 0-RTT, a transform's first flight): on a
	// client with Remembered, or with ZmuxPeer on a carrier with early
	// data; on a server to accept it and mark its streams.
	Replayable bool
	// HoldFirstFlight keeps the bootstrap pipe's first carrier write until
	// Conn.ReleaseFirstFlight, or until Config.HandshakeTimeout has passed
	// since Start (ConnectionStats.FirstFlightTimeouts); the handshake
	// timeout runs from the release. Streams opened and written before the
	// release leave in the same write as the transport parameters, as a
	// carrier whose first message carries the first flight wants. Client
	// with early data only (ZmuxPeer, or Remembered with Replayable);
	// otherwise ErrInvalidConfig.
	HoldFirstFlight bool
}

// clientEarly reports whether a client with e sends early data.
func (e *EarlyData) clientEarly() bool {
	return e.ZmuxPeer || e.Replayable && len(e.Remembered) > 0
}

// PipeAuth configures the secret sources of joins. EndToEnd is no source:
// it says the bootstrap carrier ends at the peer, which the exporter and a
// deferred PSK need to be trusted.
type PipeAuth struct {
	// PSK is a pre-shared key of at least 16 bytes.
	PSK []byte
	// Exporter derives the join secret from the exporter of the innermost
	// end-to-end handshake on pipe 0 (Exporter: a half implementing
	// KeyingMaterialExporter, or TLS/QUIC this process terminates), read
	// when the peer's parameters arrive; a failed read closes the session
	// with INTERNAL_ERROR. A carrier without an exporter (no TLS, or TLS
	// ended at a proxy, and no transform of its own) makes Start fail with
	// ErrInvalidConfig. An intermediary that terminates TLS on both sides
	// leaves the ends with different exporters, so that joins fail: the
	// binding to the exporter, or a client that pins the server, is what
	// proves the handshake end to end, not the presence of TLS.
	Exporter bool
	// DeferredPSK offers a PSK (the P flag) that the application supplies
	// after establishment with Conn.SetJoinSecret, for example once it
	// authenticated the peer over the session. Until then no join key
	// exists: local joins fail with ErrJoinPending and the peer's
	// joins are held (ConnectionStats.JoinPending) for at most
	// JoinTimeout, then closed silently; a session that loses its
	// pipes before the secret cannot resume. It combines with Exporter
	// as a static PSK does, and the peer may have the same bytes as a
	// static PSK. It must not be combined with PSK.
	DeferredPSK bool
	// EndToEnd declares in advance what Conn.DeclareEndToEnd declares: the
	// host knows pipe 0 ends at the peer before the session starts, such as
	// on a pinned connection. Without it, and without the secure carrier,
	// there are no join keys. It is no secret source of its own, and on a
	// secure first pipe it is accepted and has no effect, so that one
	// Config serves both.
	EndToEnd bool
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

// handshakeTimeout is the effective HandshakeTimeout; c may be nil.
func (c *Config) handshakeTimeout() time.Duration {
	if c == nil {
		return time.Duration(core.DefaultHandshakeTimeout)
	}
	return setting.Timeout(c.HandshakeTimeout, time.Duration(core.DefaultHandshakeTimeout))
}

// joinTimeout is the effective JoinTimeout; c may be nil (a negative
// value does not pass coreConfig).
func (c *Config) joinTimeout() time.Duration {
	if c == nil || c.JoinTimeout <= 0 {
		return DefaultJoinTimeout
	}
	return c.JoinTimeout
}

// redialBackoff returns the Redial backoff with the defaults filled in.
func (c *Config) redialBackoff() Backoff {
	if c == nil {
		return Backoff{}.filled()
	}
	return c.RedialBackoff.filled()
}

func dur(d, def time.Duration) core.Duration {
	if d == 0 {
		d = def
	}
	return core.Duration(d)
}

// Switch turns a feature that is on by default off or on. The zero value
// is the default; SwitchOff, or any negative value, turns the feature off,
// and SwitchOn, or any positive value, on.
type Switch int8

// The settings of a Switch other than the default.
const (
	SwitchOff Switch = -1
	SwitchOn  Switch = 1
)

// On reports whether a feature that is on by default is on.
func (s Switch) On() bool { return s >= 0 }

// checkProbeInterval reports a probe interval that is neither 0 (the
// default), negative (off) nor at least 1 s.
func checkProbeInterval(v time.Duration, what string) error {
	if v > 0 && v < time.Second {
		return fmt.Errorf("%w: %s %v neither 0 nor at least 1 s", ErrInvalidConfig, what, v)
	}
	return nil
}

// checkProbeJitter reports a probe jitter of 1 or more, or NaN (a negative
// jitter is 0).
func checkProbeJitter(v float64, what string) error {
	if !(v < 1) { // NaN included
		return fmt.Errorf("%w: %s %v not below 1", ErrInvalidConfig, what, v)
	}
	return nil
}

// checkLimits reports shaping limits outside their ranges.
func checkLimits(l *shaping.Limits) error {
	if a := l.Wire; a.Initial > shaping.MaxAccountInitial || a.PerMB > shaping.MaxAccountRate || a.PerSec > shaping.MaxAccountRate {
		return fmt.Errorf("%w: ShapingLimits.Wire field above its maximum", ErrInvalidConfig)
	}
	return nil
}

// askBudget calls f; nil or a panic is no budget (the panic is logged to
// l).
func askBudget(l *slog.Logger, f func() shaping.Budget) (b shaping.Budget) {
	if f == nil {
		return b
	}
	if !safeLog(l, "Config.ShapingBudget", func() { b = f() }) {
		b = shaping.Budget{}
	}
	return b
}

// coreConfig validates c and resolves it for the core.
func (c *Config) coreConfig(role, transport core.Role) (core.Config, error) {
	if c == nil {
		return (&Config{}).coreConfig(role, transport)
	}
	cc := core.DefaultConfig()
	cc.Role, cc.TransportRole = role, transport
	cc.PathAdvice = c.Redial != nil || c.Tracer != nil && c.Tracer.Event != nil
	cc.TraceStreams = c.Tracer != nil && c.Tracer.Event != nil && c.Tracer.StreamEvents
	l := &cc.Local
	p := &c.SchedulerParams
	if c.LatencyBudget < 0 || c.InitialStreamReceiveWindow < 0 || c.MaxStreamReceiveWindow < 0 ||
		c.InitialConnectionReceiveWindow < 0 || c.MaxConnectionReceiveWindow < 0 || c.SendBuffer < 0 || c.SendBufferMax < 0 ||
		c.DatagramSendQueue < 0 || c.DatagramReceiveQueue < 0 || c.DatagramStreamQueue < 0 || c.DatagramReceiveBytes < 0 ||
		c.MaxPipes < 0 || c.PipeHeadroom < 0 || c.ReplayWindow < 0 || c.MaxLanes < 0 || c.ResumeTimeout < 0 ||
		c.JoinTimeout < 0 ||
		p.OverflowDelay < 0 ||
		p.SpreadChunk < 0 {
		return cc, fmt.Errorf("%w: negative Config count, size or duration", ErrInvalidConfig)
	}
	if p.SpreadChunk > math.MaxInt32 {
		return cc, fmt.Errorf("%w: SpreadChunk %d above %d", ErrInvalidConfig, p.SpreadChunk, math.MaxInt32)
	}
	if c.ResumeTimeout > 0 && c.ResumeTimeout < time.Millisecond {
		return cc, fmt.Errorf("%w: ResumeTimeout %v below 1 ms", ErrInvalidConfig, c.ResumeTimeout)
	}
	if c.JoinTimeout > time.Duration(core.MaxJoinDeadline) {
		return cc, fmt.Errorf("%w: JoinTimeout %v above the maximum join deadline %v", ErrInvalidConfig, c.JoinTimeout, time.Duration(core.MaxJoinDeadline))
	}
	if err := c.RedialBackoff.validate("Config.RedialBackoff"); err != nil {
		return cc, err
	}
	if c.EarlyData.ZmuxPeer && role == core.RoleAuto {
		return cc, fmt.Errorf("%w: EarlyData.ZmuxPeer with RoleAuto", ErrInvalidConfig)
	}
	cc.HandshakeTimeout = core.Duration(c.handshakeTimeout())
	cc.JoinTimeout = core.Duration(c.joinTimeout())
	cc.EpochPeriod, cc.EpochJoins = core.Duration(c.EpochPeriod), int64(c.EpochJoins)
	cc.NoEpochAfterFirstPipe, cc.NoEpochAfterResume = !c.EpochAfterFirstPipe.On(), !c.EpochAfterResume.On()
	switch {
	case c.MaxIdleTimeout < 0:
		// No idle timeout: neither sent nor applied locally.
		l.Has &^= wire.HasIdle
		l.MaxIdleTimeout = 0
	case c.MaxIdleTimeout > 0 && c.MaxIdleTimeout < time.Millisecond:
		// The parameter is in milliseconds and its 0 means none.
		return cc, fmt.Errorf("%w: MaxIdleTimeout %v below 1 ms", ErrInvalidConfig, c.MaxIdleTimeout)
	case c.MaxIdleTimeout > 0:
		cc.IdleTimeout = core.Duration(c.MaxIdleTimeout)
		l.MaxIdleTimeout = wireMilliseconds(c.MaxIdleTimeout)
	}
	clv, err := c.Liveness.core()
	if err != nil {
		return cc, err
	}
	cc.SetLiveness(clv)
	if v := uint64(c.InitialStreamReceiveWindow); v != 0 {
		if v < core.MinStreamWindow {
			return cc, fmt.Errorf("%w: InitialStreamReceiveWindow below 4 KiB", ErrInvalidConfig)
		}
		l.MaxStreamDataBidiLoc, l.MaxStreamDataBidiRem, l.MaxStreamDataUni = v, v, v
	}
	if v := uint64(c.InitialConnectionReceiveWindow); v != 0 {
		if v < 16384 {
			return cc, fmt.Errorf("%w: InitialConnectionReceiveWindow below 16 KiB", ErrInvalidConfig)
		}
		l.InitialMaxData = v
	}
	cc.StreamWindowCap = uint64(c.MaxStreamReceiveWindow)
	cc.ConnWindowCap = uint64(c.MaxConnectionReceiveWindow)
	if c.LatencyBudget > 0 {
		cc.LatencyBudget = core.Duration(c.LatencyBudget)
	}
	switch {
	case c.MemoryBudget < 0:
		cc.MemoryBudget = 0 // unlimited
	case c.MemoryBudget > 0:
		cc.MemoryBudget = uint64(c.MemoryBudget)
	}
	switch {
	case c.MaxIncomingStreams < 0:
		l.MaxStreamsBidi = 0
	case c.MaxIncomingStreams > 0:
		if c.MaxIncomingStreams < 4 || c.MaxIncomingStreams > MaxStreamCredit {
			return cc, fmt.Errorf("%w: MaxIncomingStreams must be 0, negative or in [4, MaxStreamCredit]", ErrInvalidConfig)
		}
		l.MaxStreamsBidi = uint64(c.MaxIncomingStreams)
	}
	switch {
	case c.MaxIncomingUniStreams < 0:
		cc.StagedUni, l.MaxStreamsUni = false, 0
	case c.MaxIncomingUniStreams > MaxStreamCredit:
		return cc, fmt.Errorf("%w: MaxIncomingUniStreams above MaxStreamCredit", ErrInvalidConfig)
	case c.MaxIncomingUniStreams > 0:
		cc.StagedUni, l.MaxStreamsUni = false, uint64(c.MaxIncomingUniStreams)
	}
	// The preamble limit: 4096, or the smallest initial window and a
	// quarter of the session window when smaller, at least 1024.
	pre := min(uint64(core.MinStreamWindow), l.MaxStreamDataBidiRem, l.MaxStreamDataUni, l.InitialMaxData/4)
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
	switch {
	case c.DatagramStreamBytes < 0:
		cc.DgramStreamSize = 0 // unlimited
	case c.DatagramStreamBytes > 0:
		cc.DgramStreamSize = uint64(c.DatagramStreamBytes)
	}
	switch {
	case c.DatagramQueueDelay < 0:
		cc.DgramTarget = 0 // off
	case c.DatagramQueueDelay > 0:
		cc.DgramTarget = core.Duration(c.DatagramQueueDelay)
	}
	if c.SendBuffer > 0 {
		cc.SendBuffer = uint64(c.SendBuffer)
	}
	if c.SendBufferMax != 0 && uint64(c.SendBufferMax) < cc.SendBuffer {
		return cc, fmt.Errorf("%w: SendBufferMax %d below SendBuffer %d", ErrInvalidConfig, c.SendBufferMax, cc.SendBuffer)
	}
	cc.SendBufferMax = uint64(c.SendBufferMax)
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
	if c.ReplayWindow != 0 && c.ReplayWindow < max(1, cc.MaxPipes)+cc.Headroom {
		return cc, fmt.Errorf("%w: ReplayWindow %d below MaxPipes+PipeHeadroom (%d)", ErrInvalidConfig, c.ReplayWindow, max(1, cc.MaxPipes)+cc.Headroom)
	}
	cc.ReplayWindow = c.ReplayWindow
	cc.Resume = core.Duration(c.ResumeTimeout)
	switch {
	case p.Probation < 0:
		cc.Probation = 0
	case p.Probation > 0:
		cc.Probation = core.Duration(p.Probation)
	}
	cc.OverflowDelay = dur(p.OverflowDelay, time.Second)
	cc.Redundancy = p.Redundancy.On()
	if p.SpreadChunk > 0 {
		cc.SpreadChunk = int(p.SpreadChunk)
	}
	if err := checkLimits(&c.ShapingLimits); err != nil {
		return cc, err
	}
	cc.ShapeLimits = c.ShapingLimits
	cc.ShapeBudget = askBudget(c.Logger, c.ShapingBudget)
	if c.Scheduler != nil {
		cc.Policy = c.Scheduler.policy()
	}
	a := &c.PipeAuth
	if a.DeferredPSK && len(a.PSK) > 0 {
		return cc, fmt.Errorf("%w: DeferredPSK with a PSK", ErrInvalidConfig)
	}
	cc.Declared = a.EndToEnd
	cc.JoinEarlyHello = 8 << 10
	switch {
	case c.JoinEarlyHello < 0:
		cc.JoinEarlyHello = 0
	case c.JoinEarlyHello > 0:
		cc.JoinEarlyHello = c.JoinEarlyHello
	}
	cc.DeferredPSK = a.DeferredPSK
	if len(a.PSK) > 0 && len(a.PSK) < 16 {
		return cc, fmt.Errorf("%w: PSK shorter than 16 bytes", ErrInvalidConfig)
	}
	cc.PSK = a.PSK
	if cc.MaxPipes > 1 || cc.Resume > 0 {
		l.Has |= wire.HasMultipath | wire.HasSessionCrypto
		l.SessionCrypto = join.LocalCrypto()
		if a.Exporter {
			l.SecretFlags |= wire.SecretExporter
		}
		if len(a.PSK) > 0 || a.DeferredPSK {
			l.SecretFlags |= wire.SecretPSK
		}
		if _, err := rand.Read(l.SessionNonce[:]); err != nil {
			return cc, err
		}
		if cc.Resume > 0 {
			l.Has |= wire.HasResume
			l.ResumeGrace = wireMilliseconds(c.ResumeTimeout)
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
	// The GREASE transport parameter.
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
	if e := &c.EarlyData; e.HoldFirstFlight {
		if role != core.RoleInitiator || !e.clientEarly() {
			return cc, fmt.Errorf("%w: EarlyData.HoldFirstFlight without client early data", ErrInvalidConfig)
		}
		cc.HoldFirstFlight = true
	}
	return cc, nil
}

// wireMilliseconds rounds up without adding to the configured duration.
func wireMilliseconds(d time.Duration) uint64 {
	ms := d / time.Millisecond
	if d%time.Millisecond != 0 {
		ms++
	}
	return min(uint64(ms), varint.MaxVarint)
}
